// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Directory for concatenated Bitmap sections in a centroid shard.

use super::bitmap_global_index_reader::BitmapGlobalIndexReader;
use super::centroid_index::{self, Descriptor, Generation};
use crate::btree::key_serde::normalize_key_literal;
use crate::io::FileRead;
use crate::spec::{
    validate_datum_matches_type, DataField, DataType, Datum, IndexFileMeta, Predicate,
    PredicateOperator, TableSchema,
};
use crate::table::{index_file_path::IndexFileLocation, Table};
use crate::{Error, Result};
use bytes::Bytes;
use futures::future::BoxFuture;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

const BITMAP: u8 = 1;
const SECTION_HEADER: usize = 13;

pub(super) fn add_section(directory: &mut Vec<u8>, length: usize, metadata: &[u8]) -> Result<()> {
    let metadata_len = i32::try_from(metadata.len())
        .map_err(|_| centroid_index::invalid("Centroid bitmap metadata is too large"))?;
    directory.push(BITMAP);
    directory.extend((length as u64).to_be_bytes());
    directory.extend(metadata_len.to_be_bytes());
    directory.extend(metadata);
    Ok(())
}

pub(super) fn schema_id(payload: &[u8]) -> Result<i64> {
    let bytes = payload
        .get(..8)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| centroid_index::invalid("Truncated centroid bitmap schema ID"))?;
    let id = i64::from_be_bytes(bytes);
    if id < 0 {
        return Err(centroid_index::invalid("Invalid centroid bitmap schema ID"));
    }
    Ok(id)
}

fn directory(file: &IndexFileMeta) -> Result<HashMap<i32, Range<u64>>> {
    let invalid = || {
        centroid_index::invalid(format!(
            "Invalid centroid bitmap directory for '{}'",
            file.file_name
        ))
    };
    let descriptor = Descriptor::parse(file)?.ok_or_else(invalid)?;
    let payload = descriptor.payload.ok_or_else(invalid)?;
    let mut input = payload.get(8..).ok_or_else(invalid)?;
    let field_ids = file
        .global_index_meta
        .as_ref()
        .and_then(|meta| meta.extra_field_ids.as_ref())
        .filter(|fields| !fields.is_empty() && fields.len() <= input.len() / SECTION_HEADER)
        .ok_or_else(invalid)?;
    let size = u64::try_from(file.file_size).map_err(|_| invalid())?;
    let mut offset = 0u64;
    let mut sections = HashMap::new();
    for &field in field_ids {
        let header = input.get(..SECTION_HEADER).ok_or_else(invalid)?;
        if header[0] != BITMAP {
            return Err(invalid());
        }
        let length = u64::from_be_bytes(header[1..9].try_into().unwrap());
        let meta_len = usize::try_from(i32::from_be_bytes(header[9..13].try_into().unwrap()))
            .map_err(|_| invalid())?;
        let end = offset
            .checked_add(length)
            .filter(|&end| end > offset && end <= size)
            .ok_or_else(invalid)?;
        let next = SECTION_HEADER.checked_add(meta_len).ok_or_else(invalid)?;
        input = input.get(next..).ok_or_else(invalid)?;
        if sections.insert(field, offset..end).is_some() {
            return Err(invalid());
        }
        offset = end;
    }
    if !input.is_empty() || offset != size {
        return Err(invalid());
    }
    Ok(sections)
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(super) enum BitmapPredicate {
    Leaf(i32, DataType, PredicateOperator, Vec<Datum>),
    And(Vec<Self>),
    Or(Vec<Self>),
}

impl BitmapPredicate {
    fn bind(predicate: &Predicate, fields: &[DataField], allowed: &[i32]) -> Option<Self> {
        match predicate {
            Predicate::Leaf {
                column,
                index,
                data_type,
                op,
                literals,
            } => {
                let field = fields.get(*index)?;
                if field.name() != column
                    || !field.data_type().equals_ignore_nullable(data_type)
                    || !allowed.contains(&field.id())
                {
                    return None;
                }
                let values = match op {
                    PredicateOperator::IsNull | PredicateOperator::IsNotNull
                        if literals.is_empty() =>
                    {
                        vec![]
                    }
                    PredicateOperator::Eq if literals.len() == 1 => {
                        equality_literals(data_type, literals)?
                    }
                    PredicateOperator::In => equality_literals(data_type, literals)?,
                    _ => return None,
                };
                Some(Self::Leaf(
                    field.id(),
                    field.data_type().clone(),
                    *op,
                    values,
                ))
            }
            Predicate::And(children) | Predicate::Or(children) if !children.is_empty() => {
                let bound = children
                    .iter()
                    .map(|child| Self::bind(child, fields, allowed))
                    .collect::<Option<Vec<_>>>()?;
                Some(if matches!(predicate, Predicate::And(_)) {
                    Self::And(bound)
                } else {
                    Self::Or(bound)
                })
            }
            _ => None,
        }
    }

    fn matches_build_fields(&self, fields: &[DataField]) -> bool {
        match self {
            Self::Leaf(id, data_type, ..) => fields.iter().any(|field| {
                field.id() == *id && field.data_type().equals_ignore_nullable(data_type)
            }),
            Self::And(children) | Self::Or(children) => children
                .iter()
                .all(|child| child.matches_build_fields(fields)),
        }
    }

    async fn matches_build_schema(
        &self,
        table: &Table,
        schema_id: i64,
        catalog_schema: &mut Option<TableSchema>,
    ) -> Result<bool> {
        let error = match table.schema_manager().schema(schema_id).await {
            Ok(schema) => {
                return Ok(schema.id() == schema_id && self.matches_build_fields(schema.fields()));
            }
            Err(error) => error,
        };
        let Some(env) = table.rest_env() else {
            return Err(error);
        };
        if !matches!(&error, Error::IoUnexpected { source, .. } if source.kind() == opendal::ErrorKind::NotFound)
        {
            return Err(error);
        }
        // The REST catalog can serve the latest schema without a schema file.
        if env.identifier().parsed_object_name()?.branch_or_default() != table.branch() {
            return Ok(false);
        }
        let schema = match catalog_schema {
            Some(schema) => schema,
            None => {
                let response = env.current_table_identity_checked().await?;
                let (Some(id), Some(schema)) = (response.schema_id, response.schema) else {
                    return Err(centroid_index::invalid(
                        "REST table response is missing schema or schema ID",
                    ));
                };
                let schema = TableSchema::new(id, &schema);
                schema.validate_resolved_structure()?;
                table.schema_manager().cache_resolved_schema(schema.clone());
                catalog_schema.insert(schema)
            }
        };
        Ok(schema.id() == schema_id && self.matches_build_fields(schema.fields()))
    }

    pub(super) async fn for_generation(
        table: &Table,
        group: &Generation<'_>,
        predicate: &Predicate,
        catalog_schema: &mut Option<TableSchema>,
    ) -> Result<Option<Self>> {
        let Some(schema_id) = group.bitmap_schema_id else {
            return Ok(None);
        };
        let allowed = group
            .router
            .index_file
            .global_index_meta
            .as_ref()
            .unwrap()
            .extra_field_ids
            .as_deref()
            .unwrap_or_default();
        let Some(bound) = Self::bind(predicate, table.schema().fields(), allowed) else {
            return Ok(None);
        };
        if schema_id != table.schema().id()
            && !bound
                .matches_build_schema(table, schema_id, catalog_schema)
                .await?
        {
            return Ok(None);
        }
        Ok(Some(bound))
    }

    pub(super) async fn matching(
        &self,
        table: &Table,
        file: &IndexFileMeta,
    ) -> Result<RoaringTreemap> {
        let sections = directory(file)?;
        let path = IndexFileLocation::Global {
            table_path: table.location().trim_end_matches('/'),
        }
        .resolve(&file.file_name, file.external_path.as_deref());
        let input: Arc<dyn FileRead> = Arc::new(table.file_io().new_input(&path)?.reader().await?);
        let rows = self
            .evaluate(&input, &sections, &mut HashMap::new())
            .await?;
        let meta = file.global_index_meta.as_ref().unwrap();
        if rows
            .max()
            .is_some_and(|id| id > (meta.row_range_end - meta.row_range_start) as u64)
            || rows.len() > file.row_count as u64
        {
            return Err(centroid_index::invalid(
                "Centroid bitmap contains invalid row IDs",
            ));
        }
        Ok(rows)
    }

    fn evaluate<'a>(
        &'a self,
        input: &'a Arc<dyn FileRead>,
        sections: &'a HashMap<i32, Range<u64>>,
        readers: &'a mut HashMap<i32, BitmapGlobalIndexReader>,
    ) -> BoxFuture<'a, Result<RoaringTreemap>> {
        Box::pin(async move {
            match self {
                Self::Leaf(field, data_type, op, literals) => {
                    let reader = match readers.entry(*field) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let section = sections.get(field).ok_or_else(|| {
                                centroid_index::invalid("Missing centroid bitmap field")
                            })?;
                            let size = section.end - section.start;
                            let reader = SectionRead {
                                input: Arc::clone(input),
                                section: section.clone(),
                            };
                            entry.insert(
                                BitmapGlobalIndexReader::open(Box::new(reader), size)
                                    .await
                                    .map_err(bitmap_error)?,
                            )
                        }
                    };
                    reader
                        .query(*op, literals, data_type)
                        .await
                        .map_err(bitmap_error)
                }
                Self::And(children) | Self::Or(children) => {
                    let (first, rest) = children.split_first().ok_or_else(|| {
                        centroid_index::invalid("Empty centroid bitmap predicate")
                    })?;
                    let mut rows = first.evaluate(input, sections, readers).await?;
                    let intersection = matches!(self, Self::And(_));
                    for child in rest {
                        if intersection {
                            if rows.is_empty() {
                                break;
                            }
                            rows &= child.evaluate(input, sections, readers).await?;
                        } else {
                            rows |= child.evaluate(input, sections, readers).await?;
                        }
                    }
                    Ok(rows)
                }
            }
        })
    }
}

fn equality_literals(data_type: &DataType, literals: &[Datum]) -> Option<Vec<Datum>> {
    let mut normalized = literals.to_vec();
    for literal in &mut normalized {
        // Preserve signed zero for Arrow's value comparison. Non-finite
        // literals can have a different dictionary representation.
        let finite = match literal {
            Datum::Float(value) => value.is_finite(),
            Datum::Double(value) => value.is_finite(),
            _ => true,
        };
        if !finite
            || validate_datum_matches_type(literal, data_type).is_err()
            || !normalize_key_literal(literal, data_type)
        {
            return None;
        }
    }
    Some(normalized)
}

struct SectionRead {
    input: Arc<dyn FileRead>,
    section: Range<u64>,
}

#[async_trait::async_trait]
impl FileRead for SectionRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end || range.end > self.section.end - self.section.start {
            return Err(centroid_index::invalid(
                "Read outside centroid bitmap section",
            ));
        }
        self.input
            .read(self.section.start + range.start..self.section.start + range.end)
            .await
    }
}

fn bitmap_error(error: std::io::Error) -> crate::Error {
    crate::Error::DataInvalid {
        message: format!("Cannot read centroid bitmap: {error}"),
        source: Some(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::spec::{IntType, Schema};
    use axum::{routing::get, Json, Router};

    #[tokio::test]
    async fn centroid_bitmap_accepts_only_the_requested_catalog_schema() {
        let data_type = DataType::Int(IntType::new());
        let schema = Schema::builder()
            .column("key", data_type.clone())
            .build()
            .unwrap();
        let response = serde_json::json!({
            "database": "db", "name": "t", "id": "table-uuid", "schemaId": 0,
            "path": "memory:/catalog-bitmap-schema",
            "isExternal": false, "schema": schema,
        });
        let mut current = response.clone();
        current["schemaId"] = 1.into();
        let app = Router::new().route(
            "/v1/databases/db/tables/t",
            get(move || async move { Json(current) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut options = crate::Options::new();
        options.set("uri", format!("http://{}", listener.local_addr().unwrap()));
        options.set("warehouse", "test");
        options.set("token.provider", "bear");
        options.set("token", "test-token");
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let predicate =
            BitmapPredicate::Leaf(0, data_type, PredicateOperator::Eq, vec![Datum::Int(1)]);
        // A loaded handle can be older after evolution or newer after rollback.
        for loaded_id in [0, 2] {
            let mut loaded = response.clone();
            loaded["schemaId"] = loaded_id.into();
            let table = Table::from_rest_response(
                Identifier::new("db", "t"),
                serde_json::from_value(loaded).unwrap(),
                options.clone(),
            )
            .await
            .unwrap();
            let mut catalog_schema = None;
            assert!(predicate
                .matches_build_schema(&table, 1, &mut catalog_schema)
                .await
                .unwrap());
            assert_eq!(table.schema_manager().cached_schema(1).unwrap().id(), 1);
            assert!(!predicate
                .matches_build_schema(&table, 3, &mut catalog_schema)
                .await
                .unwrap());
        }
        server.abort();
    }
}
