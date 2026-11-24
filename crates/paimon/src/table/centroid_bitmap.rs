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

//! Scalar predicates over the Bitmap files of a centroid row-ID range.

use super::bitmap_global_index_reader::BitmapGlobalIndexReader;
use super::centroid_index::{self, Generation};
use crate::btree::key_serde::normalize_key_literal;
use crate::spec::{
    validate_datum_matches_type, DataField, DataType, Datum, IndexManifestEntry, Predicate,
    PredicateOperator, TableSchema,
};
use crate::table::{index_file_path::IndexFileLocation, Table};
use crate::{Error, Result};
use futures::future::BoxFuture;
use roaring::RoaringTreemap;
use std::collections::{BTreeMap, HashMap};

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
        group: &Generation<'_>,
    ) -> Result<HashMap<usize, RoaringTreemap>> {
        let rows = self
            .evaluate(table, &group.bitmaps, &mut HashMap::new())
            .await?;
        if rows.len() > group.vector_count {
            return Err(centroid_index::invalid(
                "Centroid bitmap contains invalid row IDs",
            ));
        }
        let mut matches = HashMap::<_, RoaringTreemap>::new();
        for row in rows {
            let centroid = (row / group.range_size) as usize;
            if row > i64::MAX as u64 || !group.shards.contains_key(&centroid) {
                return Err(centroid_index::invalid("Invalid centroid Bitmap position"));
            }
            matches
                .entry(centroid)
                .or_default()
                .insert(row % group.range_size);
        }
        Ok(matches)
    }

    fn evaluate<'a>(
        &'a self,
        table: &'a Table,
        files: &'a BTreeMap<i32, &IndexManifestEntry>,
        readers: &'a mut HashMap<i32, BitmapGlobalIndexReader>,
    ) -> BoxFuture<'a, Result<RoaringTreemap>> {
        Box::pin(async move {
            match self {
                Self::Leaf(field, data_type, op, literals) => {
                    let reader = match readers.entry(*field) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let file = &files
                                .get(field)
                                .ok_or_else(|| {
                                    centroid_index::invalid("Missing centroid bitmap field")
                                })?
                                .index_file;
                            let path = IndexFileLocation::Global {
                                table_path: table.location().trim_end_matches('/'),
                            }
                            .resolve(&file.file_name, file.external_path.as_deref());
                            let input = table.file_io().new_input(&path)?.reader().await?;
                            entry.insert(
                                BitmapGlobalIndexReader::open(
                                    Box::new(input),
                                    file.file_size as u64,
                                )
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
                    let mut rows = first.evaluate(table, files, readers).await?;
                    let intersection = matches!(self, Self::And(_));
                    for child in rest {
                        if intersection {
                            if rows.is_empty() {
                                break;
                            }
                            rows &= child.evaluate(table, files, readers).await?;
                        } else {
                            rows |= child.evaluate(table, files, readers).await?;
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
