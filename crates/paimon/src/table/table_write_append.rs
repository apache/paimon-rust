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

//! Append writer factory configuration survives physical-file prepares.

use super::*;

impl TableWrite {
    /// Resolve the columns captured by the destination append writer without
    /// creating files or changing bucket assignment state. Row adapters can
    /// encode only these columns, leaving unused payloads untouched. The input
    /// must contain exactly one row and the table's routing columns.
    pub fn write_fields_for_row(&self, routing_batch: &RecordBatch) -> Result<Vec<DataField>> {
        self.ensure_active()?;
        if self.format_writer.is_some() || !self.primary_key_indices.is_empty() {
            return Err(crate::Error::Unsupported {
                message: "write_fields_for_row requires a Paimon append table".into(),
            });
        }
        if routing_batch.num_rows() != 1 {
            return Err(crate::Error::DataInvalid {
                message: "write_fields_for_row requires exactly one routing row".into(),
                source: None,
            });
        }
        let names = self.automatic_routing_names();
        let key = if names.is_empty() {
            // A zero-column routing batch still represents one row. Avoid a
            // schema normalizer which would infer zero rows from no arrays.
            let bucket = CoreOptions::new(self.table.schema().options()).bucket();
            (
                EMPTY_SERIALIZED_ROW.clone(),
                if bucket == POSTPONE_BUCKET {
                    POSTPONE_BUCKET
                } else {
                    0
                },
            )
        } else {
            let routing = self.normalize_routing_columns(routing_batch, &names)?;
            let mut output = self.assign_append_routing(routing)?;
            (output.partition_bytes.remove(0), output.buckets[0])
        };
        Ok(self
            .append_write_configs
            .get(&key)
            .map(|config| &config.fields)
            .unwrap_or(&self.write_fields)
            .clone())
    }

    pub(super) fn append_write_config(&self) -> AppendWriteConfig {
        AppendWriteConfig {
            schema: self.write_schema.clone(),
            fields: self.write_fields.clone(),
            blob_consumer: self.blob_consumer.clone(),
            blob_uri_reader_factory: self.blob_uri_reader_factory.clone(),
        }
    }

    pub(super) fn has_reconfigured_append_type(&self) -> bool {
        self.append_write_type_changed
    }

    /// Java captures the physical write type per partition/bucket. Arrow's
    /// named columns must agree with that captured type, rather than being
    /// reinterpreted positionally as Java's InternalRow can be.
    pub(super) async fn write_reconfigured_append(
        &mut self,
        input: &RecordBatch,
        bucket: Option<i32>,
    ) -> Result<()> {
        let full_schema = build_target_arrow_schema(self.table.schema().fields())?;
        let actual_schema = input.schema();
        let names_match = |expected: &arrow_schema::Schema| {
            actual_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .eq(expected.fields().iter().map(|f| f.name()))
        };
        let mut routing_names = if bucket.is_some() {
            self.partition_keys.clone()
        } else {
            self.automatic_routing_names()
        };
        if let Some(field) = CoreOptions::new(self.table.schema().options()).rowkind_field() {
            routing_names.push(field.to_owned());
        }
        let routing_transport = |expected: &arrow_schema::Schema| {
            is_routing_transport(&actual_schema, expected, &routing_names)
        };
        let full_input = names_match(&full_schema);
        let candidates = std::iter::once(self.write_schema.as_ref())
            .chain(
                self.append_write_configs
                    .values()
                    .map(|config| config.schema.as_ref()),
            )
            .filter(|schema| names_match(schema) || routing_transport(schema))
            .collect::<Vec<_>>();
        let logical_schema = if full_input
            || names_match(&self.write_schema)
            || routing_transport(&self.write_schema)
        {
            self.write_schema.clone()
        } else if let Some(exact) = candidates
            .iter()
            .copied()
            .find(|schema| names_match(schema))
        {
            // A selected Arrow layout explicitly declares its logical fields.
            Arc::new(exact.clone())
        } else if candidates.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Input columns must match the table or a selected append write type, with optional routing columns".into(),
                source: None,
            });
        } else {
            // Historical types can differ only in routing/event fields while
            // admitting the same transport. Do not choose their logical NULL
            // checks by HashMap order: retain the common selected fields here,
            // then validate each destination's complete physical type below.
            let fields = self
                .table
                .schema()
                .fields()
                .iter()
                .filter(|field| {
                    candidates
                        .iter()
                        .all(|candidate| candidate.field_with_name(field.name()).is_ok())
                })
                .cloned()
                .collect::<Vec<_>>();
            build_target_arrow_schema(&fields)?
        };
        // Validate every provided transport field, without inspecting values of
        // omitted payloads. Row marshalling may add routing/event fields to a
        // captured subset, but arbitrary payload extras are not accepted.
        let transport_fields = actual_schema
            .fields()
            .iter()
            .map(|field| {
                self.table
                    .schema()
                    .fields()
                    .iter()
                    .find(|candidate| candidate.name() == field.name())
                    .cloned()
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!("Column '{}' is not in table schema", field.name()),
                        source: None,
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let transport_schema = build_target_arrow_schema(&transport_fields)?;
        self.normalize_batch_schema(&input.slice(0, 0), &transport_schema)?;
        if input.num_rows() == 0 {
            return Ok(());
        }
        let logical = project(input, &logical_schema)?;
        // The facade resolves events and checks its selected NOT NULL fields
        // before routing, as Java TableWriteImpl does. Ignored events must not
        // cast routing values or open Blob payloads.
        let filtered = self.filter_write_input_with_schema(input, &logical, &logical_schema)?;
        let input = filtered.as_ref().unwrap_or(input);
        if input.num_rows() == 0 {
            return Ok(());
        }
        let groups = if let Some(bucket) = bucket {
            let partitions = self.precomputed_partition_bytes(input, input)?;
            let mut groups: HashMap<PartitionBucketKey, Vec<usize>> = HashMap::new();
            for (row, partition) in partitions.into_iter().enumerate() {
                groups.entry((partition, bucket)).or_default().push(row);
            }
            groups
                .into_iter()
                .map(|(key, rows)| Ok((key, take_rows(input, &rows)?)))
                .collect::<Result<Vec<_>>>()?
        } else {
            let routing = self.normalize_routing_columns(input, &self.automatic_routing_names())?;
            self.divide_write_input(input, Some(routing)).await?
        };
        let mut prepared = Vec::with_capacity(groups.len());
        for (key, batch) in groups {
            let expected = self
                .append_write_configs
                .get(&key)
                .map(|config| &config.schema)
                .unwrap_or(&self.write_schema);
            let physical = if full_input || routing_transport(expected) {
                project(&batch, expected)?
            } else {
                // Selected inputs retain their named layout. Never reinterpret
                // equal-width input positionally for a previously opened writer.
                batch
            };
            if physical.num_rows() > 0 {
                prepared.push((key, self.normalize_batch_schema(&physical, expected)?));
            }
        }
        if bucket.is_some() && prepared.len() > 1 {
            return Err(crate::Error::DataInvalid {
                message: "A precomputed bucket group contained multiple partitions".into(),
                source: None,
            });
        }
        // Preflight every destination before creating files or modifying any
        // existing writer. A bad subset cannot partially stage a mixed batch.
        for ((partition, bucket), batch) in prepared {
            self.written = true;
            self.write_bucket(partition, bucket, batch).await?;
        }
        Ok(())
    }
}

fn project(batch: &RecordBatch, expected: &arrow_schema::Schema) -> Result<RecordBatch> {
    let indices = expected
        .fields()
        .iter()
        .map(|field| {
            batch
                .schema()
                .index_of(field.name())
                .map_err(|error| crate::Error::DataInvalid {
                    message: format!("Cannot project captured append write type: {error}"),
                    source: None,
                })
        })
        .collect::<Result<Vec<_>>>()?;
    batch
        .project(&indices)
        .map_err(|error| crate::Error::DataInvalid {
            message: format!("Cannot project captured append write type: {error}"),
            source: None,
        })
}

/// Routing/event fields may accompany a named subset. Equal-width reordered
/// subsets remain invalid Arrow layouts; only additional routing fields permit
/// projection, and every captured payload field must still be present.
fn is_routing_transport(
    actual: &arrow_schema::Schema,
    expected: &arrow_schema::Schema,
    routing_names: &[String],
) -> bool {
    if actual.fields().len() <= expected.fields().len() {
        return false;
    }
    let names = actual
        .fields()
        .iter()
        .map(|field| field.name())
        .collect::<HashSet<_>>();
    names.len() == actual.fields().len()
        && expected
            .fields()
            .iter()
            .all(|field| names.contains(field.name()))
        && actual.fields().iter().all(|field| {
            expected
                .fields()
                .iter()
                .any(|candidate| candidate.name() == field.name())
                || routing_names.contains(field.name())
        })
}
