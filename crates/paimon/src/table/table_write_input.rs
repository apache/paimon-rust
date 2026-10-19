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

//! Separate append input columns used for routing from columns written to files.

use super::*;
use crate::spec::batch_to_serialized_bytes;

pub(super) struct RoutingInput {
    batch: RecordBatch,
    fields: Vec<DataField>,
}

impl TableWrite {
    /// Accept the declared table layout as well as the selected write layout.
    /// The empty schema check intentionally does not inspect unwritten values:
    /// an omitted Blob must never open a URI or invoke a Blob consumer.
    pub(super) fn project_full_write_input(
        &self,
        input: &RecordBatch,
    ) -> Result<Option<RecordBatch>> {
        let fields = self.table.schema().fields();
        let names_match = |expected: &[DataField]| {
            input
                .schema()
                .fields()
                .iter()
                .map(|field| field.name())
                .eq(expected.iter().map(|field| field.name()))
        };
        if !self.table.schema().primary_keys().is_empty()
            || names_match(&self.write_fields)
            || !names_match(fields)
        {
            return Ok(None);
        }
        let full_schema = build_target_arrow_schema(fields)?;
        self.normalize_batch_schema(&input.slice(0, 0), &full_schema)?;
        let indices = self
            .write_fields
            .iter()
            .map(|field| input.schema().index_of(field.name()).unwrap())
            .collect::<Vec<_>>();
        input
            .project(&indices)
            .map(Some)
            .map_err(|error| crate::Error::DataInvalid {
                message: format!("Cannot project selected write columns: {error}"),
                source: None,
            })
    }

    fn normalize_routing_columns(
        &self,
        input: &RecordBatch,
        names: &[String],
    ) -> Result<RoutingInput> {
        let fields = names
            .iter()
            .map(|name| {
                self.table
                    .schema()
                    .fields()
                    .iter()
                    .find(|field| field.name() == name)
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>();
        let indices = names
            .iter()
            .map(|name| {
                input
                    .schema()
                    .index_of(name)
                    .map_err(|_| crate::Error::DataInvalid {
                        message: format!("Input must contain routing column '{name}'"),
                        source: None,
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = input
            .project(&indices)
            .map_err(|error| crate::Error::DataInvalid {
                message: format!("Cannot project routing columns: {error}"),
                source: None,
            })?;
        let schema = build_target_arrow_schema(&fields)?;
        // Nullability checks apply to the selected physical write type, as in
        // Java TableWriteImpl. An unwritten routing column may carry NULL.
        let schema = Arc::new(arrow_schema::Schema::new(
            schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_nullable(true))
                .collect::<Vec<_>>(),
        ));
        let batch = self.normalize_batch_schema(&batch, &schema)?;
        Ok(RoutingInput { batch, fields })
    }

    pub(super) fn normalize_automatic_routing_input(
        &self,
        input: &RecordBatch,
    ) -> Result<Option<RoutingInput>> {
        let mut names = self.partition_keys.clone();
        if CoreOptions::new(self.table.schema().options()).bucket() > 1 {
            names.extend(self.table.schema().bucket_keys());
        }
        if names
            .iter()
            .all(|name| self.write_fields.iter().any(|field| field.name() == name))
        {
            return Ok(None);
        }
        self.normalize_routing_columns(input, &names).map(Some)
    }

    /// Only append partial inputs need an alternate routing batch. These
    /// assigners have no persisted index state, so construct a local assigner
    /// using positions in that batch without replacing this writer's assigner.
    pub(super) async fn divide_write_input(
        &mut self,
        physical: &RecordBatch,
        routing: Option<RoutingInput>,
    ) -> Result<Vec<(PartitionBucketKey, RecordBatch)>> {
        let Some(routing) = routing else {
            return self.divide_by_partition_bucket(physical).await;
        };
        let indices = |names: &[String]| {
            names
                .iter()
                .map(|name| {
                    routing
                        .fields
                        .iter()
                        .position(|field| field.name() == name)
                        .unwrap()
                })
                .collect::<Vec<_>>()
        };
        let partitions = indices(&self.partition_keys);
        let options = CoreOptions::new(self.table.schema().options());
        let mut assigner = if options.bucket() > 1 {
            BucketAssignerEnum::Fixed(FixedBucketAssigner::new(
                partitions,
                indices(&self.table.schema().bucket_keys()),
                options.bucket_function_type()?,
                options.bucket(),
            ))
        } else {
            BucketAssignerEnum::Constant(ConstantBucketAssigner::new(
                partitions,
                if options.bucket() == POSTPONE_BUCKET {
                    POSTPONE_BUCKET
                } else {
                    0
                },
            ))
        };
        let output = assigner
            .assign_batch(&routing.batch, &routing.fields)
            .await?;
        Self::group_assigned_rows(physical, output)
    }

    pub(super) fn precomputed_partition_bytes(
        &self,
        input: &RecordBatch,
        physical: &RecordBatch,
    ) -> Result<Vec<Vec<u8>>> {
        if self.partition_keys.is_empty() {
            return Ok(vec![EMPTY_SERIALIZED_ROW.clone(); physical.num_rows()]);
        }
        // PK input may have been filtered by RowKind. Prefer its already
        // normalized rows; append-only partial input has no row filtering.
        let input = if self
            .partition_keys
            .iter()
            .all(|name| physical.column_by_name(name).is_some())
        {
            physical
        } else {
            input
        };
        let routing = self.normalize_routing_columns(input, &self.partition_keys)?;
        batch_to_serialized_bytes(
            &routing.batch,
            &(0..routing.fields.len()).collect::<Vec<_>>(),
            &routing.fields,
        )
    }
}
