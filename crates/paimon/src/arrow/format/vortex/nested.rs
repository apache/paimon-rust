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

//! Read complete ROWs without Vortex 0.75's recursive StructReader projection.
//! Each ROW has independent validity; a null child must not null its parent.

use super::CurrentThreadRuntime;
use crate::{Error, Result};
use std::sync::Arc;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::arrays::{ChunkedArray, DictArray, StructArray};
use vortex::array::dtype::{DType, FieldNames, StructFields};
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex::file::VortexFile;
use vortex::io::runtime::BlockingRuntime;
use vortex::layout::layouts::{chunked::Chunked, dict::Dict, struct_::Struct};
use vortex::layout::scan::{scan_builder::ScanBuilder, split_by::SplitBy};
use vortex::layout::segments::SegmentSource;
use vortex::layout::{LayoutChildType, LayoutRef};
use vortex::session::VortexSession;

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::DataInvalid {
        message: format!("Failed to read complete Vortex ROW: {error}"),
        source: None,
    }
}

pub(super) fn read_array(
    runtime: &CurrentThreadRuntime,
    session: &VortexSession,
    file: &VortexFile,
    names: &[String],
) -> Result<ArrayRef> {
    read_layout(
        runtime,
        session,
        file.segment_source(),
        file.footer().layout(),
        Some(names),
    )
}

fn projected_dtype(dtype: &DType, names: Option<&[String]>) -> Result<DType> {
    let Some(names) = names else {
        return Ok(dtype.clone());
    };
    let DType::Struct(fields, nullable) = dtype else {
        return Err(invalid("expected a root ROW"));
    };
    let types = names
        .iter()
        .map(|name| {
            fields
                .field(name.as_str())
                .ok_or_else(|| invalid(format!("missing root field {name}")))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DType::Struct(
        StructFields::new(
            FieldNames::from_iter(names.iter().map(String::as_str)),
            types,
        ),
        *nullable,
    ))
}

fn read_layout(
    runtime: &CurrentThreadRuntime,
    session: &VortexSession,
    source: Arc<dyn SegmentSource>,
    layout: &LayoutRef,
    roots: Option<&[String]>,
) -> Result<ArrayRef> {
    let dtype = projected_dtype(layout.dtype(), roots)?;
    if layout.is::<Struct>() {
        let DType::Struct(fields, _) = &dtype else {
            return Err(invalid("invalid struct layout"));
        };
        let DType::Struct(original, _) = layout.dtype() else {
            return Err(invalid("invalid struct layout"));
        };
        let nullable = layout.dtype().is_nullable();
        let validity = if nullable {
            Validity::Array(read_layout(
                runtime,
                session,
                source.clone(),
                &layout.child(0).map_err(invalid)?,
                None,
            )?)
        } else {
            Validity::NonNullable
        };
        let mut columns = Vec::with_capacity(fields.nfields());
        for name in fields.names().iter() {
            let index = original
                .find(name)
                .ok_or_else(|| invalid(format!("missing root field {name}")))?;
            let child = layout
                .child(index + usize::from(nullable))
                .map_err(invalid)?;
            columns.push(read_layout(runtime, session, source.clone(), &child, None)?);
        }
        return StructArray::try_new(
            fields.names().clone(),
            columns,
            usize::try_from(layout.row_count()).map_err(invalid)?,
            validity,
        )
        .map(IntoArray::into_array)
        .map_err(invalid);
    }
    if layout.is::<Chunked>() {
        let mut chunks = Vec::with_capacity(layout.nchildren());
        for index in 0..layout.nchildren() {
            chunks.push(read_layout(
                runtime,
                session,
                source.clone(),
                &layout.child(index).map_err(invalid)?,
                roots,
            )?);
        }
        return ChunkedArray::try_new(chunks, dtype)
            .map(IntoArray::into_array)
            .map_err(invalid);
    }
    if layout.is::<Dict>() {
        // Values use dictionary coordinates, codes use row coordinates.
        let values = read_layout(
            runtime,
            session,
            source.clone(),
            &layout.child(0).map_err(invalid)?,
            roots,
        )?;
        let codes = read_layout(
            runtime,
            session,
            source,
            &layout.child(1).map_err(invalid)?,
            None,
        )?;
        return DictArray::try_new(codes, values)
            .map(IntoArray::into_array)
            .map_err(invalid);
    }
    // Zone maps wrap data without changing its schema or row coordinates.
    for (index, kind) in layout.child_types().enumerate() {
        if matches!(kind, LayoutChildType::Transparent(_)) {
            let child = layout.child(index).map_err(invalid)?;
            if child.dtype() == layout.dtype() && child.row_count() == layout.row_count() {
                return read_layout(runtime, session, source, &child, roots);
            }
        }
    }
    let reader = layout
        .new_reader("".into(), source, session, &Default::default())
        .map_err(invalid)?;
    let tasks = ScanBuilder::new(session.clone(), reader)
        .with_concurrency(1)
        .with_split_by(SplitBy::RowCount(
            usize::try_from(layout.row_count()).map_err(invalid)?.max(1),
        ))
        .build()
        .map_err(invalid)?;
    let mut chunks = Vec::new();
    for task in tasks {
        if let Some(array) = runtime.block_on(task).map_err(invalid)? {
            chunks.push(array);
        }
    }
    let array = ChunkedArray::try_new(chunks, layout.dtype().clone())
        .map(IntoArray::into_array)
        .map_err(invalid)?;
    let Some(names) = roots else {
        return Ok(array);
    };
    // A flat root cannot skip decoding fields, but selecting from the decoded
    // array preserves validity and does not invoke layout projection.
    let mut ctx = session.create_execution_ctx();
    let rows = array.execute::<StructArray>(&mut ctx).map_err(invalid)?;
    let columns = names
        .iter()
        .map(|name| {
            rows.unmasked_field_by_name(name.as_str())
                .cloned()
                .map_err(invalid)
        })
        .collect::<Result<Vec<_>>>()?;
    StructArray::try_new(
        FieldNames::from_iter(names.iter().map(String::as_str)),
        columns,
        rows.len(),
        rows.validity().map_err(invalid)?,
    )
    .map(IntoArray::into_array)
    .map_err(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::array::dtype::arrow::FromArrowType;
    use vortex::io::session::RuntimeSessionExt;
    use vortex::layout::layouts::chunked::ChunkedLayout;
    use vortex::layout::segments::{SegmentFuture, SegmentId};
    use vortex::layout::IntoLayout;
    use vortex::layout::LayoutChildren;
    use vortex::VortexSessionDefault;

    #[derive(Clone)]
    struct Empty;
    impl SegmentSource for Empty {
        fn request(&self, _: SegmentId) -> SegmentFuture {
            panic!("empty layout must not read segments")
        }
    }
    impl LayoutChildren for Empty {
        fn to_arc(&self) -> Arc<dyn LayoutChildren> {
            Arc::new(self.clone())
        }
        fn child(&self, _: usize, _: &DType) -> vortex::error::VortexResult<LayoutRef> {
            panic!("empty layout has no children")
        }
        fn child_row_count(&self, _: usize) -> u64 {
            panic!("empty layout has no children")
        }
        fn nchildren(&self) -> usize {
            0
        }
    }

    #[test]
    fn empty_chunked_layout_preserves_projected_row_schema() {
        use arrow_schema::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Int32, true),
            Field::new(
                "profile",
                DataType::Struct(vec![Field::new("keep", DataType::Int32, true)].into()),
                true,
            ),
        ]));
        let layout =
            ChunkedLayout::new(0, DType::from_arrow(schema.clone()), Arc::new(Empty)).into_layout();
        let runtime = CurrentThreadRuntime::new();
        let session = VortexSession::default().with_handle(runtime.handle());
        let array = read_layout(
            &runtime,
            &session,
            Arc::new(Empty),
            &layout,
            Some(&["profile".into()]),
        )
        .unwrap();
        let schema = Arc::new(schema.project(&[1]).unwrap());
        let batch = super::super::vortex_array_to_record_batch(&session, array, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }
}
