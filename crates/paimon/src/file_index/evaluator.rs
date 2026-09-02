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

use std::collections::HashSet;

use bytes::Bytes;

use crate::file_index::file_index_predicate::FileIndexPredicate;
use crate::file_index::file_index_result::FileIndexResult;
use crate::file_index::FileIndexFormatReader;
use crate::io::FileIO;
use crate::spec::{DataField, DataFileMeta, DataType, Datum, Predicate, DATA_FILE_INDEX_SUFFIX};
use crate::Error;

/// Evaluate the usable data predicates against one data file's FileIndex.
pub(crate) async fn evaluate_file_index(
    file_io: &FileIO,
    bucket_path: &str,
    file: &DataFileMeta,
    table_fields: &[DataField],
    data_fields: &[DataField],
    predicates: &[Predicate],
) -> crate::Result<FileIndexResult> {
    if predicates.is_empty() || !(0..=i64::from(i32::MAX)).contains(&file.row_count) {
        return Ok(FileIndexResult::Remain);
    }

    let Some(predicate) = remap_predicates(table_fields, data_fields, predicates) else {
        return Ok(FileIndexResult::Remain);
    };
    let mut required_columns = HashSet::new();
    collect_required_columns(&predicate, &mut required_columns);
    if required_columns.is_empty() {
        return Ok(FileIndexResult::Remain);
    }

    let file_index = if let Some(embedded) = &file.embedded_index {
        FileIndexFormatReader::get_file_index_from_bytes(Bytes::copy_from_slice(embedded)).await?
    } else {
        let sidecars = file
            .extra_files
            .iter()
            .filter(|name| name.ends_with(DATA_FILE_INDEX_SUFFIX))
            .collect::<Vec<_>>();
        match sidecars.as_slice() {
            [] => return Ok(FileIndexResult::Remain),
            [sidecar] => {
                let path = file.aligned_file_path(bucket_path, sidecar);
                FileIndexFormatReader::get_file_index(file_io.new_input(&path)?).await?
            }
            _ => {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Found more than one index file for data file '{}': {}",
                        file.file_name,
                        sidecars
                            .iter()
                            .map(|name| name.as_str())
                            .collect::<Vec<_>>()
                            .join(" and ")
                    ),
                    source: None,
                })
            }
        }
    };

    let readers = file_index
        .create_index_readers(data_fields, &required_columns)
        .await?;
    if readers.values().all(Vec::is_empty) {
        return Ok(FileIndexResult::Remain);
    }
    Ok(FileIndexPredicate::new(readers).evaluate(&predicate))
}

fn remap_predicates(
    table_fields: &[DataField],
    data_fields: &[DataField],
    predicates: &[Predicate],
) -> Option<Predicate> {
    let remapped = predicates
        .iter()
        .filter_map(|predicate| remap_predicate(table_fields, data_fields, predicate))
        .collect::<Vec<_>>();
    (!remapped.is_empty()).then(|| Predicate::and(remapped))
}

fn remap_predicate(
    table_fields: &[DataField],
    data_fields: &[DataField],
    predicate: &Predicate,
) -> Option<Predicate> {
    match predicate {
        Predicate::Leaf {
            column,
            index,
            data_type,
            op,
            literals,
        } => {
            let table_field = table_fields.get(*index)?;
            if table_field.name() != column {
                return None;
            }
            let (data_index, data_field) = data_fields
                .iter()
                .enumerate()
                .find(|(_, field)| field.id() == table_field.id())?;
            let literals = devolve_literals(data_type, data_field.data_type(), literals)?;
            Some(Predicate::Leaf {
                column: data_field.name().to_string(),
                index: data_index,
                data_type: data_field.data_type().clone(),
                op: *op,
                literals,
            })
        }
        Predicate::And(children) => {
            let remapped = children
                .iter()
                .filter_map(|child| remap_predicate(table_fields, data_fields, child))
                .collect::<Vec<_>>();
            (!remapped.is_empty()).then(|| Predicate::and(remapped))
        }
        Predicate::Or(children) => {
            let remapped = children
                .iter()
                .map(|child| remap_predicate(table_fields, data_fields, child))
                .collect::<Option<Vec<_>>>()?;
            Some(Predicate::or(remapped))
        }
        Predicate::Not(inner) => {
            remap_predicate_exact(table_fields, data_fields, inner).map(Predicate::negate)
        }
        Predicate::AlwaysTrue => Some(Predicate::AlwaysTrue),
        Predicate::AlwaysFalse => Some(Predicate::AlwaysFalse),
    }
}

/// Remap only complete subtrees, as required below negation where widening is unsafe.
fn remap_predicate_exact(
    table_fields: &[DataField],
    data_fields: &[DataField],
    predicate: &Predicate,
) -> Option<Predicate> {
    match predicate {
        Predicate::And(children) => children
            .iter()
            .map(|child| remap_predicate_exact(table_fields, data_fields, child))
            .collect::<Option<Vec<_>>>()
            .map(Predicate::and),
        Predicate::Or(children) => children
            .iter()
            .map(|child| remap_predicate_exact(table_fields, data_fields, child))
            .collect::<Option<Vec<_>>>()
            .map(Predicate::or),
        Predicate::Not(inner) => {
            remap_predicate_exact(table_fields, data_fields, inner).map(Predicate::negate)
        }
        Predicate::Leaf { .. } | Predicate::AlwaysTrue | Predicate::AlwaysFalse => {
            remap_predicate(table_fields, data_fields, predicate)
        }
    }
}

fn devolve_literals(
    table_type: &DataType,
    data_type: &DataType,
    literals: &[Datum],
) -> Option<Vec<Datum>> {
    if same_type_ignoring_nullability(table_type, data_type) {
        return Some(literals.to_vec());
    }
    if !is_integer_type(table_type) || !is_integer_type(data_type) {
        return None;
    }
    literals
        .iter()
        .map(|literal| {
            let value = integer_value(table_type, literal)?;
            integer_datum(data_type, value)
        })
        .collect()
}

fn same_type_ignoring_nullability(left: &DataType, right: &DataType) -> bool {
    match (
        left.copy_with_nullable(true),
        right.copy_with_nullable(true),
    ) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn is_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::TinyInt(_) | DataType::SmallInt(_) | DataType::Int(_) | DataType::BigInt(_)
    )
}

fn integer_value(data_type: &DataType, datum: &Datum) -> Option<i64> {
    match (data_type, datum) {
        (DataType::TinyInt(_), Datum::TinyInt(value)) => Some(i64::from(*value)),
        (DataType::SmallInt(_), Datum::SmallInt(value)) => Some(i64::from(*value)),
        (DataType::Int(_), Datum::Int(value)) => Some(i64::from(*value)),
        (DataType::BigInt(_), Datum::Long(value)) => Some(*value),
        _ => None,
    }
}

fn integer_datum(data_type: &DataType, value: i64) -> Option<Datum> {
    match data_type {
        DataType::TinyInt(_) => i8::try_from(value).ok().map(Datum::TinyInt),
        DataType::SmallInt(_) => i16::try_from(value).ok().map(Datum::SmallInt),
        DataType::Int(_) => i32::try_from(value).ok().map(Datum::Int),
        DataType::BigInt(_) => Some(Datum::Long(value)),
        _ => None,
    }
}

fn collect_required_columns(predicate: &Predicate, columns: &mut HashSet<String>) {
    match predicate {
        Predicate::Leaf { column, .. } => {
            columns.insert(column.clone());
        }
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                collect_required_columns(child, columns);
            }
        }
        Predicate::Not(inner) => collect_required_columns(inner, columns),
        Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::common::Options;
    use crate::file_index::file_index_format::write_column_indexes;
    use crate::file_index::file_index_result::FileIndexResult;
    use crate::file_index::file_indexer_factory::{FileIndexerFactory, BITMAP_INDEX};
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BigIntType, FloatType, IntType, PredicateBuilder, PredicateOperator, VarCharType,
    };

    fn field(id: i32, name: &str, data_type: DataType) -> DataField {
        DataField::new(id, name.to_string(), data_type)
    }

    fn data_file(row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: "part-0.parquet".to_string(),
            file_size: 1,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            column_max_sequence_numbers: None,
        }
    }

    async fn bitmap_index_bytes(
        path: &str,
        column: &str,
        data_type: DataType,
        values: &[Datum],
    ) -> crate::Result<Bytes> {
        let mut writer =
            FileIndexerFactory::create_writer(BITMAP_INDEX, data_type, &Options::new())?;
        for value in values {
            writer.write(Some(value))?;
        }
        let indexes = HashMap::from([(
            column.to_string(),
            HashMap::from([(BITMAP_INDEX.to_string(), Some(writer.serialized_bytes()?))]),
        )]);
        write_column_indexes(path, indexes)
            .await?
            .to_input_file()
            .read()
            .await
    }

    fn assert_int_leaf(
        predicate: Predicate,
        expected_column: &str,
        expected_index: usize,
        expected_literal: i32,
    ) {
        assert!(matches!(
            predicate,
            Predicate::Leaf {
                column,
                index,
                data_type: DataType::Int(_),
                op: PredicateOperator::Eq,
                literals,
            } if column == expected_column
                && index == expected_index
                && literals == vec![Datum::Int(expected_literal)]
        ));
    }

    #[test]
    fn test_remap_predicate_uses_field_id_for_rename_reorder_and_integer_devolution() {
        let table_fields = vec![
            field(0, "new_id", DataType::BigInt(BigIntType::new())),
            field(1, "name", DataType::VarChar(VarCharType::new(20).unwrap())),
        ];
        let data_fields = vec![
            table_fields[1].clone(),
            field(0, "old_id", DataType::Int(IntType::new())),
        ];
        let predicate = PredicateBuilder::new(&table_fields)
            .equal("new_id", Datum::Long(42))
            .unwrap();

        let remapped = remap_predicate(&table_fields, &data_fields, &predicate).unwrap();

        assert_int_leaf(remapped, "old_id", 1, 42);
    }

    #[test]
    fn test_remap_predicate_falls_back_for_unsafe_schema_changes() {
        let table_fields = vec![
            field(0, "id", DataType::BigInt(BigIntType::new())),
            field(1, "added", DataType::Int(IntType::new())),
        ];
        let data_fields = vec![field(0, "id", DataType::Int(IntType::new()))];
        let builder = PredicateBuilder::new(&table_fields);

        assert!(remap_predicate(
            &table_fields,
            &data_fields,
            &builder
                .equal("id", Datum::Long(i64::from(i32::MAX) + 1))
                .unwrap(),
        )
        .is_none());
        assert!(remap_predicate(
            &table_fields,
            &data_fields,
            &builder.equal("added", Datum::Int(1)).unwrap(),
        )
        .is_none());

        let promoted_table = vec![field(0, "id", DataType::Float(FloatType::new()))];
        let promoted_predicate = PredicateBuilder::new(&promoted_table)
            .equal("id", Datum::Float(1.0))
            .unwrap();
        assert!(remap_predicate(&promoted_table, &data_fields, &promoted_predicate).is_none());
    }

    #[test]
    fn test_remap_predicate_keeps_safe_and_child_but_requires_complete_or_and_not() {
        let table_fields = vec![
            field(0, "id", DataType::Int(IntType::new())),
            field(1, "added", DataType::Int(IntType::new())),
        ];
        let data_fields = vec![table_fields[0].clone()];
        let builder = PredicateBuilder::new(&table_fields);
        let safe = builder.equal("id", Datum::Int(1)).unwrap();
        let unsafe_predicate = builder.equal("added", Datum::Int(2)).unwrap();

        let remapped_and = remap_predicate(
            &table_fields,
            &data_fields,
            &Predicate::and(vec![safe.clone(), unsafe_predicate.clone()]),
        )
        .unwrap();
        assert_int_leaf(remapped_and, "id", 0, 1);
        assert!(remap_predicate(
            &table_fields,
            &data_fields,
            &Predicate::or(vec![safe.clone(), unsafe_predicate.clone()]),
        )
        .is_none());
        assert!(remap_predicate(
            &table_fields,
            &data_fields,
            &Predicate::negate(unsafe_predicate.clone()),
        )
        .is_none());

        let exact_double_not = Predicate::Not(Box::new(Predicate::Not(Box::new(safe.clone()))));
        assert_int_leaf(
            remap_predicate(&table_fields, &data_fields, &exact_double_not).unwrap(),
            "id",
            0,
            1,
        );

        let nested_not = Predicate::negate(Predicate::and(vec![
            Predicate::negate(safe),
            unsafe_predicate,
        ]));
        assert!(remap_predicate(&table_fields, &data_fields, &nested_not).is_none());
    }

    #[tokio::test]
    async fn test_evaluator_prefers_embedded_index_over_ambiguous_sidecars() -> crate::Result<()> {
        let fields = vec![field(0, "id", DataType::Int(IntType::new()))];
        let bytes = bitmap_index_bytes(
            "memory:/evaluator_embedded_source",
            "id",
            fields[0].data_type().clone(),
            &[Datum::Int(1), Datum::Int(2)],
        )
        .await?;
        let mut file = data_file(2);
        file.embedded_index = Some(bytes.to_vec());
        file.extra_files = vec!["first.index".to_string(), "second.index".to_string()];
        let predicate = PredicateBuilder::new(&fields).equal("id", Datum::Int(2))?;
        let file_io = FileIOBuilder::new("memory").build()?;

        let result = evaluate_file_index(
            &file_io,
            "memory:/unused-bucket",
            &file,
            &fields,
            &fields,
            &[predicate],
        )
        .await?;

        assert_eq!(
            result,
            FileIndexResult::Selection([1_u32].into_iter().collect())
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluator_resolves_regular_and_external_sidecars() -> crate::Result<()> {
        let fields = vec![field(0, "id", DataType::Int(IntType::new()))];
        let bytes = bitmap_index_bytes(
            "memory:/evaluator_sidecar_source",
            "id",
            fields[0].data_type().clone(),
            &[Datum::Int(1), Datum::Int(2)],
        )
        .await?;
        let predicate = PredicateBuilder::new(&fields).equal("id", Datum::Int(1))?;
        let file_io = FileIOBuilder::new("memory").build()?;

        for (bucket_path, external_path, sidecar_path) in [
            (
                "memory:/regular/bucket-0",
                None,
                "memory:/regular/bucket-0/part-0.parquet.index",
            ),
            (
                "memory:/ignored/bucket-0",
                Some("memory:/external/data/part-0.parquet".to_string()),
                "memory:/external/data/part-0.parquet.index",
            ),
        ] {
            file_io
                .new_output(sidecar_path)?
                .write(bytes.clone())
                .await?;
            let mut file = data_file(2);
            file.extra_files = vec!["part-0.parquet.index".to_string()];
            file.external_path = external_path;

            let result = evaluate_file_index(
                &file_io,
                bucket_path,
                &file,
                &fields,
                &fields,
                std::slice::from_ref(&predicate),
            )
            .await?;
            assert_eq!(
                result,
                FileIndexResult::Selection([0_u32].into_iter().collect())
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluator_rejects_ambiguous_sidecars_without_embedded_index() -> crate::Result<()>
    {
        let fields = vec![field(0, "id", DataType::Int(IntType::new()))];
        let predicate = PredicateBuilder::new(&fields).equal("id", Datum::Int(1))?;
        let mut file = data_file(1);
        file.extra_files = vec![
            "first.index".to_string(),
            "notes.txt".to_string(),
            "second.index".to_string(),
        ];
        let file_io = FileIOBuilder::new("memory").build()?;

        let error = evaluate_file_index(
            &file_io,
            "memory:/bucket-0",
            &file,
            &fields,
            &fields,
            &[predicate],
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error,
            Error::DataInvalid { message, .. }
                if message.contains("first.index") && message.contains("second.index")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluator_absent_or_unsupported_index_remains() -> crate::Result<()> {
        let fields = vec![field(0, "id", DataType::Int(IntType::new()))];
        let predicate = PredicateBuilder::new(&fields).equal("id", Datum::Int(1))?;
        let file_io = FileIOBuilder::new("memory").build()?;
        let file = data_file(1);
        assert_eq!(
            evaluate_file_index(
                &file_io,
                "memory:/bucket-0",
                &file,
                &fields,
                &fields,
                std::slice::from_ref(&predicate),
            )
            .await?,
            FileIndexResult::Remain
        );

        let indexes = HashMap::from([(
            "id".to_string(),
            HashMap::from([(
                "range-bitmap".to_string(),
                Some(Bytes::from_static(b"unsupported payload")),
            )]),
        )]);
        let bytes = write_column_indexes("memory:/evaluator_unsupported_source", indexes)
            .await?
            .to_input_file()
            .read()
            .await?;
        let mut file = data_file(1);
        file.embedded_index = Some(bytes.to_vec());
        assert_eq!(
            evaluate_file_index(
                &file_io,
                "memory:/bucket-0",
                &file,
                &fields,
                &fields,
                &[predicate],
            )
            .await?,
            FileIndexResult::Remain
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_evaluator_row_count_boundaries_are_conservative() -> crate::Result<()> {
        let fields = vec![field(0, "id", DataType::Int(IntType::new()))];
        let predicate = PredicateBuilder::new(&fields).equal("id", Datum::Int(1))?;
        let file_io = FileIOBuilder::new("memory").build()?;

        for row_count in [-1, i64::from(i32::MAX) + 1] {
            let mut file = data_file(row_count);
            file.embedded_index = Some(vec![0]);
            assert_eq!(
                evaluate_file_index(
                    &file_io,
                    "memory:/bucket-0",
                    &file,
                    &fields,
                    &fields,
                    std::slice::from_ref(&predicate),
                )
                .await?,
                FileIndexResult::Remain
            );
        }

        for row_count in [0, i64::from(i32::MAX)] {
            let mut file = data_file(row_count);
            file.embedded_index = Some(vec![0]);
            assert!(matches!(
                evaluate_file_index(
                    &file_io,
                    "memory:/bucket-0",
                    &file,
                    &fields,
                    &fields,
                    std::slice::from_ref(&predicate),
                )
                .await,
                Err(Error::FileIndexFormatInvalid { .. })
            ));
        }
        Ok(())
    }
}
