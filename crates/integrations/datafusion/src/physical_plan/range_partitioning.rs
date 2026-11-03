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

use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Schema, TimeUnit};
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::{ScalarValue, SplitPoint};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, Partitioning, PhysicalSortExpr, RangePartitioning};
use paimon::spec::{BinaryRow, DataField};
use paimon::table::Table;
use paimon::DataSplit;

use super::scan::datum_to_scalar;

/// Group whole splits by their typed partition keys, then combine adjacent key
/// groups into at most the original number of execution partitions. Equal keys
/// must never straddle a boundary: DataFusion can use this declaration to omit
/// the exchange before a final aggregate or a co-partitioned join.
///
/// This describes distribution only. Files and rows within a range need not be
/// sorted. Failure to prove any part of the contract leaves the original scan
/// assignments and UnknownPartitioning intact.
pub(super) fn range_partitioned_splits(
    table: &Table,
    schema: &Schema,
    read_type: &[DataField],
    partitions: &[Arc<[DataSplit]>],
) -> Option<(Vec<Arc<[DataSplit]>>, Partitioning)> {
    let fields = table.schema().partition_fields();
    let options = table.schema().core_options();
    if fields.is_empty()
        || fields.len() != table.schema().partition_keys().len()
        || partitions.len() < 2
        || read_type.len() != schema.fields().len()
        || options.query_auth_enabled()
    {
        return None;
    }

    // Native PK value aggregation can change non-key partition columns. Those
    // values can remain in files even after the merge engine is changed, so the
    // current engine alone cannot prove range membership. Full PK columns are
    // immutable; format-table readers reconstruct partition columns from splits.
    let primary_keys = table.schema().primary_keys();
    if !options.is_format_table()
        && !primary_keys.is_empty()
        && table
            .schema()
            .partition_keys()
            .iter()
            .any(|key| !primary_keys.contains(key))
    {
        return None;
    }

    // Use the actual projected column positions. Variant extraction and omitted
    // partition keys must not be mistaken for an unchanged partition column.
    let indices = fields
        .iter()
        .map(|field| {
            let index = read_type.iter().position(|read| read == field)?;
            let output = schema.field(index);
            (output.name() == field.name() && supported_type(output.data_type())).then_some(index)
        })
        .collect::<Option<Vec<_>>>()?;
    let options = SortOptions::new(false, false); // ASC NULLS LAST
    let ordering = LexOrdering::new(indices.iter().map(|&index| {
        PhysicalSortExpr::new(
            Arc::new(Column::new(schema.field(index).name(), index)),
            options,
        )
    }))?;

    let splits = partitions
        .iter()
        .flat_map(|splits| splits.iter())
        .collect::<Vec<_>>();
    let keys = splits
        .iter()
        .map(|split| {
            let row = split.partition();
            if split.has_query_auth_rules()
                || row.arity() != fields.len() as i32
                || row.data().len() < BinaryRow::cal_fix_part_size_in_bytes(row.arity()) as usize
            {
                return None;
            }
            fields
                .iter()
                .zip(&indices)
                .enumerate()
                .map(|(pos, (field, &index))| {
                    let data_type = schema.field(index).data_type();
                    match row.get_datum(pos, field.data_type()).ok()? {
                        Some(value) => datum_to_scalar(value, data_type),
                        None => ScalarValue::try_from(data_type).ok(),
                    }
                })
                .collect::<Option<Vec<_>>>()
        })
        .collect::<Option<Vec<_>>>()?;
    if keys.is_empty() {
        return None;
    }

    // Arrow's row encoding gives exactly the comparison semantics used by the
    // engine, including typed numbers, UTF-8 strings, compound keys and NULLs.
    let converter = RowConverter::new(
        indices
            .iter()
            .map(|&index| {
                SortField::new_with_options(schema.field(index).data_type().clone(), options)
            })
            .collect(),
    )
    .ok()?;
    let columns = (0..fields.len())
        .map(|index| ScalarValue::iter_to_array(keys.iter().map(|key| key[index].clone())).ok())
        .collect::<Option<Vec<_>>>()?;
    let rows = converter.convert_columns(&columns).ok()?;
    let mut sorted = (0..splits.len()).collect::<Vec<_>>();
    sorted.sort_by(|&left, &right| rows.row(left).cmp(&rows.row(right)));
    let distinct = 1 + sorted
        .windows(2)
        .filter(|pair| rows.row(pair[0]) != rows.row(pair[1]))
        .count();
    let count = distinct.min(partitions.len());
    if count < 2 {
        return None;
    }

    let mut grouped = vec![Vec::new(); count];
    let mut split_points = Vec::with_capacity(count - 1);
    let mut key_index = 0;
    for (position, &index) in sorted.iter().enumerate() {
        if position > 0 && rows.row(sorted[position - 1]) != rows.row(index) {
            key_index += 1;
        }
        let partition = key_index * count / distinct;
        if partition > 0 && grouped[partition].is_empty() {
            // The first key of each upper partition is the inclusive boundary.
            split_points.push(SplitPoint::new(keys[index].clone()));
        }
        grouped[partition].push(splits[index].clone());
    }
    let range = RangePartitioning::try_new(ordering, split_points).ok()?;
    Some((
        grouped.into_iter().map(Arc::from).collect(),
        Partitioning::Range(range),
    ))
}

fn supported_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Utf8View
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Time32(TimeUnit::Millisecond)
            | DataType::Decimal128(_, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{
        DataType as PaimonType, Datum, IntType, Schema as PaimonSchema, TableSchema, TimeType,
    };
    use paimon::DataSplitBuilder;

    fn table(data_type: PaimonType, query_auth: bool) -> Table {
        let schema = PaimonSchema::builder()
            .column("pt", data_type)
            .partition_keys(["pt"])
            .option("query-auth.enabled", query_auth.to_string())
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("file").build().unwrap(),
            Identifier::new("test", "ranges"),
            "file:/tmp/ranges".to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn split(row: BinaryRow, bucket: i32) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(row)
            .with_bucket(bucket)
            .with_bucket_path("file:/tmp/ranges/bucket".to_string())
            .with_data_files(vec![])
            .build()
            .unwrap()
    }

    fn partitions(data_type: &PaimonType, values: &[Option<Datum>]) -> Vec<Arc<[DataSplit]>> {
        values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                Arc::from(vec![split(
                    BinaryRow::from_datums(&[(value.as_ref(), data_type)]),
                    i as i32,
                )])
            })
            .collect()
    }

    #[test]
    fn time_partitions_follow_millisecond_order_including_null() {
        // TIME cannot currently be created through SQLContext's DDL parser.
        let data_type = PaimonType::Time(TimeType::new(3).unwrap());
        let table = table(data_type.clone(), false);
        let schema = crate::table::datafusion_arrow_schema(table.schema().fields(), true).unwrap();
        let partitions = partitions(
            &data_type,
            &[Some(Datum::Time(86_399_999)), None, Some(Datum::Time(1))],
        );
        let (grouped, partitioning) =
            range_partitioned_splits(&table, &schema, table.schema().fields(), &partitions)
                .unwrap();
        assert_eq!(
            grouped
                .iter()
                .map(|group| group[0].bucket())
                .collect::<Vec<_>>(),
            vec![2, 0, 1]
        );
        let Partitioning::Range(range) = partitioning else {
            panic!("expected ranges")
        };
        assert_eq!(
            range.split_points()[0].values(),
            &[ScalarValue::Time32Millisecond(Some(86_399_999))]
        );
        assert_eq!(
            range.split_points()[1].values(),
            &[ScalarValue::Time32Millisecond(None)]
        );
    }

    #[test]
    fn missing_or_malformed_metadata_never_declares_ranges() {
        let data_type = PaimonType::Int(IntType::new());
        let table = table(data_type.clone(), false);
        let schema = crate::table::datafusion_arrow_schema(table.schema().fields(), true).unwrap();
        for row in [
            BinaryRow::new(0),
            BinaryRow::new(1),
            BinaryRow::from_bytes(1, vec![0; 9]),
        ] {
            let partitions = vec![
                Arc::from(vec![split(row, 0)]),
                Arc::from(vec![split(
                    BinaryRow::from_datums(&[(Some(&Datum::Int(2)), &data_type)]),
                    1,
                )]),
            ];
            assert!(range_partitioned_splits(
                &table,
                &schema,
                table.schema().fields(),
                &partitions
            )
            .is_none());
        }
    }

    #[test]
    fn uneven_key_groups_keep_every_split_in_exactly_one_range() {
        let data_type = PaimonType::Int(IntType::new());
        let table = table(data_type.clone(), false);
        let schema = crate::table::datafusion_arrow_schema(table.schema().fields(), true).unwrap();
        let splits = partitions(
            &data_type,
            &[5, 1, 4, 2, 3, 2, 4].map(|value| Some(Datum::Int(value))),
        );
        let partitions = crate::table::bucket_round_robin(
            splits
                .iter()
                .flat_map(|group| group.iter().cloned())
                .collect(),
            3,
        )
        .into_iter()
        .map(Arc::from)
        .collect::<Vec<_>>();
        let (grouped, partitioning) =
            range_partitioned_splits(&table, &schema, table.schema().fields(), &partitions)
                .unwrap();
        let buckets = grouped
            .iter()
            .map(|group| {
                let mut buckets = group.iter().map(DataSplit::bucket).collect::<Vec<_>>();
                buckets.sort();
                buckets
            })
            .collect::<Vec<_>>();
        assert_eq!(buckets, vec![vec![1, 3, 5], vec![2, 4, 6], vec![0]]);
        let Partitioning::Range(range) = partitioning else {
            panic!("expected ranges")
        };
        assert_eq!(
            range
                .split_points()
                .iter()
                .map(|point| point.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![ScalarValue::Int32(Some(3))],
                vec![ScalarValue::Int32(Some(5))]
            ]
        );
    }

    #[test]
    fn binary_keys_do_not_infer_ranges_from_signed_byte_order() {
        let data_type = PaimonType::VarBinary(paimon::spec::VarBinaryType::new(8).unwrap());
        let table = table(data_type.clone(), false);
        let schema = crate::table::datafusion_arrow_schema(table.schema().fields(), true).unwrap();
        let partitions = partitions(
            &data_type,
            &[
                Some(Datum::Bytes(vec![0x80])),
                Some(Datum::Bytes(vec![0x7f])),
            ],
        );
        assert!(
            range_partitioned_splits(&table, &schema, table.schema().fields(), &partitions)
                .is_none()
        );
    }

    #[test]
    fn authorization_and_single_key_groups_keep_original_assignments() {
        let data_type = PaimonType::Int(IntType::new());
        let unrestricted = table(data_type.clone(), false);
        let restricted = table(data_type.clone(), true);
        let schema =
            crate::table::datafusion_arrow_schema(unrestricted.schema().fields(), true).unwrap();
        let mixed = partitions(&data_type, &[Some(Datum::Int(1)), Some(Datum::Int(2))]);
        assert!(range_partitioned_splits(
            &restricted,
            &schema,
            restricted.schema().fields(),
            &mixed
        )
        .is_none());
        // Transported rules must also disable range claims when the table
        // handle itself has no query-auth option.
        let mut serialized = serde_json::to_value(&mixed[0][0]).unwrap();
        serialized["query_auth_response"] = serde_json::json!({"columnMasking": {"pt": "{}"}});
        let masked: DataSplit = serde_json::from_value(serialized).unwrap();
        assert!(masked.has_query_auth_rules());
        let masked = vec![Arc::from(vec![masked]), Arc::clone(&mixed[1])];
        assert!(range_partitioned_splits(
            &unrestricted,
            &schema,
            unrestricted.schema().fields(),
            &masked
        )
        .is_none());
        for values in [
            vec![Some(Datum::Int(1)), Some(Datum::Int(1))],
            vec![None, None],
        ] {
            let partitions = partitions(&data_type, &values);
            assert!(range_partitioned_splits(
                &unrestricted,
                &schema,
                unrestricted.schema().fields(),
                &partitions
            )
            .is_none());
        }
        let single = vec![Arc::from(
            mixed
                .iter()
                .flat_map(|group| group.iter().cloned())
                .collect::<Vec<_>>(),
        )];
        assert!(range_partitioned_splits(
            &unrestricted,
            &schema,
            unrestricted.schema().fields(),
            &single
        )
        .is_none());
    }
}
