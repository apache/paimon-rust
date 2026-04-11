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

pub(crate) mod filtering;
pub(crate) mod format;
mod reader;
pub(crate) mod schema_evolution;

pub use crate::arrow::reader::ArrowReaderBuilder;

use crate::spec::{
    ArrayType, BigIntType, BooleanType, DataField, DataType as PaimonDataType, DateType,
    DecimalType, DoubleType, FloatType, IntType, LocalZonedTimestampType, MapType, RowType,
    SmallIntType, TimeType, TimestampType, TinyIntType, VarBinaryType, VarCharType,
};
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use std::sync::Arc;

/// Converts a Paimon [`DataType`](PaimonDataType) to an Arrow [`DataType`](ArrowDataType).
pub fn paimon_type_to_arrow(dt: &PaimonDataType) -> crate::Result<ArrowDataType> {
    Ok(match dt {
        PaimonDataType::Boolean(_) => ArrowDataType::Boolean,
        PaimonDataType::TinyInt(_) => ArrowDataType::Int8,
        PaimonDataType::SmallInt(_) => ArrowDataType::Int16,
        PaimonDataType::Int(_) => ArrowDataType::Int32,
        PaimonDataType::BigInt(_) => ArrowDataType::Int64,
        PaimonDataType::Float(_) => ArrowDataType::Float32,
        PaimonDataType::Double(_) => ArrowDataType::Float64,
        PaimonDataType::VarChar(_) | PaimonDataType::Char(_) => ArrowDataType::Utf8,
        PaimonDataType::Binary(_) | PaimonDataType::VarBinary(_) => ArrowDataType::Binary,
        PaimonDataType::Date(_) => ArrowDataType::Date32,
        PaimonDataType::Time(_) => ArrowDataType::Time32(TimeUnit::Millisecond),
        PaimonDataType::Timestamp(t) => {
            ArrowDataType::Timestamp(timestamp_time_unit(t.precision())?, None)
        }
        PaimonDataType::LocalZonedTimestamp(t) => {
            ArrowDataType::Timestamp(timestamp_time_unit(t.precision())?, Some("UTC".into()))
        }
        PaimonDataType::Decimal(d) => {
            let p = u8::try_from(d.precision()).map_err(|_| crate::Error::Unsupported {
                message: "Decimal precision exceeds u8".to_string(),
            })?;
            let s = i8::try_from(d.scale() as i32).map_err(|_| crate::Error::Unsupported {
                message: "Decimal scale out of i8 range".to_string(),
            })?;
            ArrowDataType::Decimal128(p, s)
        }
        PaimonDataType::Array(a) => {
            let element_type = paimon_type_to_arrow(a.element_type())?;
            ArrowDataType::List(Arc::new(ArrowField::new(
                "element",
                element_type,
                a.element_type().is_nullable(),
            )))
        }
        PaimonDataType::Map(m) => {
            let key_type = paimon_type_to_arrow(m.key_type())?;
            let value_type = paimon_type_to_arrow(m.value_type())?;
            ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("key", key_type, false),
                            ArrowField::new("value", value_type, m.value_type().is_nullable()),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
        PaimonDataType::Multiset(m) => {
            let element_type = paimon_type_to_arrow(m.element_type())?;
            ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("key", element_type, m.element_type().is_nullable()),
                            ArrowField::new("value", ArrowDataType::Int32, false),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
        PaimonDataType::Row(r) => {
            let fields: Vec<ArrowField> = r
                .fields()
                .iter()
                .map(|f| {
                    let arrow_type = paimon_type_to_arrow(f.data_type())?;
                    Ok(ArrowField::new(
                        f.name(),
                        arrow_type,
                        f.data_type().is_nullable(),
                    ))
                })
                .collect::<crate::Result<Vec<_>>>()?;
            ArrowDataType::Struct(fields.into())
        }
    })
}

fn timestamp_time_unit(precision: u32) -> crate::Result<TimeUnit> {
    match precision {
        0..=3 => Ok(TimeUnit::Millisecond),
        4..=6 => Ok(TimeUnit::Microsecond),
        7..=9 => Ok(TimeUnit::Nanosecond),
        _ => Err(crate::Error::Unsupported {
            message: format!("Unsupported TIMESTAMP precision {precision}"),
        }),
    }
}

/// Convert an Arrow [`DataType`](ArrowDataType) to a Paimon [`DataType`](PaimonDataType).
pub fn arrow_to_paimon_type(
    arrow_type: &ArrowDataType,
    nullable: bool,
) -> crate::Result<PaimonDataType> {
    match arrow_type {
        ArrowDataType::Boolean => Ok(PaimonDataType::Boolean(BooleanType::with_nullable(
            nullable,
        ))),
        ArrowDataType::Int8 => Ok(PaimonDataType::TinyInt(TinyIntType::with_nullable(
            nullable,
        ))),
        ArrowDataType::Int16 => Ok(PaimonDataType::SmallInt(SmallIntType::with_nullable(
            nullable,
        ))),
        ArrowDataType::Int32 => Ok(PaimonDataType::Int(IntType::with_nullable(nullable))),
        ArrowDataType::Int64 => Ok(PaimonDataType::BigInt(BigIntType::with_nullable(nullable))),
        ArrowDataType::Float32 => Ok(PaimonDataType::Float(FloatType::with_nullable(nullable))),
        ArrowDataType::Float64 => Ok(PaimonDataType::Double(DoubleType::with_nullable(nullable))),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            Ok(PaimonDataType::VarChar(VarCharType::with_nullable(
                nullable,
                VarCharType::MAX_LENGTH,
            )?))
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView => Ok(
            PaimonDataType::VarBinary(VarBinaryType::try_new(nullable, VarBinaryType::MAX_LENGTH)?),
        ),
        ArrowDataType::Date32 => Ok(PaimonDataType::Date(DateType::with_nullable(nullable))),
        ArrowDataType::Timestamp(unit, tz) => {
            let precision = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            if tz.is_some() {
                Ok(PaimonDataType::LocalZonedTimestamp(
                    LocalZonedTimestampType::with_nullable(nullable, precision)?,
                ))
            } else {
                Ok(PaimonDataType::Timestamp(TimestampType::with_nullable(
                    nullable, precision,
                )?))
            }
        }
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => {
            let precision = match arrow_type {
                ArrowDataType::Time32(TimeUnit::Second) => 0,
                ArrowDataType::Time32(TimeUnit::Millisecond) => 3,
                ArrowDataType::Time64(TimeUnit::Microsecond) => 6,
                ArrowDataType::Time64(TimeUnit::Nanosecond) => 9,
                _ => 0,
            };
            Ok(PaimonDataType::Time(TimeType::with_nullable(
                nullable, precision,
            )?))
        }
        ArrowDataType::Decimal128(p, s) => Ok(PaimonDataType::Decimal(DecimalType::with_nullable(
            nullable, *p as u32, *s as u32,
        )?)),
        ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
            let element = arrow_to_paimon_type(field.data_type(), field.is_nullable())?;
            Ok(PaimonDataType::Array(ArrayType::with_nullable(
                nullable, element,
            )))
        }
        ArrowDataType::Map(entries_field, _) => {
            if let ArrowDataType::Struct(fields) = entries_field.data_type() {
                if fields.len() == 2 {
                    let key = arrow_to_paimon_type(fields[0].data_type(), fields[0].is_nullable())?;
                    let value =
                        arrow_to_paimon_type(fields[1].data_type(), fields[1].is_nullable())?;
                    return Ok(PaimonDataType::Map(MapType::with_nullable(
                        nullable, key, value,
                    )));
                }
            }
            Err(crate::Error::Unsupported {
                message: format!("Unsupported Map structure: {arrow_type:?}"),
            })
        }
        ArrowDataType::Struct(fields) => {
            let field_slice: Vec<ArrowField> = fields.iter().map(|f| f.as_ref().clone()).collect();
            let paimon_fields = arrow_fields_to_paimon(&field_slice)?;
            Ok(PaimonDataType::Row(RowType::with_nullable(
                nullable,
                paimon_fields,
            )))
        }
        _ => Err(crate::Error::Unsupported {
            message: format!("Unsupported Arrow type for Paimon conversion: {arrow_type:?}"),
        }),
    }
}

/// Convert Arrow fields to Paimon [`DataField`]s with auto-assigned IDs starting from 0.
pub fn arrow_fields_to_paimon(fields: &[ArrowField]) -> crate::Result<Vec<DataField>> {
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let paimon_type = arrow_to_paimon_type(f.data_type(), f.is_nullable())?;
            Ok(DataField::new(i as i32, f.name().clone(), paimon_type))
        })
        .collect()
}

/// Build an Arrow [`Schema`](ArrowSchema) from Paimon [`DataField`]s.
pub fn build_target_arrow_schema(fields: &[DataField]) -> crate::Result<Arc<ArrowSchema>> {
    let arrow_fields: Vec<ArrowField> = fields
        .iter()
        .map(|f| {
            let arrow_type = paimon_type_to_arrow(f.data_type())?;
            Ok(ArrowField::new(
                f.name(),
                arrow_type,
                f.data_type().is_nullable(),
            ))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(Arc::new(ArrowSchema::new(arrow_fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::*;

    /// Helper: paimon -> arrow -> paimon roundtrip, assert the arrow type matches expected.
    fn assert_paimon_to_arrow(paimon: &PaimonDataType, expected_arrow: &ArrowDataType) {
        let arrow = paimon_type_to_arrow(paimon).unwrap();
        assert_eq!(&arrow, expected_arrow, "paimon_type_to_arrow mismatch");
    }

    /// Helper: arrow -> paimon, assert the paimon type variant matches.
    fn assert_arrow_to_paimon(
        arrow: &ArrowDataType,
        nullable: bool,
        expected_paimon: &PaimonDataType,
    ) {
        let paimon = arrow_to_paimon_type(arrow, nullable).unwrap();
        assert_eq!(&paimon, expected_paimon, "arrow_to_paimon_type mismatch");
    }

    #[test]
    fn test_primitive_roundtrip() {
        let cases: Vec<(PaimonDataType, ArrowDataType)> = vec![
            (
                PaimonDataType::Boolean(BooleanType::new()),
                ArrowDataType::Boolean,
            ),
            (
                PaimonDataType::TinyInt(TinyIntType::new()),
                ArrowDataType::Int8,
            ),
            (
                PaimonDataType::SmallInt(SmallIntType::new()),
                ArrowDataType::Int16,
            ),
            (PaimonDataType::Int(IntType::new()), ArrowDataType::Int32),
            (
                PaimonDataType::BigInt(BigIntType::new()),
                ArrowDataType::Int64,
            ),
            (
                PaimonDataType::Float(FloatType::new()),
                ArrowDataType::Float32,
            ),
            (
                PaimonDataType::Double(DoubleType::new()),
                ArrowDataType::Float64,
            ),
            (PaimonDataType::Date(DateType::new()), ArrowDataType::Date32),
        ];
        for (paimon, arrow) in &cases {
            assert_paimon_to_arrow(paimon, arrow);
            assert_arrow_to_paimon(arrow, true, paimon);
        }
    }

    #[test]
    fn test_string_types() {
        let varchar = PaimonDataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap());
        assert_paimon_to_arrow(&varchar, &ArrowDataType::Utf8);

        // All string-like arrow types map to VarChar
        for arrow in &[
            ArrowDataType::Utf8,
            ArrowDataType::LargeUtf8,
            ArrowDataType::Utf8View,
        ] {
            assert_arrow_to_paimon(arrow, true, &varchar);
        }
    }

    #[test]
    fn test_binary_types() {
        let varbinary = PaimonDataType::VarBinary(
            VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).unwrap(),
        );
        assert_paimon_to_arrow(&varbinary, &ArrowDataType::Binary);

        for arrow in &[
            ArrowDataType::Binary,
            ArrowDataType::LargeBinary,
            ArrowDataType::BinaryView,
        ] {
            assert_arrow_to_paimon(arrow, true, &varbinary);
        }
    }

    #[test]
    fn test_timestamp_roundtrip() {
        // millisecond precision
        let ts3 = PaimonDataType::Timestamp(TimestampType::new(3).unwrap());
        assert_paimon_to_arrow(&ts3, &ArrowDataType::Timestamp(TimeUnit::Millisecond, None));
        assert_arrow_to_paimon(
            &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            true,
            &ts3,
        );

        // microsecond precision
        let ts6 = PaimonDataType::Timestamp(TimestampType::new(6).unwrap());
        assert_paimon_to_arrow(&ts6, &ArrowDataType::Timestamp(TimeUnit::Microsecond, None));
        assert_arrow_to_paimon(
            &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
            &ts6,
        );

        // nanosecond precision
        let ts9 = PaimonDataType::Timestamp(TimestampType::new(9).unwrap());
        assert_paimon_to_arrow(&ts9, &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None));
        assert_arrow_to_paimon(
            &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
            &ts9,
        );
    }

    #[test]
    fn test_local_zoned_timestamp() {
        let lzts = PaimonDataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap());
        let arrow = ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        assert_paimon_to_arrow(&lzts, &arrow);
        assert_arrow_to_paimon(&arrow, true, &lzts);
    }

    #[test]
    fn test_decimal_roundtrip() {
        let dec = PaimonDataType::Decimal(DecimalType::new(10, 2).unwrap());
        assert_paimon_to_arrow(&dec, &ArrowDataType::Decimal128(10, 2));
        assert_arrow_to_paimon(&ArrowDataType::Decimal128(10, 2), true, &dec);
    }

    #[test]
    fn test_array_roundtrip() {
        let paimon_arr = PaimonDataType::Array(ArrayType::new(PaimonDataType::Int(IntType::new())));
        let arrow_list = ArrowDataType::List(Arc::new(ArrowField::new(
            "element",
            ArrowDataType::Int32,
            true,
        )));
        assert_paimon_to_arrow(&paimon_arr, &arrow_list);

        // arrow -> paimon: element field name doesn't matter
        let arrow_list2 = ArrowDataType::List(Arc::new(ArrowField::new(
            "item",
            ArrowDataType::Int32,
            true,
        )));
        let result = arrow_to_paimon_type(&arrow_list2, true).unwrap();
        assert!(matches!(result, PaimonDataType::Array(_)));
    }

    #[test]
    fn test_map_roundtrip() {
        let paimon_map = PaimonDataType::Map(MapType::new(
            PaimonDataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
            PaimonDataType::Int(IntType::new()),
        ));
        let arrow_map = paimon_type_to_arrow(&paimon_map).unwrap();
        let back = arrow_to_paimon_type(&arrow_map, true).unwrap();
        assert!(matches!(back, PaimonDataType::Map(_)));
    }

    #[test]
    fn test_row_roundtrip() {
        let row = PaimonDataType::Row(RowType::new(vec![
            DataField::new(0, "a".to_string(), PaimonDataType::Int(IntType::new())),
            DataField::new(
                1,
                "b".to_string(),
                PaimonDataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
            ),
        ]));
        let arrow = paimon_type_to_arrow(&row).unwrap();
        let back = arrow_to_paimon_type(&arrow, true).unwrap();
        assert!(matches!(back, PaimonDataType::Row(_)));
    }

    #[test]
    fn test_not_nullable() {
        let paimon = arrow_to_paimon_type(&ArrowDataType::Int32, false).unwrap();
        assert!(!paimon.is_nullable());

        let paimon = arrow_to_paimon_type(&ArrowDataType::Int32, true).unwrap();
        assert!(paimon.is_nullable());
    }

    #[test]
    fn test_unsupported_arrow_type() {
        let result = arrow_to_paimon_type(&ArrowDataType::Duration(TimeUnit::Second), true);
        assert!(result.is_err());
    }

    #[test]
    fn test_arrow_fields_to_paimon_ids() {
        let fields = vec![
            ArrowField::new("x", ArrowDataType::Int32, true),
            ArrowField::new("y", ArrowDataType::Utf8, false),
        ];
        let paimon_fields = arrow_fields_to_paimon(&fields).unwrap();
        assert_eq!(paimon_fields.len(), 2);
        assert_eq!(paimon_fields[0].id(), 0);
        assert_eq!(paimon_fields[0].name(), "x");
        assert_eq!(paimon_fields[1].id(), 1);
        assert_eq!(paimon_fields[1].name(), "y");
        assert!(!paimon_fields[1].data_type().is_nullable());
    }
}
