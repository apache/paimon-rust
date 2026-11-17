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

mod date_extract;
pub(crate) use date_extract::evaluate_date_extract;
mod cast_width;
pub(crate) use cast_width::apply_declared_width;
mod cast_executor;
pub(crate) use cast_executor::CastExecutor;
#[cfg(test)]
mod cast_completion_java_test;
mod cast_text;
pub(crate) use cast_text::{
    parse_time as parse_java_time, parse_timestamp as parse_java_timestamp,
};
mod evolution_cast;
pub(crate) use evolution_cast::{cast_decimal_to_floating, cast_numeric, supports_numeric_cast};
mod evolution_decimal;
mod evolution_string;
mod evolution_temporal;
pub(crate) mod filtering;
pub(crate) mod format;
pub(crate) mod nested_evolution;
pub(crate) mod partition;
mod read_budget;
pub(crate) mod residual;
mod row_filter;
mod string_transform;
pub(crate) use string_transform::evaluate_string_transform;
pub(crate) mod schema_evolution;
pub use schema_evolution::cast_array_for_schema_evolution;
pub(crate) mod shredding;
mod variant;

pub use nested_evolution::{visible_generic_list, visible_map};
pub use read_budget::ReadBudget;
pub use row_filter::{RowFilter, RowFilterContext, RowFilterFactory};
pub use variant::{variant_get, variant_get_numeric_fields};

use crate::spec::{
    ArrayType, BigIntType, BinaryType, BooleanType, DataField, DataType as PaimonDataType,
    DateType, DecimalType, DoubleType, FloatType, IntType, LocalZonedTimestampType, MapType,
    RowType, SmallIntType, TimeType, TimestampType, TinyIntType, VarBinaryType, VarCharType,
    VariantType, VectorType,
};
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::{Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::sync::Arc;

const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";
const PAIMON_GENERATION_EXPRESSION_META_KEY: &str = "paimon:generationExpression";

/// Converts a Paimon [`DataType`](PaimonDataType) to an Arrow [`DataType`](ArrowDataType).
pub fn paimon_type_to_arrow(dt: &PaimonDataType) -> crate::Result<ArrowDataType> {
    paimon_arrow_type(dt, false)
}

fn paimon_arrow_type(dt: &PaimonDataType, for_read: bool) -> crate::Result<ArrowDataType> {
    let field = |name: &str, ty: &PaimonDataType| paimon_arrow_field(name, ty, for_read);
    Ok(match dt {
        PaimonDataType::Boolean(_) => ArrowDataType::Boolean,
        PaimonDataType::TinyInt(_) => ArrowDataType::Int8,
        PaimonDataType::SmallInt(_) => ArrowDataType::Int16,
        PaimonDataType::Int(_) => ArrowDataType::Int32,
        PaimonDataType::BigInt(_) => ArrowDataType::Int64,
        PaimonDataType::Float(_) => ArrowDataType::Float32,
        PaimonDataType::Double(_) => ArrowDataType::Float64,
        PaimonDataType::VarChar(_) | PaimonDataType::Char(_) => ArrowDataType::Utf8,
        PaimonDataType::Binary(_)
        | PaimonDataType::VarBinary(_)
        | PaimonDataType::Geometry(_)
        | PaimonDataType::Geography(_) => ArrowDataType::Binary,
        PaimonDataType::Blob(_) => ArrowDataType::LargeBinary,
        PaimonDataType::Variant(_) => variant_arrow_type(),
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
            ArrowDataType::List(Arc::new(field("element", a.element_type())?))
        }
        PaimonDataType::Map(m) => ArrowDataType::Map(
            Arc::new(ArrowField::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        field("key", m.key_type())?.with_nullable(false),
                        field("value", m.value_type())?,
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        PaimonDataType::Multiset(m) => ArrowDataType::Map(
            Arc::new(ArrowField::new(
                "entries",
                ArrowDataType::Struct(
                    vec![
                        field("key", m.element_type())?.with_nullable(false),
                        ArrowField::new("value", ArrowDataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        PaimonDataType::Row(r) => {
            let fields: Vec<ArrowField> = r
                .fields()
                .iter()
                .map(|f| {
                    Ok(with_generation_expression(
                        field(f.name(), f.data_type())?,
                        f,
                    ))
                })
                .collect::<crate::Result<Vec<_>>>()?;
            ArrowDataType::Struct(fields.into())
        }
        PaimonDataType::Vector(v) => {
            let element_type = paimon_type_to_arrow(v.element_type())?;
            // VectorType::MAX_LENGTH is i32::MAX as u32 (validated at construction),
            // so the length always fits in the i32 Arrow FixedSizeList size.
            let length = v.length() as i32;
            ArrowDataType::FixedSizeList(
                Arc::new(ArrowField::new(
                    "element",
                    element_type,
                    v.element_type().is_nullable(),
                )),
                length,
            )
        }
    })
}

fn timestamp_time_unit(precision: u32) -> crate::Result<TimeUnit> {
    match precision {
        0 => Ok(TimeUnit::Second),
        1..=3 => Ok(TimeUnit::Millisecond),
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
            let element = arrow_field_to_paimon_type(field)?;
            Ok(PaimonDataType::Array(ArrayType::with_nullable(
                nullable, element,
            )))
        }
        ArrowDataType::Map(entries_field, _) => {
            if let ArrowDataType::Struct(fields) = entries_field.data_type() {
                if fields.len() == 2 {
                    let key = arrow_field_to_paimon_type(&fields[0])?;
                    let value = arrow_field_to_paimon_type(&fields[1])?;
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
            if is_variant_arrow_fields(fields) && has_variant_arrow_field_ids(fields) {
                return Ok(PaimonDataType::Variant(VariantType::with_nullable(
                    nullable,
                )));
            }
            let paimon_fields = convert_arrow_fields(fields)?;
            Ok(PaimonDataType::Row(RowType::with_nullable(
                nullable,
                paimon_fields,
            )))
        }
        ArrowDataType::FixedSizeList(field, size) => {
            let element = arrow_to_paimon_type(field.data_type(), field.is_nullable())?;
            // FixedSizeList size is i32; reject non-positive sizes with a clear error
            // rather than casting a negative into a huge u32.
            let length = u32::try_from(*size).map_err(|_| crate::Error::DataTypeInvalid {
                message: format!("Invalid vector (FixedSizeList) length: {size}"),
            })?;
            Ok(PaimonDataType::Vector(VectorType::try_new(
                nullable, length, element,
            )?))
        }
        ArrowDataType::FixedSizeBinary(size) => {
            // Fixed-length binary maps to Paimon `BINARY(n)`. The size is an
            // i32; reject non-positive lengths with a clear error rather than
            // casting a negative into a huge usize.
            let length = usize::try_from(*size).map_err(|_| crate::Error::DataTypeInvalid {
                message: format!("Invalid FixedSizeBinary length: {size}"),
            })?;
            Ok(PaimonDataType::Binary(BinaryType::with_nullable(
                nullable, length,
            )?))
        }
        // A dictionary-encoded column has the logical type of its values; the
        // key (index) type is a physical encoding detail. DataFusion routinely
        // produces `Dictionary(Int32, Utf8)` for low-cardinality strings, so
        // resolving to the value type lets schema inference (e.g. CREATE VIEW)
        // succeed instead of failing as unsupported.
        ArrowDataType::Dictionary(_, value_type) => arrow_to_paimon_type(value_type, nullable),
        _ => Err(crate::Error::Unsupported {
            message: format!("Unsupported Arrow type for Paimon conversion: {arrow_type:?}"),
        }),
    }
}

pub fn variant_arrow_type() -> ArrowDataType {
    ArrowDataType::Struct(
        vec![
            arrow_field_with_paimon_id("value", ArrowDataType::Binary, false, 0),
            arrow_field_with_paimon_id("metadata", ArrowDataType::Binary, false, 1),
        ]
        .into(),
    )
}

pub(crate) fn is_variant_arrow_fields(fields: &arrow_schema::Fields) -> bool {
    fields.len() == 2
        && fields[0].name() == "value"
        && fields[0].data_type() == &ArrowDataType::Binary
        && fields[0].extension_type_name().is_none()
        && !fields[0].is_nullable()
        && fields[1].name() == "metadata"
        && fields[1].data_type() == &ArrowDataType::Binary
        && fields[1].extension_type_name().is_none()
        && !fields[1].is_nullable()
}

fn has_variant_arrow_field_ids(fields: &arrow_schema::Fields) -> bool {
    fields.len() == 2
        && arrow_field_id(&fields[0]) == Some(0)
        && arrow_field_id(&fields[1]) == Some(1)
}

fn arrow_field_id(field: &ArrowField) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)?
        .parse()
        .ok()
}

fn arrow_field_with_paimon_id(
    name: impl Into<String>,
    data_type: ArrowDataType,
    nullable: bool,
    id: i32,
) -> ArrowField {
    let mut metadata = HashMap::new();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
    ArrowField::new(name, data_type, nullable).with_metadata(metadata)
}

fn with_generation_expression(mut arrow_field: ArrowField, field: &DataField) -> ArrowField {
    if let Some(expression) = field.generation_expression() {
        arrow_field.metadata_mut().insert(
            PAIMON_GENERATION_EXPRESSION_META_KEY.to_string(),
            expression.to_string(),
        );
    }
    arrow_field
}

/// Convert Arrow fields to Paimon [`DataField`]s, preserving explicit field IDs
/// and generation expressions stored under `paimon:generationExpression`.
/// Fields without IDs receive positional IDs starting from 0.
pub fn arrow_fields_to_paimon(fields: &[ArrowField]) -> crate::Result<Vec<DataField>> {
    convert_arrow_fields(fields)
}

fn convert_arrow_fields(fields: &[impl Borrow<ArrowField>]) -> crate::Result<Vec<DataField>> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let field = field.borrow();
            Ok(DataField::new(
                arrow_field_id(field).unwrap_or(index as i32),
                field.name().clone(),
                arrow_field_to_paimon_type(field)?,
            )
            .with_generation_expression(
                field
                    .metadata()
                    .get(PAIMON_GENERATION_EXPRESSION_META_KEY)
                    .cloned(),
            ))
        })
        .collect()
}

/// Build an Arrow [`Schema`](ArrowSchema) from Paimon [`DataField`]s.
pub fn build_target_arrow_schema(fields: &[DataField]) -> crate::Result<Arc<ArrowSchema>> {
    build_arrow_schema(fields, false)
}

/// Read results can contain NULL after a Java cast overflows or a historical
/// file lacks a requested field, even when the current logical type is NOT NULL.
pub(crate) fn paimon_type_to_arrow_for_read(dt: &PaimonDataType) -> crate::Result<ArrowDataType> {
    paimon_arrow_type(dt, true)
}

/// Build the nullable runtime schema used by table readers.
pub fn build_read_arrow_schema(fields: &[DataField]) -> crate::Result<Arc<ArrowSchema>> {
    build_arrow_schema(fields, true)
}

fn build_arrow_schema(fields: &[DataField], for_read: bool) -> crate::Result<Arc<ArrowSchema>> {
    let fields = fields
        .iter()
        .map(|f| {
            let mut field = paimon_arrow_field(f.name(), f.data_type(), for_read)?;
            field
                .metadata_mut()
                .insert(PARQUET_FIELD_ID_META_KEY.into(), f.id().to_string());
            Ok(with_generation_expression(field, f))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(Arc::new(ArrowSchema::new(fields)))
}

pub(crate) const GEOARROW_WKB: &str = "geoarrow.wkb";

#[derive(serde::Serialize, serde::Deserialize)]
struct GeoArrowMetadata<C> {
    #[serde(skip_serializing_if = "Option::is_none")]
    edges: Option<String>,
    crs: Option<C>,
}

fn geoarrow_metadata_error(error: serde_json::Error) -> crate::Error {
    crate::Error::DataTypeInvalid {
        message: format!("Invalid geoarrow.wkb metadata: {error}"),
    }
}

/// Build an Arrow field, including extension metadata for logical types such as WKB.
pub fn paimon_type_to_arrow_field(name: &str, ty: &PaimonDataType) -> crate::Result<ArrowField> {
    paimon_arrow_field(name, ty, false)
}

fn paimon_arrow_field(
    name: &str,
    ty: &PaimonDataType,
    for_read: bool,
) -> crate::Result<ArrowField> {
    use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
    let mut field = ArrowField::new(
        name,
        paimon_arrow_type(ty, for_read)?,
        for_read || ty.is_nullable(),
    );
    let (crs, edges) = match ty {
        PaimonDataType::Geometry(g) => (g.crs(), None),
        PaimonDataType::Geography(g) => (g.crs(), Some(g.algorithm().to_string())),
        _ => return Ok(field),
    };
    // Java writes the original CRS as a string, including PROJJSON.
    let metadata = serde_json::to_string(&GeoArrowMetadata {
        edges,
        crs: Some(crs),
    })
    .map_err(geoarrow_metadata_error)?;
    field
        .metadata_mut()
        .insert(EXTENSION_TYPE_NAME_KEY.into(), GEOARROW_WKB.into());
    field
        .metadata_mut()
        .insert(EXTENSION_TYPE_METADATA_KEY.into(), metadata);
    Ok(field)
}

/// Convert an Arrow field, preserving its GeoArrow logical type when present.
pub fn arrow_field_to_paimon_type(field: &ArrowField) -> crate::Result<PaimonDataType> {
    if field.extension_type_name() != Some(GEOARROW_WKB) {
        return arrow_to_paimon_type(field.data_type(), field.is_nullable());
    }
    if !matches!(
        field.data_type(),
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView
    ) {
        return Err(crate::Error::DataTypeInvalid {
            message: format!("Invalid geoarrow.wkb storage type: {}", field.data_type()),
        });
    }
    let md: GeoArrowMetadata<&serde_json::value::RawValue> =
        serde_json::from_str(field.extension_type_metadata().unwrap_or("{}"))
            .map_err(geoarrow_metadata_error)?;
    // GeoArrow omits the CRS when unknown. Parquet represents an unknown CRS with srid:0.
    let crs = match md.crs.map(serde_json::value::RawValue::get) {
        None => "srid:0".into(),
        Some(s) if s.starts_with('"') => {
            serde_json::from_str(s).map_err(geoarrow_metadata_error)?
        }
        Some(object) if object.starts_with('{') => object.to_owned(),
        Some(value) => {
            return Err(crate::Error::DataTypeInvalid {
                message: format!("Invalid geoarrow.wkb CRS: {value}"),
            })
        }
    };
    let nullable = field.is_nullable();
    match md.edges.as_deref() {
        None | Some("planar") => Ok(PaimonDataType::Geometry(
            crate::spec::GeometryType::with_nullable(nullable, crs)?,
        )),
        Some(edges) => Ok(PaimonDataType::Geography(
            crate::spec::GeographyType::with_nullable(nullable, crs, edges.parse()?)?,
        )),
    }
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
    fn geospatial_arrow_crs_preserves_identifiers_and_projjson() {
        let projjson = r#" { "type": "GeographicCRS", "name": "Example" } "#;
        for crs in ["OGC:CRS84", "srid:0", projjson, "{custom"] {
            let ty = PaimonDataType::Geometry(crate::spec::GeometryType::new(crs).unwrap());
            let field = paimon_type_to_arrow_field("geometry", &ty).unwrap();
            assert_eq!(arrow_field_to_paimon_type(&field).unwrap(), ty);
            let metadata: serde_json::Value =
                serde_json::from_str(field.extension_type_metadata().unwrap()).unwrap();
            assert_eq!(metadata, serde_json::json!({ "crs": crs }));
            assert_eq!(field.metadata().len(), 2);
        }

        let mut field = ArrowField::new("geom", ArrowDataType::Binary, true);
        field.metadata_mut().insert(
            arrow_schema::extension::EXTENSION_TYPE_NAME_KEY.into(),
            GEOARROW_WKB.into(),
        );
        for (metadata, expected) in [
            (format!(r#"{{"crs":{projjson}}}"#), projjson.trim()),
            ("{}".to_owned(), "srid:0"),
        ] {
            field.metadata_mut().insert(
                arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY.into(),
                metadata,
            );
            assert_eq!(
                arrow_field_to_paimon_type(&field).unwrap(),
                PaimonDataType::Geometry(GeometryType::new(expected).unwrap())
            );
        }
    }

    #[test]
    fn geospatial_fields_with_variant_names_stay_a_row() {
        let geometry = PaimonDataType::Geometry(GeometryType::default());
        let binary = PaimonDataType::VarBinary(
            VarBinaryType::try_new(false, VarBinaryType::MAX_LENGTH).unwrap(),
        );
        let fields = [
            DataField::new(
                0,
                "value".into(),
                geometry.copy_with_nullable(false).unwrap(),
            ),
            DataField::new(1, "metadata".into(), binary),
        ];
        let row =
            ArrowDataType::Struct(build_target_arrow_schema(&fields).unwrap().fields().clone());
        assert_eq!(
            arrow_to_paimon_type(&row, true).unwrap(),
            PaimonDataType::Row(RowType::new(fields.to_vec()))
        );
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
        let binary = PaimonDataType::Binary(BinaryType::new(16).unwrap());
        let varbinary = PaimonDataType::VarBinary(
            VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).unwrap(),
        );
        assert_paimon_to_arrow(&binary, &ArrowDataType::Binary);
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
    fn map_and_multiset_keys_are_non_nullable_in_read_and_write_schemas() {
        for (data_type, value_nullable) in [
            (
                PaimonDataType::Map(MapType::new(
                    PaimonDataType::Int(IntType::new()),
                    PaimonDataType::Int(IntType::new()),
                )),
                true,
            ),
            (
                PaimonDataType::Multiset(crate::spec::MultisetType::new(PaimonDataType::Int(
                    IntType::new(),
                ))),
                false,
            ),
        ] {
            let fields = vec![DataField::new(0, "values".into(), data_type)];
            for schema in [
                build_target_arrow_schema(&fields).unwrap(),
                build_read_arrow_schema(&fields).unwrap(),
            ] {
                assert!(schema.field(0).is_nullable());
                let ArrowDataType::Map(entries, _) = schema.field(0).data_type() else {
                    panic!("MAP and MULTISET must both use Arrow Map storage");
                };
                assert!(!entries.is_nullable());
                let ArrowDataType::Struct(fields) = entries.data_type() else {
                    panic!("map entries must be a struct");
                };
                assert!(!fields[0].is_nullable());
                assert_eq!(fields[1].is_nullable(), value_nullable);
            }
        }
    }

    #[test]
    fn test_dictionary_resolves_to_value_type() {
        // A dictionary-encoded column carries the logical type of its values;
        // the key/index type is ignored. This is the common DataFusion case
        // (`arrow_cast(x, 'Dictionary(Int32, Utf8)')`).
        let varchar = PaimonDataType::VarChar(
            VarCharType::with_nullable(true, VarCharType::MAX_LENGTH).unwrap(),
        );
        assert_arrow_to_paimon(
            &ArrowDataType::Dictionary(
                Box::new(ArrowDataType::Int32),
                Box::new(ArrowDataType::Utf8),
            ),
            true,
            &varchar,
        );

        // The value type drives the result regardless of the key type.
        assert_arrow_to_paimon(
            &ArrowDataType::Dictionary(
                Box::new(ArrowDataType::Int8),
                Box::new(ArrowDataType::Int64),
            ),
            true,
            &PaimonDataType::BigInt(BigIntType::with_nullable(true)),
        );

        // Nullability is carried onto the resolved value type.
        let non_null_varchar = PaimonDataType::VarChar(
            VarCharType::with_nullable(false, VarCharType::MAX_LENGTH).unwrap(),
        );
        assert_arrow_to_paimon(
            &ArrowDataType::Dictionary(
                Box::new(ArrowDataType::Int32),
                Box::new(ArrowDataType::Utf8),
            ),
            false,
            &non_null_varchar,
        );
    }

    #[test]
    fn test_fixed_size_binary_maps_to_binary() {
        assert_arrow_to_paimon(
            &ArrowDataType::FixedSizeBinary(16),
            true,
            &PaimonDataType::Binary(BinaryType::with_nullable(true, 16).unwrap()),
        );

        // A zero (or negative) length is rejected rather than silently wrapping.
        assert!(arrow_to_paimon_type(&ArrowDataType::FixedSizeBinary(0), true).is_err());
    }

    #[test]
    fn test_blob_type_maps_one_way_to_arrow_large_binary() {
        let blob = PaimonDataType::Blob(BlobType::new());
        let varbinary = PaimonDataType::VarBinary(
            VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).unwrap(),
        );

        assert_paimon_to_arrow(&blob, &ArrowDataType::LargeBinary);
        assert_arrow_to_paimon(&ArrowDataType::Binary, true, &varbinary);

        let array = PaimonDataType::Array(ArrayType::new(blob.clone()));
        assert_paimon_to_arrow(
            &array,
            &ArrowDataType::List(Arc::new(ArrowField::new(
                "element",
                ArrowDataType::LargeBinary,
                true,
            ))),
        );

        let map = PaimonDataType::Map(MapType::new(
            PaimonDataType::VarChar(VarCharType::string_type()),
            blob,
        ));
        let map_arrow = paimon_type_to_arrow(&map).unwrap();
        let ArrowDataType::Map(entries, _) = map_arrow else {
            panic!("expected Arrow Map");
        };
        let ArrowDataType::Struct(fields) = entries.data_type() else {
            panic!("expected Arrow Map entries Struct");
        };
        assert_eq!(fields[1].data_type(), &ArrowDataType::LargeBinary);
    }

    #[test]
    fn test_variant_roundtrip() {
        let variant = PaimonDataType::Variant(VariantType::new());
        let arrow = variant_arrow_type();
        assert_paimon_to_arrow(&variant, &arrow);
        assert_arrow_to_paimon(&arrow, true, &variant);
    }

    #[test]
    fn test_plain_value_metadata_struct_stays_row() {
        let arrow = ArrowDataType::Struct(
            vec![
                ArrowField::new("value", ArrowDataType::Binary, false),
                ArrowField::new("metadata", ArrowDataType::Binary, false),
            ]
            .into(),
        );
        let paimon = arrow_to_paimon_type(&arrow, true).unwrap();

        assert!(matches!(paimon, PaimonDataType::Row(_)));
    }

    #[test]
    fn test_variant_arrow_field_ids_for_parquet() {
        let schema = build_target_arrow_schema(&[DataField::new(
            7,
            "payload".to_string(),
            PaimonDataType::Variant(VariantType::new()),
        )])
        .unwrap();
        let field = schema.field(0);
        assert_eq!(
            field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"7".to_string())
        );
        let ArrowDataType::Struct(fields) = field.data_type() else {
            panic!("expected variant Struct");
        };
        assert_eq!(
            fields[0].metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"0".to_string())
        );
        assert_eq!(
            fields[1].metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"1".to_string())
        );
    }

    #[test]
    fn test_timestamp_roundtrip() {
        // second precision
        let ts0 = PaimonDataType::Timestamp(TimestampType::new(0).unwrap());
        assert_paimon_to_arrow(&ts0, &ArrowDataType::Timestamp(TimeUnit::Second, None));
        assert_arrow_to_paimon(
            &ArrowDataType::Timestamp(TimeUnit::Second, None),
            true,
            &ts0,
        );

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
        let lzts0 = PaimonDataType::LocalZonedTimestamp(LocalZonedTimestampType::new(0).unwrap());
        let arrow0 = ArrowDataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
        assert_paimon_to_arrow(&lzts0, &arrow0);
        assert_arrow_to_paimon(&arrow0, true, &lzts0);

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
            DataField::new(0, "a".to_string(), PaimonDataType::Int(IntType::new()))
                .with_generation_expression(Some("LENGTH(b)".into())),
            DataField::new(
                1,
                "b".to_string(),
                PaimonDataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
            ),
            DataField::new(
                2,
                "geometry".into(),
                PaimonDataType::Geometry(GeometryType::new("EPSG:3857").unwrap()),
            )
            .with_generation_expression(Some("future_geometry(b)".into())),
            DataField::new(
                3,
                "geography".into(),
                PaimonDataType::Geography(
                    GeographyType::new("OGC:CRS84", EdgeAlgorithm::Karney).unwrap(),
                ),
            )
            .with_generation_expression(Some("future_geography(b)".into())),
        ]));
        let arrow = paimon_type_to_arrow(&row).unwrap();
        let back = arrow_to_paimon_type(&arrow, true).unwrap();
        assert_eq!(back, row);
        let read = paimon_type_to_arrow_for_read(&row).unwrap();
        assert_eq!(arrow_to_paimon_type(&read, true).unwrap(), row);
    }

    #[test]
    fn generated_column_arrow_schema_roundtrip() {
        let fields = vec![
            DataField::new(
                7,
                "source".into(),
                PaimonDataType::Int(IntType::with_nullable(false)),
            ),
            DataField::new(
                11,
                "generated".into(),
                PaimonDataType::Int(IntType::with_nullable(false)),
            )
            .with_generation_expression(Some("CAST(source AS INT)".into())),
            DataField::new(
                15,
                "geometry".into(),
                PaimonDataType::Geometry(GeometryType::with_nullable(false, "EPSG:3857").unwrap()),
            )
            .with_generation_expression(Some("future_geometry(source)".into())),
            DataField::new(
                19,
                "geography".into(),
                PaimonDataType::Geography(
                    GeographyType::with_nullable(false, "OGC:CRS84", EdgeAlgorithm::Karney)
                        .unwrap(),
                ),
            )
            .with_generation_expression(Some("future_geography(source)".into())),
        ];
        for (schema, nullable) in [
            (build_target_arrow_schema(&fields).unwrap(), false),
            (build_read_arrow_schema(&fields).unwrap(), true),
        ] {
            for (index, field) in fields.iter().enumerate() {
                assert_eq!(
                    schema
                        .field(index)
                        .metadata()
                        .get("paimon:generationExpression")
                        .map(String::as_str),
                    field.generation_expression()
                );
            }
            let arrow_fields: Vec<_> = schema
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect();
            let restored = arrow_fields_to_paimon(&arrow_fields).unwrap();
            let expected: Vec<_> = fields
                .iter()
                .cloned()
                .map(|field| {
                    let data_type = field.data_type().copy_with_nullable(nullable).unwrap();
                    field.with_data_type(data_type)
                })
                .collect();
            assert_eq!(restored, expected);
        }
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
    fn test_vector_to_arrow_nullable() {
        let paimon = PaimonDataType::Vector(
            VectorType::try_new(true, 128, PaimonDataType::Float(FloatType::new())).unwrap(),
        );
        let expected = ArrowDataType::FixedSizeList(
            Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
            128,
        );
        assert_paimon_to_arrow(&paimon, &expected);
    }

    #[test]
    fn test_vector_to_arrow_not_null_vector_has_float64_child() {
        // The vector's own non-nullability is not represented in the ArrowDataType;
        // only the child element type and length are.
        let paimon = PaimonDataType::Vector(
            VectorType::try_new(false, 2, PaimonDataType::Double(DoubleType::new())).unwrap(),
        );
        let arrow = paimon_type_to_arrow(&paimon).unwrap();
        match arrow {
            ArrowDataType::FixedSizeList(field, size) => {
                assert_eq!(size, 2);
                assert_eq!(field.data_type(), &ArrowDataType::Float64);
            }
            other => panic!("expected FixedSizeList, got {other:?}"),
        }
    }

    #[test]
    fn test_arrow_fixed_size_list_to_vector() {
        let arrow = ArrowDataType::FixedSizeList(
            Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
            4,
        );
        let paimon = arrow_to_paimon_type(&arrow, true).unwrap();
        match paimon {
            PaimonDataType::Vector(v) => {
                assert_eq!(v.length(), 4);
                assert_eq!(v.element_type(), &PaimonDataType::Float(FloatType::new()));
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn test_arrow_fixed_size_list_invalid_element_rejected() {
        let arrow = ArrowDataType::FixedSizeList(
            Arc::new(ArrowField::new("element", ArrowDataType::Utf8, true)),
            4,
        );
        let err = arrow_to_paimon_type(&arrow, true);
        assert!(matches!(err, Err(crate::Error::DataTypeInvalid { .. })));
    }

    #[test]
    fn test_arrow_fixed_size_list_zero_length_rejected() {
        let arrow = ArrowDataType::FixedSizeList(
            Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
            0,
        );
        let err = arrow_to_paimon_type(&arrow, true);
        assert!(matches!(err, Err(crate::Error::DataTypeInvalid { .. })));
    }

    #[test]
    fn test_arrow_fixed_size_list_negative_length_rejected() {
        // A negative FixedSizeList size IS directly constructible in Arrow, so it
        // exercises the `u32::try_from(*size)` negative branch in the conversion
        // (distinct from the zero case, which `VectorType::try_new` rejects).
        let arrow = ArrowDataType::FixedSizeList(
            Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
            -1,
        );
        let err = arrow_to_paimon_type(&arrow, true);
        assert!(matches!(err, Err(crate::Error::DataTypeInvalid { .. })));
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

#[cfg(test)]
pub(crate) mod cast_test_utils;
