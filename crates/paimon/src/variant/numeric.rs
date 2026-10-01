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

use super::*;

pub(crate) struct VariantFloat32Projection {
    fields: HashMap<usize, ProjectedField>,
    output_width: usize,
    last_layout: Option<ObjectLayoutProjection>,
}

struct ObjectLayoutProjection {
    field_count: usize,
    id_size: usize,
    ids: Vec<u8>,
    selected: Vec<(usize, usize)>,
}

struct ProjectedField {
    name: String,
    outputs: Vec<usize>,
}

impl VariantFloat32Projection {
    pub(crate) fn new(metadata: &[u8], fields: &[String]) -> Result<Self> {
        validate_metadata(metadata)?;
        let mut requested = HashMap::<&str, Vec<usize>>::new();
        for (index, field) in fields.iter().enumerate() {
            requested.entry(field).or_default().push(index);
        }

        let offset_size = metadata_offset_size(metadata)?;
        let dict_size = read_unsigned(metadata, 1, offset_size)?;
        let mut projected = HashMap::with_capacity(fields.len());
        for id in 0..dict_size {
            let name = get_metadata_key_ref(metadata, id)?;
            if let Some(outputs) = requested.get(name) {
                projected.insert(
                    id,
                    ProjectedField {
                        name: name.to_string(),
                        outputs: outputs.clone(),
                    },
                );
            }
        }
        Ok(Self {
            fields: projected,
            output_width: fields.len(),
            last_layout: None,
        })
    }

    pub(crate) fn extract_float32(
        &mut self,
        value: &[u8],
        metadata: &[u8],
        offsets: &mut Vec<usize>,
        output: &mut [Option<f32>],
    ) -> Result<()> {
        if output.len() != self.output_width {
            return data_invalid("Invalid Variant numeric output width");
        }
        output.fill(None);
        if value_kind(value, 0)? != VariantKind::Object {
            validate_payload(value, metadata)?;
            return Ok(());
        }

        let layout = object_layout(value, 0)?;
        offsets.clear();
        offsets.reserve(layout.size + 1);
        for index in 0..=layout.size {
            offsets.push(read_unsigned(
                value,
                layout.offset_start + layout.offset_size * index,
                layout.offset_size,
            )?);
        }
        let data_size = offsets[layout.size];
        if layout.data_start.checked_add(data_size) != Some(value.len()) {
            return data_invalid("Malformed Variant root size");
        }

        let monotonic =
            offsets.first() == Some(&0) && offsets.windows(2).all(|pair| pair[0] < pair[1]);
        let sorted_offsets = if monotonic {
            None
        } else {
            let mut sorted = offsets.clone();
            sorted.sort_unstable();
            if sorted.first() != Some(&0)
                || sorted.last() != Some(&data_size)
                || sorted.windows(2).any(|pair| pair[0] == pair[1])
            {
                return data_invalid("Malformed Variant object offsets");
            }
            Some(sorted)
        };

        self.prepare_layout(value, metadata, &layout)?;
        for (index, id) in &self.last_layout.as_ref().unwrap().selected {
            let projected = self.fields.get(id).unwrap();
            let start = offsets[*index];
            let end = match &sorted_offsets {
                None => offsets[*index + 1],
                Some(sorted) => {
                    let next = sorted.partition_point(|offset| *offset <= start);
                    *sorted.get(next).ok_or_else(|| Error::DataInvalid {
                        message: "Malformed Variant object offsets".to_string(),
                        source: None,
                    })?
                }
            };
            let child_pos =
                layout
                    .data_start
                    .checked_add(start)
                    .ok_or_else(|| Error::DataInvalid {
                        message: "Malformed Variant object offsets".to_string(),
                        source: None,
                    })?;
            let child_size = validate_value(value, metadata, child_pos, 1)?;
            if child_size != end - start {
                return data_invalid("Malformed Variant child size");
            }
            let child = VariantRef::new_at(value, metadata, child_pos)?;
            let numeric = numeric_to_float32(child, &projected.name)?;
            for output_index in &projected.outputs {
                output[*output_index] = numeric;
            }
        }
        Ok(())
    }

    fn prepare_layout(
        &mut self,
        value: &[u8],
        metadata: &[u8],
        layout: &ObjectLayout,
    ) -> Result<()> {
        let ids = &value[layout.id_start..layout.offset_start];
        if self.last_layout.as_ref().is_some_and(|cached| {
            cached.field_count == layout.size
                && cached.id_size == layout.id_size
                && cached.ids == ids
        }) {
            return Ok(());
        }

        let mut previous_key = None;
        let mut selected = Vec::with_capacity(self.fields.len());
        for index in 0..layout.size {
            let id = read_unsigned(
                value,
                layout.id_start + layout.id_size * index,
                layout.id_size,
            )?;
            let key = get_metadata_key_ref(metadata, id)?;
            if previous_key
                .is_some_and(|previous| java_string_cmp(previous, key) != std::cmp::Ordering::Less)
            {
                return data_invalid("Malformed Variant object key order");
            }
            previous_key = Some(key);
            if self.fields.contains_key(&id) {
                selected.push((index, id));
            }
        }
        self.last_layout = Some(ObjectLayoutProjection {
            field_count: layout.size,
            id_size: layout.id_size,
            ids: ids.to_vec(),
            selected,
        });
        Ok(())
    }
}

fn numeric_to_float32(value: VariantRef<'_>, field: &str) -> Result<Option<f32>> {
    Ok(match value.kind()? {
        VariantKind::Null => None,
        VariantKind::Long => Some(value.get_long()? as f32),
        VariantKind::Float => Some(value.get_float()?),
        VariantKind::Double => Some(value.get_double()? as f32),
        VariantKind::Decimal => {
            let decimal = value.get_decimal()?;
            Some((decimal.unscaled as f64 / 10f64.powi(decimal.scale as i32)) as f32)
        }
        kind => {
            return Err(Error::Unsupported {
                message: format!("Variant field '{field}' has non-numeric type {kind:?}"),
            });
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_non_monotonic_object_offsets() {
        let variant = GenericVariant::parse_json(r#"{"a":1,"b":2}"#).unwrap();
        let mut value = variant.value().to_vec();
        let layout = object_layout(&value, 0).unwrap();
        let first = read_unsigned(&value, layout.offset_start, layout.offset_size).unwrap();
        let second = read_unsigned(
            &value,
            layout.offset_start + layout.offset_size,
            layout.offset_size,
        )
        .unwrap();
        write_le_at(&mut value, layout.offset_start, second, layout.offset_size);
        write_le_at(
            &mut value,
            layout.offset_start + layout.offset_size,
            first,
            layout.offset_size,
        );
        validate_payload(&value, variant.metadata()).unwrap();

        let fields = vec!["a".to_string(), "b".to_string(), "a".to_string()];
        let mut projection = VariantFloat32Projection::new(variant.metadata(), &fields).unwrap();
        let mut offsets = Vec::new();
        let mut output = vec![None; fields.len()];
        projection
            .extract_float32(&value, variant.metadata(), &mut offsets, &mut output)
            .unwrap();
        assert_eq!(output, vec![Some(2.0), Some(1.0), Some(2.0)]);
    }
}
