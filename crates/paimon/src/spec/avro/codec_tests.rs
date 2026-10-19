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

use super::cursor::AvroCursor;
use super::ocf::{parse_ocf_streaming, OcfCodec};
use crate::spec::objects_file::new_avro_writer;
use apache_avro::types::Value;
use apache_avro::{Reader, Schema};

const FIXTURES: &[(&str, &[u8], OcfCodec)] = &[
    (
        "java/bzip2",
        include_bytes!("../../../testdata/avro_codec/java_bzip2.avro"),
        OcfCodec::Bzip2,
    ),
    (
        "java/xz",
        include_bytes!("../../../testdata/avro_codec/java_xz.avro"),
        OcfCodec::Xz,
    ),
    (
        "fastavro/bzip2",
        include_bytes!("../../../testdata/avro_codec/fastavro_bzip2.avro"),
        OcfCodec::Bzip2,
    ),
    (
        "fastavro/xz",
        include_bytes!("../../../testdata/avro_codec/fastavro_xz.avro"),
        OcfCodec::Xz,
    ),
];

fn expected_records() -> Vec<Value> {
    [
        (-7, "before-epoch".to_string(), vec![0, 1, 255]),
        (0, "\u{4e8c}".to_string(), vec![]),
        (
            i64::MAX,
            format!("end{}", "x".repeat(96)),
            (0..32).collect(),
        ),
    ]
    .into_iter()
    .map(|(id, label, payload)| {
        Value::Record(vec![
            ("id".to_string(), Value::Long(id)),
            ("label".to_string(), Value::String(label)),
            ("payload".to_string(), Value::Bytes(payload)),
        ])
    })
    .collect()
}

fn assert_blocks(bytes: &[u8], expected_codec: OcfCodec) {
    let (header, mut blocks) = parse_ocf_streaming(bytes).unwrap();
    assert_eq!(header.codec, expected_codec);
    let schema = Schema::parse_str(&header.schema_json).unwrap();
    let mut encoded_end = header.encoded_len;
    for expected in expected_records() {
        let block = blocks.next_block().unwrap().expect("missing OCF block");
        assert_eq!(block.object_count, 1);
        assert_eq!(block.encoded_offset, encoded_end);
        encoded_end += block.encoded_len;
        assert_eq!(&bytes[encoded_end - 16..encoded_end], &header.sync_marker);
        let mut data = block.data.as_ref();
        assert_eq!(
            apache_avro::from_avro_datum(&schema, &mut data, None).unwrap(),
            expected
        );
        assert!(data.is_empty(), "unconsumed bytes in decoded block");
    }
    assert!(blocks.next_block().unwrap().is_none());
    assert_eq!(encoded_end, bytes.len());

    // Both the streaming OCF reader and the reference reader must observe the
    // same records, rather than merely accepting the codec metadata.
    let reference = Reader::new(bytes)
        .unwrap()
        .collect::<apache_avro::AvroResult<Vec<_>>>()
        .unwrap();
    assert_eq!(reference, expected_records());

    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Record {
        id: i64,
        label: String,
        #[serde(with = "serde_bytes")]
        payload: Vec<u8>,
    }
    let generic = crate::spec::from_avro_bytes::<Record>(bytes).unwrap();
    assert_eq!(
        generic,
        vec![
            Record {
                id: -7,
                label: "before-epoch".to_string(),
                payload: vec![0, 1, 255],
            },
            Record {
                id: 0,
                label: "\u{4e8c}".to_string(),
                payload: vec![],
            },
            Record {
                id: i64::MAX,
                label: format!("end{}", "x".repeat(96)),
                payload: (0..32).collect(),
            },
        ]
    );
}

#[test]
fn test_bzip2_xz_java_and_fastavro_blocks() {
    for (name, bytes, codec) in FIXTURES {
        assert_eq!(
            parse_ocf_streaming(bytes).unwrap().0.sync_marker,
            std::array::from_fn(|i| i as u8),
            "{name}"
        );
        assert_blocks(bytes, *codec);
    }
}

#[test]
fn test_bzip2_xz_writer_multiple_blocks() {
    let schema =
        Schema::parse_str(include_str!("../../../testdata/avro_codec/records.avsc")).unwrap();
    for (compression, codec) in [
        ("bzip2", OcfCodec::Bzip2),
        ("xz", OcfCodec::Xz),
        ("BZIP2", OcfCodec::Bzip2),
        ("XZ", OcfCodec::Xz),
    ] {
        let mut writer = new_avro_writer(&schema, compression, 1).unwrap();
        for record in expected_records() {
            writer.append(record).unwrap();
        }
        assert_blocks(&writer.into_inner().unwrap(), codec);
    }
}

#[test]
fn test_bzip2_xz_public_reader_resolves_named_records() {
    #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    struct Child {
        id: i64,
    }
    #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    struct Parent {
        first: Child,
        second: Option<Child>,
    }
    let schema = Schema::parse_str(
        r#"{
            "type": "record", "name": "Parent", "fields": [
                {"name": "first", "type": {
                    "type": "record", "name": "Child",
                    "fields": [{"name": "id", "type": "long"}]
                }},
                {"name": "second", "type": ["null", "Child"]}
            ]
        }"#,
    )
    .unwrap();
    let expected = vec![
        Parent {
            first: Child { id: -7 },
            second: None,
        },
        Parent {
            first: Child { id: i64::MIN },
            second: Some(Child { id: i64::MAX }),
        },
    ];
    for compression in ["null", "deflate", "snappy", "zstd", "bzip2", "xz"] {
        let mut writer = new_avro_writer(&schema, compression, 1).unwrap();
        for record in &expected {
            writer
                .append(
                    apache_avro::to_value(record)
                        .unwrap()
                        .resolve(&schema)
                        .unwrap(),
                )
                .unwrap();
        }
        let bytes = writer.into_inner().unwrap();
        assert_eq!(
            crate::spec::from_avro_bytes::<Parent>(&bytes).unwrap(),
            expected,
            "{compression}"
        );
    }
}

#[test]
fn test_bzip2_xz_public_reader_empty_container_and_oversized_count() {
    for (name, bytes, _) in FIXTURES {
        let (header, _) = parse_ocf_streaming(bytes).unwrap();
        assert!(
            crate::spec::from_avro_bytes::<serde_json::Value>(&bytes[..header.encoded_len])
                .unwrap()
                .is_empty()
        );

        let mut cursor = AvroCursor::new(&bytes[header.encoded_len..]);
        cursor.read_long().unwrap();
        let mut changed = bytes[..header.encoded_len].to_vec();
        write_long(i64::MAX as usize, &mut changed);
        changed.extend_from_slice(&bytes[header.encoded_len + cursor.position()..]);
        // The untrusted count must not cause a capacity-overflow allocation.
        // Decode the actual payload and fail when the block runs out of data.
        assert!(
            crate::spec::from_avro_bytes::<serde::de::IgnoredAny>(&changed).is_err(),
            "{name}"
        );
    }
}

fn write_long(value: usize, output: &mut Vec<u8>) {
    let mut encoded = (value as u64) << 1;
    while encoded >= 0x80 {
        output.push((encoded as u8) | 0x80);
        encoded >>= 7;
    }
    output.push(encoded as u8);
}

fn replace_first_payload(bytes: &[u8], update: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let (header, _) = parse_ocf_streaming(bytes).unwrap();
    let mut cursor = AvroCursor::new(&bytes[header.encoded_len..]);
    let count = cursor.read_long().unwrap() as usize;
    let size = cursor.read_long().unwrap() as usize;
    let mut payload = cursor.read_fixed(size).unwrap().to_vec();
    update(&mut payload);
    let mut changed = bytes[..header.encoded_len].to_vec();
    write_long(count, &mut changed);
    write_long(payload.len(), &mut changed);
    changed.extend_from_slice(&payload);
    // Preserve the rest of the container, including the original sync markers.
    changed.extend_from_slice(&bytes[header.encoded_len + cursor.position()..]);
    changed
}

#[test]
fn test_bzip2_xz_corrupt_and_truncated_compressed_blocks_return_errors() {
    for (name, bytes, _) in FIXTURES {
        for damage in [0, 1, 2] {
            let corrupted = replace_first_payload(bytes, |payload| match damage {
                0 => payload.clear(),
                1 => payload.truncate(payload.len() / 2),
                2 => payload[0] ^= 0xff,
                _ => unreachable!(),
            });
            let (_, mut blocks) = parse_ocf_streaming(&corrupted).unwrap();
            let error = blocks
                .next_block()
                .err()
                .expect("invalid codec block accepted");
            assert!(
                error.to_string().contains("decompression failed"),
                "{name}, damage {damage}: {error}"
            );
            let generic_error = crate::spec::from_avro_bytes::<serde::de::IgnoredAny>(&corrupted)
                .expect_err("public OCF reader accepted invalid codec block");
            assert!(
                generic_error.to_string().contains("decompression failed"),
                "public OCF reader, {name}, damage {damage}: {generic_error}"
            );
        }
    }
}

#[test]
fn test_bzip2_xz_container_truncation_and_sync_mismatch_return_errors() {
    for (name, bytes, _) in FIXTURES {
        let (_, mut blocks) = parse_ocf_streaming(&bytes[..bytes.len() - 1]).unwrap();
        assert!(blocks.next_block().unwrap().is_some());
        assert!(blocks.next_block().unwrap().is_some());
        assert!(blocks.next_block().is_err(), "{name}");

        let mut corrupted = bytes.to_vec();
        *corrupted.last_mut().unwrap() ^= 1;
        let (_, mut blocks) = parse_ocf_streaming(&corrupted).unwrap();
        assert!(blocks.next_block().unwrap().is_some());
        assert!(blocks.next_block().unwrap().is_some());
        assert!(
            blocks
                .next_block()
                .err()
                .unwrap()
                .to_string()
                .contains("sync marker mismatch"),
            "{name}"
        );
    }
}
