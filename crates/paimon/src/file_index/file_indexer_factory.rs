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

use bytes::Bytes;

use crate::common::Options;
use crate::file_index::bitmap::writer::BitmapFileIndexWriter;
use crate::file_index::bitmap::BitmapFileIndexReader;
use crate::file_index::bloom_filter::{BloomFilterReader, BloomFilterWriter};
use crate::file_index::bsi::{BsiFileIndexReader, BsiFileIndexWriter};
use crate::file_index::file_index_reader::FileIndexReader;
use crate::file_index::file_index_writer::FileIndexWriter;
use crate::file_index::range_bitmap::writer::RangeBitmapFileIndexWriter;
use crate::file_index::range_bitmap::RangeBitmapFileIndexReader;
use crate::spec::DataType;
use crate::{Error, Result};

pub(crate) const BITMAP_INDEX: &str = "bitmap";
pub(crate) const BLOOM_FILTER_INDEX: &str = "bloom-filter";
pub(crate) const RANGE_BITMAP_INDEX: &str = "range-bitmap";
pub(crate) const BSI_INDEX: &str = "bsi";

struct FailOpenFileIndexReader;

impl FileIndexReader for FailOpenFileIndexReader {}

#[derive(Clone, Copy)]
enum BuiltinFileIndexer {
    Bitmap,
    BloomFilter,
    RangeBitmap,
    Bsi,
}

impl BuiltinFileIndexer {
    fn from_identifier(identifier: &str) -> Result<Self> {
        match identifier {
            BITMAP_INDEX => Ok(Self::Bitmap),
            BLOOM_FILTER_INDEX => Ok(Self::BloomFilter),
            RANGE_BITMAP_INDEX => Ok(Self::RangeBitmap),
            BSI_INDEX => Ok(Self::Bsi),
            _ => Err(Error::Unsupported {
                message: format!("Unknown file index identifier: {identifier}"),
            }),
        }
    }
}

/// Factory for the file index implementations built into this crate.
pub(crate) struct FileIndexerFactory;

impl FileIndexerFactory {
    /// Whether this identifier has a built-in reader.
    pub(crate) fn is_supported(identifier: &str) -> bool {
        matches!(
            identifier,
            BITMAP_INDEX | BLOOM_FILTER_INDEX | RANGE_BITMAP_INDEX | BSI_INDEX
        )
    }

    /// Reader support does not imply that the index can be generated.
    pub(crate) fn is_write_supported(identifier: &str) -> bool {
        matches!(
            identifier,
            BITMAP_INDEX | BLOOM_FILTER_INDEX | RANGE_BITMAP_INDEX | BSI_INDEX
        )
    }

    pub(crate) fn create_writer(
        identifier: &str,
        data_type: DataType,
        options: &Options,
    ) -> Result<Box<dyn FileIndexWriter>> {
        match BuiltinFileIndexer::from_identifier(identifier)? {
            BuiltinFileIndexer::Bitmap => Ok(Box::new(BitmapFileIndexWriter::try_new(
                data_type, options,
            )?)),
            BuiltinFileIndexer::BloomFilter => {
                Ok(Box::new(BloomFilterWriter::try_new(data_type, options)?))
            }
            BuiltinFileIndexer::RangeBitmap => Ok(Box::new(RangeBitmapFileIndexWriter::try_new(
                data_type, options,
            )?)),
            BuiltinFileIndexer::Bsi => {
                Ok(Box::new(BsiFileIndexWriter::try_new(data_type, options)?))
            }
        }
    }

    pub(crate) fn create_reader(
        identifier: &str,
        data_type: DataType,
        serialized: Bytes,
    ) -> Result<Box<dyn FileIndexReader>> {
        match BuiltinFileIndexer::from_identifier(identifier)? {
            BuiltinFileIndexer::Bitmap => Ok(Box::new(BitmapFileIndexReader::try_new(
                data_type, serialized,
            )?)),
            BuiltinFileIndexer::BloomFilter => {
                Ok(Box::new(BloomFilterReader::try_new(data_type, serialized)?))
            }
            BuiltinFileIndexer::RangeBitmap => {
                // File indexes are optional accelerators. Rust used to ignore
                // range-bitmap payloads entirely, so a payload written by a
                // newer Java version or damaged in storage must conservatively
                // disable pruning instead of turning a readable data file into
                // a query failure.
                Ok(
                    match RangeBitmapFileIndexReader::try_new(data_type, serialized) {
                        Ok(reader) => Box::new(reader),
                        Err(_) => Box::new(FailOpenFileIndexReader),
                    },
                )
            }
            BuiltinFileIndexer::Bsi => {
                Ok(match BsiFileIndexReader::try_new(data_type, serialized) {
                    Ok(reader) => Box::new(reader),
                    Err(_) => Box::new(FailOpenFileIndexReader),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::*;
    use crate::file_index::file_index_result::FileIndexResult;
    use crate::spec::{BinaryType, BooleanType, Datum, IntType, PredicateOperator};

    fn int_type() -> DataType {
        DataType::Int(IntType::new())
    }

    #[test]
    fn test_builtin_writers_track_empty_rows_consistently() {
        for identifier in [
            BITMAP_INDEX,
            BLOOM_FILTER_INDEX,
            RANGE_BITMAP_INDEX,
            BSI_INDEX,
        ] {
            assert!(FileIndexerFactory::is_write_supported(identifier));
            let mut writer =
                FileIndexerFactory::create_writer(identifier, int_type(), &Options::new()).unwrap();

            assert!(writer.empty(), "{identifier}");
            writer.serialized_bytes().unwrap();
            assert!(writer.empty(), "{identifier}");

            writer.write(None).unwrap();
            assert!(!writer.empty(), "{identifier}");
            writer.serialized_bytes().unwrap();
        }
    }

    #[test]
    fn test_factory_delegates_type_and_option_validation() {
        let mut bloom_options = Options::new();
        bloom_options.set("items", "0");
        assert!(matches!(
            FileIndexerFactory::create_writer(BLOOM_FILTER_INDEX, int_type(), &bloom_options),
            Err(Error::ConfigInvalid { .. })
        ));

        let mut bitmap_options = Options::new();
        bitmap_options.set("version", "1");
        assert!(matches!(
            FileIndexerFactory::create_writer(BITMAP_INDEX, int_type(), &bitmap_options),
            Err(Error::Unsupported { .. })
        ));

        assert!(matches!(
            FileIndexerFactory::create_writer(
                BITMAP_INDEX,
                DataType::Binary(BinaryType::new(4).unwrap()),
                &Options::new()
            ),
            Err(Error::Unsupported { .. })
        ));
        assert!(matches!(
            FileIndexerFactory::create_writer(
                BLOOM_FILTER_INDEX,
                DataType::Boolean(BooleanType::new()),
                &Options::new()
            ),
            Err(Error::Unsupported { .. })
        ));
    }

    #[test]
    fn test_unknown_identifier_is_rejected() {
        assert!(FileIndexerFactory::is_supported(RANGE_BITMAP_INDEX));
        assert!(FileIndexerFactory::is_write_supported(RANGE_BITMAP_INDEX));
        assert!(!FileIndexerFactory::is_write_supported("unknown"));
        assert!(matches!(
            FileIndexerFactory::create_writer("unknown", int_type(), &Options::new()),
            Err(Error::Unsupported { .. })
        ));
        assert!(matches!(
            FileIndexerFactory::create_reader("unknown", int_type(), Bytes::new()),
            Err(Error::Unsupported { .. })
        ));
    }

    #[test]
    fn test_writer_rejects_mismatched_datum() {
        for identifier in [
            BITMAP_INDEX,
            BLOOM_FILTER_INDEX,
            RANGE_BITMAP_INDEX,
            BSI_INDEX,
        ] {
            let mut writer =
                FileIndexerFactory::create_writer(identifier, int_type(), &Options::new()).unwrap();

            assert!(matches!(
                writer.write(Some(&Datum::Long(1))),
                Err(Error::DataInvalid { .. })
            ));
            assert!(writer.empty(), "{identifier}");
        }
    }

    #[test]
    fn test_range_bitmap_huge_cardinality_fails_open() {
        let mut dictionary = BytesMut::new();
        dictionary.put_i32(13);
        dictionary.put_u8(1);
        dictionary.put_i32(0);
        dictionary.put_i32(0);
        dictionary.put_i32(0);

        let mut serialized = BytesMut::new();
        serialized.put_i32(21);
        serialized.put_u8(1);
        serialized.put_i32(i32::MAX);
        serialized.put_i32(i32::MAX);
        serialized.put_i32(0);
        serialized.put_i32(0);
        serialized.put_i32(dictionary.len() as i32);
        serialized.extend_from_slice(&dictionary);
        let serialized = serialized.freeze();

        assert!(matches!(
            RangeBitmapFileIndexReader::try_new(int_type(), serialized.clone()),
            Err(Error::FileIndexFormatInvalid { .. })
        ));

        let reader =
            FileIndexerFactory::create_reader(RANGE_BITMAP_INDEX, int_type(), serialized).unwrap();
        assert_eq!(
            FileIndexResult::Remain,
            reader.evaluate("a", 0, &int_type(), PredicateOperator::Eq, &[Datum::Int(0)])
        );
    }

    #[test]
    fn test_range_bitmap_malformed_bsi_fails_open() {
        // Java V1 index for [1, 3, 5, 7, 9, null, null, 10]. Change its
        // declared slice count from three to one while leaving the BSI header
        // and payload otherwise intact.
        let mut serialized = hex::decode(concat!(
            "00000015010000000800000006000000010000000a000000420000000d010000",
            "0001000000040000001900000000010000000100000000000000000000000500",
            "00001400000004000000030000000500000007000000090000000a0000002201",
            "030000001300000018000000000000001600000016000000140000002a000000",
            "143b3000000100000500020000000400070000003a3000000100000000000200",
            "100000000100030007003a300000010000000000010010000000020003003a30",
            "000001000000000001001000000004000700"
        ))
        .unwrap();
        let outer_header_length = i32::from_be_bytes(serialized[0..4].try_into().unwrap()) as usize;
        let dictionary_length_offset = 4 + outer_header_length - 4;
        let dictionary_length = i32::from_be_bytes(
            serialized[dictionary_length_offset..dictionary_length_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let bsi_offset = 4 + outer_header_length + dictionary_length;
        serialized[bsi_offset + 5] = 1;
        let serialized = Bytes::from(serialized);

        assert!(matches!(
            RangeBitmapFileIndexReader::try_new(int_type(), serialized.clone()),
            Err(Error::FileIndexFormatInvalid { .. })
        ));

        let reader =
            FileIndexerFactory::create_reader(RANGE_BITMAP_INDEX, int_type(), serialized).unwrap();
        assert_eq!(
            FileIndexResult::Remain,
            reader.evaluate("a", 0, &int_type(), PredicateOperator::Eq, &[Datum::Int(1)])
        );
    }
}
