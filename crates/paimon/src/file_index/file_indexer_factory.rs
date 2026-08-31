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
use crate::file_index::file_index_reader::FileIndexReader;
use crate::file_index::file_index_writer::FileIndexWriter;
use crate::spec::DataType;
use crate::{Error, Result};

pub(crate) const BITMAP_INDEX: &str = "bitmap";
pub(crate) const BLOOM_FILTER_INDEX: &str = "bloom-filter";

#[derive(Clone, Copy)]
enum BuiltinFileIndexer {
    Bitmap,
    BloomFilter,
}

impl BuiltinFileIndexer {
    fn from_identifier(identifier: &str) -> Result<Self> {
        match identifier {
            BITMAP_INDEX => Ok(Self::Bitmap),
            BLOOM_FILTER_INDEX => Ok(Self::BloomFilter),
            _ => Err(Error::Unsupported {
                message: format!("Unknown file index identifier: {identifier}"),
            }),
        }
    }
}

/// Factory for the file index implementations built into this crate.
pub(crate) struct FileIndexerFactory;

impl FileIndexerFactory {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BinaryType, BooleanType, Datum, IntType};

    fn int_type() -> DataType {
        DataType::Int(IntType::new())
    }

    #[test]
    fn test_builtin_writers_track_empty_rows_consistently() {
        for identifier in [BITMAP_INDEX, BLOOM_FILTER_INDEX] {
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
        for identifier in [BITMAP_INDEX, BLOOM_FILTER_INDEX] {
            let mut writer =
                FileIndexerFactory::create_writer(identifier, int_type(), &Options::new()).unwrap();

            assert!(matches!(
                writer.write(Some(&Datum::Long(1))),
                Err(Error::DataInvalid { .. })
            ));
            assert!(writer.empty(), "{identifier}");
        }
    }
}
