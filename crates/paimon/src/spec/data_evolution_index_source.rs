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

//! Java DataEvolutionIndexSourceMeta, carried by GlobalIndexMeta.source_meta.

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataEvolutionIndexSourceMeta {
    scan_snapshot_id: i64,
}

impl DataEvolutionIndexSourceMeta {
    const MAGIC: [u8; 4] = *b"DEIX";
    const VERSION: i32 = 1;

    pub fn new(scan_snapshot_id: i64) -> Result<Self> {
        if scan_snapshot_id <= 0 {
            return Err(invalid("Scan snapshot id must be positive."));
        }
        Ok(Self { scan_snapshot_id })
    }

    pub fn scan_snapshot_id(&self) -> i64 {
        self.scan_snapshot_id
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&Self::MAGIC);
        bytes.extend_from_slice(&Self::VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.scan_snapshot_id.to_be_bytes());
        bytes
    }

    pub fn is_data_evolution_meta(bytes: Option<&[u8]>) -> bool {
        bytes.is_some_and(|bytes| bytes.starts_with(&Self::MAGIC))
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 16 || !Self::is_data_evolution_meta(Some(bytes)) {
            return Err(invalid("Invalid data-evolution index source metadata."));
        }
        let version = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
        if version != Self::VERSION {
            return Err(invalid(format!(
                "Unsupported data-evolution index source version: {version}."
            )));
        }
        Self::new(i64::from_be_bytes(bytes[8..16].try_into().unwrap()))
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_wire_format_and_invalid_frames() {
        let java = [0x44, 0x45, 0x49, 0x58, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 42];
        let source = DataEvolutionIndexSourceMeta::new(42).unwrap();
        assert_eq!(source.serialize(), java);
        assert_eq!(
            DataEvolutionIndexSourceMeta::deserialize(&java).unwrap(),
            source
        );
        for length in 0..16 {
            assert!(DataEvolutionIndexSourceMeta::deserialize(&java[..length]).is_err());
        }
        for offset in [0, 7, 15] {
            let mut invalid = java;
            invalid[offset] = 0;
            assert!(DataEvolutionIndexSourceMeta::deserialize(&invalid).is_err());
        }
        let mut trailing = java.to_vec();
        trailing.push(0);
        assert!(DataEvolutionIndexSourceMeta::deserialize(&trailing).is_err());
        assert!(!DataEvolutionIndexSourceMeta::is_data_evolution_meta(None));
        assert!(!DataEvolutionIndexSourceMeta::is_data_evolution_meta(Some(
            &[0, 0, 0, 1]
        )));
    }
}
