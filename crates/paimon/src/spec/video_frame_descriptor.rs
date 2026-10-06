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

//! Java VideoFrameDescriptor v1, including the persisted keyframe-index range.

use super::BlobDescriptor;
use crate::{Error, Result};

const VERSION: u8 = 1;
const MAGIC: i64 = 0x564944454F46524D;
const FIXED_LENGTH: usize = 1 + 8 + 4 + 5 * 8;

/// One logical frame in an encoded video, with an optional keyframe index.
/// The payload is the complete encoded video; `frame_index` locates the frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoFrameDescriptor {
    payload: BlobDescriptor,
    frame_index: i64,
    keyframe_index_offset: i64,
    keyframe_index_length: i64,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: format!("Invalid VideoFrameDescriptor data: {}", message.into()),
        source: None,
    }
}

impl VideoFrameDescriptor {
    /// Construct a descriptor using the same locator validation as Java.
    pub fn new(
        uri: String,
        offset: i64,
        length: i64,
        frame_index: i64,
        keyframe_index_offset: i64,
        keyframe_index_length: i64,
    ) -> Result<Self> {
        if frame_index < 0 {
            return Err(invalid("negative frame index"));
        }
        if !(keyframe_index_length == 0 && keyframe_index_offset == -1
            || keyframe_index_length > 0 && keyframe_index_offset >= 0)
        {
            return Err(invalid("invalid video keyframe index range"));
        }
        Ok(Self {
            payload: BlobDescriptor::new(uri, offset, length),
            frame_index,
            keyframe_index_offset,
            keyframe_index_length,
        })
    }

    /// Physical video identity, without the logical frame locator.
    pub fn payload_descriptor(&self) -> &BlobDescriptor {
        &self.payload
    }
    /// Zero-based frame ordinal within the physical video.
    pub fn frame_index(&self) -> i64 {
        self.frame_index
    }
    /// The optional index is stored in the same object as the physical video.
    pub fn keyframe_index_descriptor(&self) -> Option<BlobDescriptor> {
        (self.keyframe_index_length > 0).then(|| {
            BlobDescriptor::new(
                self.payload.uri().into(),
                self.keyframe_index_offset,
                self.keyframe_index_length,
            )
        })
    }

    /// Serialize to Java's exact little-endian wire layout.
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(FIXED_LENGTH + self.payload.uri().len());
        bytes.push(VERSION);
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        bytes.extend_from_slice(&(self.payload.uri().len() as i32).to_le_bytes());
        bytes.extend_from_slice(self.payload.uri().as_bytes());
        for value in [
            self.payload.offset(),
            self.payload.length(),
            self.frame_index,
            self.keyframe_index_offset,
            self.keyframe_index_length,
        ] {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    /// Recognize the version and magic only, matching Java's detection helper.
    pub fn is_video_frame_descriptor(bytes: &[u8]) -> bool {
        bytes.len() >= 9 && bytes[0] == VERSION && bytes[1..9] == MAGIC.to_le_bytes()
    }

    /// Deserialize a complete frame reference; trailing bytes are invalid.
    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < FIXED_LENGTH {
            return Err(invalid("too short"));
        }
        if bytes[0] != VERSION {
            return Err(Error::Unsupported {
                message: format!("Unsupported VideoFrameDescriptor version {}", bytes[0]),
            });
        }
        if bytes[1..9] != MAGIC.to_le_bytes() {
            return Err(invalid("missing magic header"));
        }
        let uri_length = i32::from_le_bytes(bytes[9..13].try_into().unwrap());
        let uri_length = usize::try_from(uri_length).map_err(|_| invalid("negative URI length"))?;
        if uri_length > bytes.len() - FIXED_LENGTH {
            return Err(invalid("URI length exceeds data size"));
        }
        if uri_length != bytes.len() - FIXED_LENGTH {
            return Err(invalid("trailing bytes"));
        }
        let end = 13 + uri_length;
        // Java new String(bytes, UTF_8) replaces invalid UTF-8 sequences.
        let uri = String::from_utf8_lossy(&bytes[13..end]).into_owned();
        let values: Vec<_> = bytes[end..]
            .as_chunks::<8>()
            .0
            .iter()
            .map(|bytes| i64::from_le_bytes(*bytes))
            .collect();
        Self::new(uri, values[0], values[1], values[2], values[3], values[4])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_matches_current_java_fixture() {
        let text = include_str!("../../testdata/video/video-frame-descriptor-v1.hex");
        let bytes = hex::decode(
            text.lines()
                .find(|line| !line.is_empty() && !line.starts_with('#'))
                .unwrap(),
        )
        .unwrap();
        let descriptor =
            VideoFrameDescriptor::new("s3://bucket/视频.mp4".into(), 7, 99, 42, 106, 8).unwrap();
        assert_eq!(descriptor.serialize(), bytes);
        assert_eq!(
            VideoFrameDescriptor::deserialize(&bytes).unwrap(),
            descriptor
        );
        assert_eq!(
            descriptor.keyframe_index_descriptor().unwrap(),
            BlobDescriptor::new("s3://bucket/视频.mp4".into(), 106, 8)
        );
        assert!(!BlobDescriptor::is_blob_descriptor(&bytes));
    }

    #[test]
    fn descriptors_without_indexes_and_unknown_payload_lengths_round_trip() {
        let frame =
            VideoFrameDescriptor::new("memory:/video".into(), 4, -1, i64::MAX, -1, 0).unwrap();
        assert!(frame.keyframe_index_descriptor().is_none());
        assert_eq!(
            frame,
            VideoFrameDescriptor::deserialize(&frame.serialize()).unwrap()
        );
    }

    #[test]
    fn malformed_descriptors_are_rejected_without_overflow() {
        let original = VideoFrameDescriptor::new("x".into(), 0, 1, 0, -1, 0)
            .unwrap()
            .serialize();
        for length in 0..original.len() {
            assert!(VideoFrameDescriptor::deserialize(&original[..length]).is_err());
        }
        for length in [-1i32, i32::MAX] {
            let mut bytes = original.clone();
            bytes[9..13].copy_from_slice(&length.to_le_bytes());
            assert!(VideoFrameDescriptor::deserialize(&bytes).is_err());
        }
        let mut bytes = original.clone();
        bytes.push(0);
        assert!(VideoFrameDescriptor::deserialize(&bytes).is_err());
        let mut bytes = original.clone();
        bytes[1] ^= 1;
        assert!(!VideoFrameDescriptor::is_video_frame_descriptor(&bytes));
        assert!(VideoFrameDescriptor::deserialize(&bytes).is_err());
        let mut bytes = original;
        bytes[0] = 2;
        assert!(matches!(
            VideoFrameDescriptor::deserialize(&bytes),
            Err(Error::Unsupported { .. })
        ));
        for (frame, offset, length) in [(-1, -1, 0), (0, 0, 0), (0, -1, 1), (0, 0, -1)] {
            assert!(VideoFrameDescriptor::new("x".into(), 0, 1, frame, offset, length).is_err());
        }
    }
}
