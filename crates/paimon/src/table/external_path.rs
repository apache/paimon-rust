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

//! External index placement follows Java's FileStorePathFactory. Existing
//! files always resolve using their recorded path, never current write options.

use crate::Result;
use rand::Rng;
use std::collections::HashMap;

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

pub(super) fn new_index_external_path(
    options: &HashMap<String, String>,
    in_data_directory: bool,
    relative_bucket: &str,
    file_name: &str,
) -> Result<Option<String>> {
    if !in_data_directory {
        return Ok(options
            .get("global-index.external-path")
            .map(|path| format!("{}/{file_name}", path.trim_end_matches('/'))));
    }
    Ok(ExternalPathProvider::new(options, relative_bucket)?
        .map(|mut provider| provider.next_path(file_name)))
}

/// Mirrors Java's per-bucket ExternalPathProvider. The random starting point
/// avoids concentrating single-file buckets on the first configured root.
struct ExternalPathProvider {
    paths: Vec<String>,
    bucket: String,
    position: usize,
    entropy: bool,
    cumulative_weights: Vec<u64>,
}

impl ExternalPathProvider {
    fn new(options: &HashMap<String, String>, bucket: &str) -> Result<Option<Self>> {
        let strategy = options
            .get("data-file.external-paths.strategy")
            .map(|value| value.to_ascii_lowercase())
            .unwrap_or_else(|| "none".into());
        let Some(paths) = options
            .get("data-file.external-paths")
            .filter(|paths| !paths.is_empty())
        else {
            return Ok(None);
        };
        if strategy == "none" {
            return Ok(None);
        }
        if !matches!(
            strategy.as_str(),
            "round-robin" | "specific-fs" | "weight-robin" | "entropy-inject"
        ) {
            return Err(invalid(format!(
                "Unsupported external path strategy: {strategy}"
            )));
        }
        let specific_fs = if strategy == "specific-fs" {
            Some(
                options
                    .get("data-file.external-paths.specific-fs")
                    .ok_or_else(|| invalid("External path specific-fs is required"))?,
            )
        } else {
            None
        };
        let mut roots = Vec::new();
        // Java String.split discards trailing empty entries.
        for path in paths.trim_end_matches(',').split(',').map(str::trim) {
            let uri = url::Url::parse(path)
                .map_err(|_| invalid(format!("External path must have a URI scheme: {path}")))?;
            if specific_fs.is_none_or(|scheme| uri.scheme().eq_ignore_ascii_case(scheme)) {
                roots.push(path.trim_end_matches('/').to_string());
            }
        }
        if roots.is_empty() {
            return Err(invalid("External paths should not be empty"));
        }
        let mut cumulative_weights = Vec::new();
        if strategy == "weight-robin" && roots.len() > 1 {
            if let Some(weights) = options
                .get("data-file.external-paths.weights")
                .filter(|weights| !weights.trim().is_empty())
            {
                let mut total = 0_u64;
                for weight in weights.trim_end_matches(',').split(',') {
                    let weight = weight
                        .trim()
                        .parse::<i32>()
                        .ok()
                        .filter(|weight| *weight > 0)
                        .ok_or_else(|| {
                            invalid("External path weights must be positive integers")
                        })?;
                    total = total
                        .checked_add(weight as u64)
                        .ok_or_else(|| invalid("External path weight overflow"))?;
                    cumulative_weights.push(total);
                }
                if cumulative_weights.len() != roots.len() {
                    return Err(invalid(
                        "The number of external paths and weights should be the same",
                    ));
                }
            }
        }
        let entropy = strategy == "entropy-inject";
        let position = if entropy {
            0
        } else {
            rand::thread_rng().gen_range(0..roots.len())
        };
        Ok(Some(Self {
            paths: roots,
            bucket: bucket.trim_matches('/').into(),
            position,
            entropy,
            cumulative_weights,
        }))
    }

    fn next_path(&mut self, file_name: &str) -> String {
        let index = if let Some(total) = self.cumulative_weights.last() {
            let value = rand::thread_rng().gen_range(0..*total);
            self.cumulative_weights
                .partition_point(|weight| *weight <= value)
        } else {
            self.position = (self.position + 1) % self.paths.len();
            self.position
        };
        let mut path = self.paths[index].clone();
        if !self.bucket.is_empty() {
            path.push('/');
            path.push_str(&self.bucket);
        }
        if self.entropy {
            let hash =
                crate::spec::murmur_hash::hash_bytes_guava(file_name.as_bytes()) as u32 & 0xfffff;
            path.push_str(&format!(
                "/{:04b}/{:04b}/{:04b}/{:08b}",
                hash >> 16,
                (hash >> 12) & 15,
                (hash >> 8) & 15,
                hash & 255
            ));
        }
        format!("{path}/{file_name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(strategy: &str) -> HashMap<String, String> {
        HashMap::from([
            (
                "data-file.external-paths".into(),
                "file:///a/, file:///b".into(),
            ),
            ("data-file.external-paths.strategy".into(), strategy.into()),
            (
                "global-index.external-path".into(),
                "file:///global/".into(),
            ),
        ])
    }

    #[test]
    fn round_robin_and_location_precedence() {
        let settings = options("round-robin");
        let mut provider = ExternalPathProvider::new(&settings, "p=x/bucket-0")
            .unwrap()
            .unwrap();
        let paths = [provider.next_path("index-1"), provider.next_path("index-1")];
        assert!(paths.contains(&"file:///a/p=x/bucket-0/index-1".into()));
        assert!(paths.contains(&"file:///b/p=x/bucket-0/index-1".into()));
        assert_eq!(
            new_index_external_path(&settings, false, "p=x/bucket-0", "index-1")
                .unwrap()
                .as_deref(),
            Some("file:///global/index-1")
        );
        assert!(
            new_index_external_path(&options("none"), true, "bucket-0", "index-1")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn specific_fs_and_invalid_options() {
        let mut options = options("specific-fs");
        assert!(ExternalPathProvider::new(&options, "").is_err());
        options.insert("data-file.external-paths.specific-fs".into(), "FILE".into());
        options.insert(
            "data-file.external-paths".into(),
            "s3://bucket/path,file:///a".into(),
        );
        assert_eq!(
            ExternalPathProvider::new(&options, "bucket-0")
                .unwrap()
                .unwrap()
                .next_path("index-1"),
            "file:///a/bucket-0/index-1"
        );
        options.insert("data-file.external-paths.specific-fs".into(), "oss".into());
        assert!(ExternalPathProvider::new(&options, "").is_err());
        options.insert("data-file.external-paths".into(), "/no/scheme".into());
        assert!(ExternalPathProvider::new(&options, "").is_err());
    }

    #[test]
    fn weighted_validation_and_fallback() {
        let mut options = options("weight-robin");
        assert!(ExternalPathProvider::new(&options, "")
            .unwrap()
            .unwrap()
            .cumulative_weights
            .is_empty());
        for weights in ["0,1", "-1,2", "x,2", "1", "2147483648,1"] {
            options.insert("data-file.external-paths.weights".into(), weights.into());
            assert!(
                ExternalPathProvider::new(&options, "").is_err(),
                "{weights}"
            );
        }
        options.insert("data-file.external-paths.weights".into(), "1,2".into());
        let mut provider = ExternalPathProvider::new(&options, "bucket-0")
            .unwrap()
            .unwrap();
        assert_eq!(provider.cumulative_weights, vec![1, 3]);
        for _ in 0..10 {
            assert!(["file:///a/bucket-0/index", "file:///b/bucket-0/index"]
                .contains(&provider.next_path("index").as_str()));
        }
    }

    #[test]
    fn comma_lists_follow_java_trailing_empty_semantics() {
        let mut options = options("weight-robin");
        options.insert(
            "data-file.external-paths".into(),
            "file:///a,file:///b,,".into(),
        );
        options.insert("data-file.external-paths.weights".into(), "1,2,,".into());
        let provider = ExternalPathProvider::new(&options, "").unwrap().unwrap();
        assert_eq!(provider.paths, vec!["file:///a", "file:///b"]);
        assert_eq!(provider.cumulative_weights, vec![1, 3]);
        options.insert(
            "data-file.external-paths".into(),
            "file:///a,,file:///b".into(),
        );
        assert!(ExternalPathProvider::new(&options, "").is_err());
    }

    #[test]
    fn entropy_uses_guava_hash_and_rotates_from_second_root() {
        let mut provider = ExternalPathProvider::new(&options("entropy-inject"), "p=x/bucket-0")
            .unwrap()
            .unwrap();
        // Guava murmur3_32(0), UTF-8 "hello": 0x248bfa47.
        assert_eq!(
            provider.next_path("hello"),
            "file:///b/p=x/bucket-0/1011/1111/1010/01000111/hello"
        );
        assert_eq!(
            provider.next_path("hello"),
            "file:///a/p=x/bucket-0/1011/1111/1010/01000111/hello"
        );
    }
}
