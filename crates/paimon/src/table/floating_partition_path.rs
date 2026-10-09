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

//! Resolve floating partition names written by different supported JDKs.
//! Java's Float/Double.toString changed in JDK 19. Manifest partitions retain
//! the exact typed value, so a missing canonical file may be found under an
//! older Java spelling without accepting unrelated partition layouts.

use super::Table;
use crate::spec::{
    escape_path_name, BinaryRow, DataFileMeta, DataType, IndexManifestEntry, PartitionComputer,
    POSTPONE_BUCKET,
};
use crate::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) struct FloatingPartitionPathResolver<'a> {
    table: &'a Table,
    computer: PartitionComputer,
    enabled: bool,
    candidates: HashMap<Vec<u8>, Arc<Vec<String>>>,
    directory_files: HashMap<String, Option<HashSet<String>>>,
}

impl<'a> FloatingPartitionPathResolver<'a> {
    pub(super) fn new(table: &'a Table) -> Result<Self> {
        let schema = table.schema();
        let options = schema.core_options();
        Ok(Self {
            table,
            computer: PartitionComputer::new(
                schema.partition_keys(),
                schema.fields(),
                options.partition_default_name(),
                options.legacy_partition_name(),
            )?,
            enabled: schema
                .partition_fields()
                .iter()
                .any(|field| matches!(field.data_type(), DataType::Float(_) | DataType::Double(_))),
            candidates: HashMap::new(),
            directory_files: HashMap::new(),
        })
    }

    pub(super) async fn resolve_index_paths(
        &mut self,
        entries: &mut [IndexManifestEntry],
    ) -> Result<()> {
        if !self.enabled
            || !self
                .table
                .schema()
                .core_options()
                .index_file_in_data_file_dir()
        {
            return Ok(());
        }
        for entry in entries {
            let file = &mut entry.index_file;
            if file.external_path.is_some()
                || !matches!(file.index_type.as_str(), "DELETION_VECTORS" | "HASH")
            {
                continue;
            }
            let partition = BinaryRow::from_serialized_bytes(&entry.partition)?;
            let directory = self.computer.generate_partition_path(&partition)?;
            let bucket_path = crate::spec::bucket_path_under(
                &self.table.data_file_location(),
                &directory,
                entry.bucket,
            );
            let canonical = format!("{bucket_path}/{}", file.file_name);
            let path = self
                .resolve(&partition, entry.bucket, &file.file_name, &canonical)
                .await?;
            if path != canonical {
                file.external_path = Some(path);
            }
        }
        Ok(())
    }

    /// Resolve one existing index for reading while preserving its original
    /// manifest metadata for a later DELETE entry.
    pub(super) async fn index_path(
        &mut self,
        entry: &IndexManifestEntry,
        canonical: &str,
    ) -> Result<String> {
        if !self.enabled
            || entry.index_file.external_path.is_some()
            || !self
                .table
                .schema()
                .core_options()
                .index_file_in_data_file_dir()
        {
            return Ok(canonical.to_string());
        }
        let partition = BinaryRow::from_serialized_bytes(&entry.partition)?;
        self.resolve(
            &partition,
            entry.bucket,
            &entry.index_file.file_name,
            canonical,
        )
        .await
    }

    pub(super) async fn resolve_data_file_paths(
        &mut self,
        partition: &BinaryRow,
        bucket: i32,
        bucket_path: &str,
        files: &mut [DataFileMeta],
    ) -> Result<Option<Arc<HashMap<String, DataFileMeta>>>> {
        if !self.enabled {
            return Ok(None);
        }
        let mut original = HashMap::new();
        for file in files {
            if file.external_path.is_some() {
                continue;
            }
            let canonical = file.data_file_path(bucket_path);
            let path = self
                .resolve(partition, bucket, &file.file_name, &canonical)
                .await?;
            if path != canonical {
                // This is a split-local copy, never a change to the manifest.
                // Readers use this parent for aligned sidecars / file indexes.
                original.insert(file.file_name.clone(), file.clone());
                file.external_path = Some(path);
            }
        }
        Ok((!original.is_empty()).then(|| Arc::new(original)))
    }

    pub(super) async fn resolve(
        &mut self,
        partition: &BinaryRow,
        bucket: i32,
        name: &str,
        canonical: &str,
    ) -> Result<String> {
        if !self.enabled || self.file_exists(canonical).await? {
            return Ok(canonical.to_string());
        }
        let key = partition.to_serialized_bytes();
        if !self.candidates.contains_key(&key) {
            let paths = self.find_partition_directories(partition).await?;
            self.candidates.insert(key.clone(), Arc::new(paths));
        }
        let bucket = if bucket == POSTPONE_BUCKET {
            "postpone".to_string()
        } else {
            bucket.to_string()
        };
        let paths = self.candidates[&key].clone();
        for path in paths.iter() {
            let candidate = format!("{path}/bucket-{bucket}/{name}");
            if candidate != canonical && self.file_exists(&candidate).await? {
                return Ok(candidate);
            }
        }
        // Keep the normal missing-file error; never suppress or retry a read.
        Ok(canonical.to_string())
    }

    async fn file_exists(&mut self, path: &str) -> Result<bool> {
        let Some((directory, name)) = path.rsplit_once('/') else {
            return self.table.file_io().exists(path).await;
        };
        if !self.directory_files.contains_key(directory) {
            // One listing per bucket avoids a serial HEAD for every canonical
            // data file. Get-only credentials can still use ordinary existence
            // checks when directory listing is unavailable.
            let names = self
                .table
                .file_io()
                .list_status(directory)
                .await
                .ok()
                .map(|files| {
                    files
                        .into_iter()
                        .filter(|file| !file.is_dir)
                        .filter_map(|file| file.path.rsplit('/').next().map(str::to_owned))
                        .collect()
                });
            self.directory_files.insert(directory.to_string(), names);
        }
        if self.directory_files[directory]
            .as_ref()
            .is_some_and(|files| files.contains(name))
        {
            return Ok(true);
        }
        // Recheck negative entries so a file placed in the canonical directory
        // after the listing retains priority over an alternative Java spelling.
        self.table.file_io().exists(path).await
    }

    async fn find_partition_directories(&self, partition: &BinaryRow) -> Result<Vec<String>> {
        let fields = self.table.schema().partition_fields();
        let values = self.computer.generate_part_values(partition)?;
        let io = self.table.file_io();
        let mut paths = vec![self
            .table
            .data_file_location()
            .trim_end_matches('/')
            .to_string()];
        for (index, (key, value)) in values.iter().enumerate() {
            let prefix = format!("{}=", escape_path_name(key));
            let ty = fields[index].data_type();
            if partition.is_null_at(index)
                || !matches!(ty, DataType::Float(_) | DataType::Double(_))
            {
                paths = paths
                    .into_iter()
                    .map(|path| format!("{path}/{prefix}{}", escape_path_name(value)))
                    .collect();
                continue;
            }
            let mut matched = Vec::new();
            for path in paths {
                if !io.exists_dir(&path).await? {
                    continue;
                }
                for status in io.list_status(&path).await? {
                    if !status.is_dir {
                        continue;
                    }
                    let name = status
                        .path
                        .trim_end_matches('/')
                        .rsplit('/')
                        .next()
                        .unwrap_or("");
                    let Some(text) = name.strip_prefix(&prefix) else {
                        continue;
                    };
                    if same_java_float(text, partition, index, ty)? {
                        matched.push(status.path.trim_end_matches('/').to_string());
                    }
                }
            }
            matched.sort();
            paths = matched;
        }
        Ok(paths)
    }
}

fn same_java_float(text: &str, row: &BinaryRow, index: usize, ty: &DataType) -> Result<bool> {
    // Java always includes a fractional digit, uses uppercase E and omits +
    // from positive exponents. Exclude Python/C-style spellings and whitespace.
    if !text.contains('.') || text.contains(['e', '+']) || text.trim() != text {
        return Ok(false);
    }
    Ok(match ty {
        DataType::Float(_) => {
            let expected = row.get_float(index)?.to_bits();
            text.parse::<f32>()
                .is_ok_and(|value| value.to_bits() == expected)
        }
        DataType::Double(_) => {
            let expected = row.get_double(index)?.to_bits();
            text.parse::<f64>()
                .is_ok_and(|value| value.to_bits() == expected)
        }
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{BinaryRowBuilder, DoubleType, Schema, TableSchema, VarCharType};

    #[tokio::test]
    async fn resolves_java8_names_but_prefers_existing_canonical_files() {
        let schema = Schema::builder()
            .column("region", DataType::VarChar(VarCharType::string_type()))
            .column("p", DataType::Double(DoubleType::new()))
            .partition_keys(["region", "p"])
            .option("data-file.path-directory", "data")
            .build()
            .unwrap();
        let io = FileIOBuilder::new("memory").build().unwrap();
        let table = Table::new(
            io.clone(),
            Identifier::new("db", "t"),
            "memory:/float-path".into(),
            TableSchema::new(0, &schema),
            None,
        );
        let mut row = BinaryRowBuilder::new(2);
        row.write_string(0, "a/b");
        row.write_double(1, 1e23);
        let row = row.build();
        let old =
            "memory:/float-path/data/region=a%2Fb/p=9.999999999999999E22/bucket-0/data.parquet";
        let canonical = "memory:/float-path/data/region=a%2Fb/p=1.0E23/bucket-0/data.parquet";
        io.new_output(old)
            .unwrap()
            .write(bytes::Bytes::from_static(b"java8"))
            .await
            .unwrap();
        io.new_output("memory:/float-path/data/region=a%2Fb/p=1.0E22/bucket-0/data.parquet")
            .unwrap()
            .write(bytes::Bytes::from_static(b"other partition"))
            .await
            .unwrap();
        let mut resolver = FloatingPartitionPathResolver::new(&table).unwrap();
        assert_eq!(
            resolver
                .resolve(&row, 0, "data.parquet", canonical)
                .await
                .unwrap(),
            old
        );
        io.new_output(canonical)
            .unwrap()
            .write(bytes::Bytes::from_static(b"java21"))
            .await
            .unwrap();
        assert_eq!(
            resolver
                .resolve(&row, 0, "data.parquet", canonical)
                .await
                .unwrap(),
            canonical
        );
        let missing = canonical.replace("data.parquet", "missing.parquet");
        assert_eq!(
            resolver
                .resolve(&row, 0, "missing.parquet", &missing)
                .await
                .unwrap(),
            missing
        );
    }
}
