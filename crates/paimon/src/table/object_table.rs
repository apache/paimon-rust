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

use crate::catalog::Identifier;
use crate::io::FileIO;
use crate::spec::{
    BigIntType, CoreOptions, DataField, DataType, Schema, TableSchema, TableType, VarCharType,
    PATH_OPTION,
};
use crate::{Error, Result};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

/// Metadata for one file exposed by an [`ObjectTable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    path: String,
    name: String,
    length: i64,
    mtime: i64,
    atime: i64,
    owner: Option<String>,
}

impl ObjectEntry {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn length(&self) -> i64 {
        self.length
    }

    pub fn mtime(&self) -> i64 {
        self.mtime
    }

    pub fn atime(&self) -> i64 {
        self.atime
    }

    pub fn owner(&self) -> Option<&str> {
        self.owner.as_deref()
    }
}

/// Read-only view over the files below a configured object location.
#[derive(Debug, Clone)]
pub struct ObjectTable {
    file_io: FileIO,
    identifier: Identifier,
    location: String,
    comment: Option<String>,
}

impl ObjectTable {
    pub(crate) fn normalize_creation(schema: &Schema, default_location: &str) -> Result<Schema> {
        let mut options = schema.options().clone();
        if options
            .get(PATH_OPTION)
            .is_none_or(|path| path.trim().is_empty())
        {
            options.insert(PATH_OPTION.to_string(), default_location.to_string());
        }

        let mut builder = Schema::builder()
            .options(options)
            .comment(schema.comment().map(str::to_string));
        for field in Self::fields() {
            builder = builder.column_with_description(
                field.name().to_string(),
                field.data_type().clone(),
                field.description().map(str::to_string),
            );
        }
        builder.build()
    }

    pub fn try_new(file_io: FileIO, identifier: Identifier, schema: &TableSchema) -> Result<Self> {
        Self::try_new_with_default_location(file_io, identifier, schema, None)
    }

    pub(crate) fn try_new_with_default_location(
        file_io: FileIO,
        identifier: Identifier,
        schema: &TableSchema,
        default_location: Option<&str>,
    ) -> Result<Self> {
        let options = CoreOptions::new(schema.options());
        if options.table_type()? != TableType::ObjectTable {
            return Err(Error::Unsupported {
                message: format!(
                    "table '{}' is not declared 'object-table'",
                    identifier.full_name()
                ),
            });
        }
        options.ensure_engine_can_serve(&identifier.full_name())?;
        let location = options
            .path()
            .filter(|path| !path.trim().is_empty())
            .or_else(|| default_location.filter(|path| !path.trim().is_empty()))
            .ok_or_else(|| Error::ConfigInvalid {
                message: format!(
                    "Object table '{}' requires a non-empty 'path' option",
                    identifier.full_name()
                ),
            })?
            .to_string();
        Ok(Self {
            file_io,
            identifier,
            location,
            comment: schema.comment().map(str::to_string),
        })
    }

    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    pub fn location(&self) -> &str {
        &self.location
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// Fixed schema matching Java Paimon's `ObjectTable.SCHEMA`.
    pub fn fields() -> Vec<DataField> {
        vec![
            DataField::new(
                0,
                "path".to_string(),
                DataType::VarChar(
                    VarCharType::with_nullable(false, VarCharType::MAX_LENGTH)
                        .expect("the maximum varchar length is valid"),
                ),
            )
            .with_description(Some("Relative path of object".to_string())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(
                    VarCharType::with_nullable(false, VarCharType::MAX_LENGTH)
                        .expect("the maximum varchar length is valid"),
                ),
            )
            .with_description(Some("Name of object".to_string())),
            DataField::new(
                2,
                "length".to_string(),
                DataType::BigInt(BigIntType::with_nullable(false)),
            )
            .with_description(Some("Bytes length of object".to_string())),
            DataField::new(
                3,
                "mtime".to_string(),
                DataType::BigInt(BigIntType::with_nullable(false)),
            )
            .with_description(Some("Modification time of object".to_string())),
            DataField::new(
                4,
                "atime".to_string(),
                DataType::BigInt(BigIntType::with_nullable(false)),
            )
            .with_description(Some("Access time of object".to_string())),
            DataField::new(
                5,
                "owner".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            )
            .with_description(Some("Owner of object".to_string())),
        ]
    }

    /// Recursively list all files under the object location.
    pub async fn list_objects(&self) -> Result<Vec<ObjectEntry>> {
        self.list_objects_with_limit(None).await
    }

    /// Recursively list files under the object location, stopping after
    /// `limit` files have been yielded by the storage backend when supplied.
    pub async fn list_objects_with_limit(&self, limit: Option<usize>) -> Result<Vec<ObjectEntry>> {
        let mut entries = self
            .stream_objects(limit)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    /// Stream files under the object location in storage listing order.
    pub async fn stream_objects(
        &self,
        limit: Option<usize>,
    ) -> Result<BoxStream<'static, Result<ObjectEntry>>> {
        let statuses = self
            .file_io
            .list_status_recursive_stream(&self.location, limit)
            .await?;
        let location = self.location.clone();
        let location_path = normalized_path(&location);
        Ok(statuses
            .map(move |status| {
                status
                    .and_then(|status| object_entry_from_status(&location, &location_path, status))
            })
            .boxed())
    }
}

fn object_entry_from_status(
    location: &str,
    location_path: &str,
    status: crate::io::FileStatus,
) -> Result<ObjectEntry> {
    let status_path = normalized_path(&status.path);
    let relative = status_path
        .strip_prefix(location_path)
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Object path '{}' is outside table location '{}'",
                status.path, location
            ),
            source: None,
        })?
        .trim_start_matches('/')
        .to_string();
    let name = relative
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| Error::DataInvalid {
            message: format!("Object path '{}' has no file name", status.path),
            source: None,
        })?
        .to_string();
    let length = i64::try_from(status.size).map_err(|_| Error::DataInvalid {
        message: format!("Object '{}' is too large to fit in BIGINT", status.path),
        source: None,
    })?;
    Ok(ObjectEntry {
        path: relative,
        name,
        length,
        mtime: status
            .last_modified
            .map(|modified| modified.timestamp_millis())
            .unwrap_or(0),
        // OpenDAL does not expose these values portably.
        atime: 0,
        owner: None,
    })
}

fn normalized_path(value: &str) -> String {
    url::Url::parse(value)
        .map(|url| trim_trailing_slashes(url.path()))
        .unwrap_or_else(|_| trim_trailing_slashes(value))
}

fn trim_trailing_slashes(value: &str) -> String {
    let trimmed = value.trim_end_matches('/');
    if trimmed.is_empty() && value.starts_with('/') {
        "/".to_string()
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn list_objects_with_limit_returns_at_most_limit_entries() {
        let location = "memory:/objects";
        let file_io = FileIO::from_path(location).unwrap().build().unwrap();
        for path in ["z.txt", "nested/b.txt", "a.txt", "nested/a.txt"] {
            file_io
                .new_output(&format!("{location}/{path}"))
                .unwrap()
                .write(Bytes::from_static(b"x"))
                .await
                .unwrap();
        }
        let schema = Schema::builder()
            .column("ignored", DataType::BigInt(BigIntType::new()))
            .option("type", "object-table")
            .option(PATH_OPTION, location)
            .build()
            .unwrap();
        let table = ObjectTable::try_new(
            file_io,
            Identifier::new("db", "objects"),
            &TableSchema::new(0, &schema),
        )
        .unwrap();

        let paths = table
            .list_objects_with_limit(Some(2))
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 2);
        assert!(paths.windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(paths.iter().all(|path| {
            ["z.txt", "nested/b.txt", "a.txt", "nested/a.txt"].contains(&path.as_str())
        }));
    }

    #[tokio::test]
    async fn list_objects_with_huge_limit_does_not_preallocate() {
        let location = "memory:/empty-objects";
        let file_io = FileIO::from_path(location).unwrap().build().unwrap();
        let schema = Schema::builder()
            .column("ignored", DataType::BigInt(BigIntType::new()))
            .option("type", "object-table")
            .option(PATH_OPTION, location)
            .build()
            .unwrap();
        let table = ObjectTable::try_new(
            file_io,
            Identifier::new("db", "objects"),
            &TableSchema::new(0, &schema),
        )
        .unwrap();

        let entries = table
            .list_objects_with_limit(Some(usize::MAX))
            .await
            .unwrap();
        assert!(entries.is_empty());
    }
}
