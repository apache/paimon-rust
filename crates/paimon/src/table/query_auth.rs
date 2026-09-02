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

//! What the REST server authorized a user to read from one table.

use crate::api::AuthTableQueryResponse;

/// The server's answer for one user on one table, kept unparsed.
///
/// `session` pins it to the handle that asked: `to_arrow` is public and the
/// response names neither table nor principal. Routing options are unbound on
/// purpose — sound only while unrestricted grants authorize.
#[derive(Debug, PartialEq)]
pub(crate) struct QueryAuthGrant {
    response: AuthTableQueryResponse,
    session: u64,
}

impl QueryAuthGrant {
    pub(crate) fn new(response: AuthTableQueryResponse, session: u64) -> Self {
        Self { response, session }
    }

    /// The only case this client can serve.
    pub(crate) fn is_unrestricted(&self) -> bool {
        self.response.is_unrestricted()
    }

    /// Travelled and branch views read a schema the server did not rule on.
    /// Everything else follows from the session, which only the catalog mints.
    pub(crate) fn matches_table(&self, table: &super::Table) -> bool {
        !table.is_time_traveled()
            && !table.is_branch_reference()
            && table.query_auth_session() == Some(self.session)
    }
}

/// `value_stats` and `write_cols` are public on every split and an older file
/// can name a dropped column. Refused rather than scrubbed — rewriting encoded
/// stats is how bounds get mismatched.
pub(crate) async fn reject_unauthorized_stats(
    plan: &super::Plan,
    current: &crate::spec::TableSchema,
    schemas: &super::schema_manager::SchemaManager,
) -> crate::Result<()> {
    let refuse = |column: &str| {
        Err(unsupported(&format!(
            "a data file still carries statistics for '{column}', which the current schema — the \
             one the server authorized — does not have"
        )))
    };
    let named = |name: &String| current.fields().iter().any(|f| f.name() == name);
    let mut checked = std::collections::HashSet::new();
    for split in plan.splits() {
        for file in split.data_files() {
            for column in file
                .value_stats_cols
                .iter()
                .chain(file.write_cols.iter())
                .flatten()
            {
                if !named(column) {
                    return refuse(column);
                }
            }
            // The file's own schema is the authority: a name can be dropped and
            // re-added under a new id, and the lists may be absent entirely.
            if file.schema_id == current.id() || !checked.insert(file.schema_id) {
                continue;
            }
            let older = schemas.schema(file.schema_id).await?;
            if let Some(gone) = older.fields().iter().find(|f| {
                !current.fields().iter().any(|c| {
                    c.id() == f.id()
                        && c.name() == f.name()
                        && shape(c.data_type()) == shape(f.data_type())
                })
            }) {
                return refuse(gone.name());
            }
        }
    }
    Ok(())
}

/// The physical shape, descriptions stripped: `DataField` equality includes them,
/// so a comment-only edit would otherwise read as an unauthorized column.
fn shape(ty: &crate::spec::DataType) -> crate::spec::DataType {
    use crate::spec::{ArrayType, DataType, MapType, MultisetType, RowType};
    match ty {
        DataType::Row(row) => DataType::Row(RowType::new(
            row.fields()
                .iter()
                .map(|f| {
                    crate::spec::DataField::new(f.id(), f.name().to_string(), shape(f.data_type()))
                })
                .collect(),
        )),
        DataType::Array(a) => DataType::Array(ArrayType::with_nullable(
            ty.is_nullable(),
            shape(a.element_type()),
        )),
        DataType::Multiset(m) => DataType::Multiset(MultisetType::with_nullable(
            ty.is_nullable(),
            shape(m.element_type()),
        )),
        DataType::Map(m) => DataType::Map(MapType::with_nullable(
            ty.is_nullable(),
            shape(m.key_type()),
            shape(m.value_type()),
        )),
        other => other.clone(),
    }
}

/// A refusal naming the option, so callers never match on prose.
pub(crate) fn unsupported(reason: &str) -> crate::Error {
    crate::Error::Unsupported {
        message: format!(
            "reading a table with 'query-auth.enabled' = true is not supported: {reason}"
        ),
    }
}

/// Column permissions cover real schema fields, so the server can neither grant
/// nor refuse `_ROW_ID` and friends.
pub(crate) fn reject_system_columns<'a>(
    names: impl IntoIterator<Item = &'a str>,
) -> crate::Result<()> {
    for name in names {
        if crate::spec::is_reserved_system_field_name(name) {
            return Err(unsupported(&format!(
                "the system column '{name}' is not one the server can authorize: column \
                 permissions are granted over table columns"
            )));
        }
    }
    Ok(())
}

/// The read resolves older files by field id, so a non-canonical `(id, name)`
/// pair reads as something no grant covered. System fields have no entry.
pub(crate) fn reject_noncanonical_fields(
    read_type: &[crate::spec::DataField],
    schema_fields: &[crate::spec::DataField],
) -> crate::Result<()> {
    for field in read_type {
        if crate::spec::is_reserved_system_field_name(field.name()) {
            continue;
        }
        // The whole shape: an older field can keep `(id, name)` and carry an
        // extra nested child.
        let canonical = schema_fields.iter().any(|f| {
            f.id() == field.id()
                && f.name() == field.name()
                && shape(f.data_type()) == shape(field.data_type())
        });
        if !canonical {
            return Err(unsupported(&format!(
                "'{}' (field id {}) is not a column of the current schema, which is what the \
                 server authorized",
                field.name(),
                field.id()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::reject_system_columns;
    use crate::table::query_auth_table;

    #[tokio::test]
    async fn test_a_grant_is_pinned_to_the_handle_that_obtained_it() {
        let a = crate::table::rest_query_auth_table().await;
        let b = crate::table::rest_query_auth_table().await;
        let grant = super::QueryAuthGrant::new(
            crate::api::AuthTableQueryResponse::default(),
            a.query_auth_session().unwrap(),
        );
        assert!(grant.matches_table(&a));
        assert!(
            !grant.matches_table(&b),
            "another handle — another principal or another table — must not reuse it"
        );
    }

    #[tokio::test]
    async fn test_a_time_travel_selector_alone_is_refused() {
        for selector in [
            "scan.snapshot-id",
            "scan.version",
            "scan.tag-name",
            "scan.timestamp-millis",
            "scan.watermark",
        ] {
            let table = query_auth_table().copy_with_options(std::collections::HashMap::from([(
                selector.to_string(),
                "1".to_string(),
            )]));
            assert!(!table.is_time_traveled(), "{selector} sets no flag");
            let err = table.authorize_read(true).await.unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("time-travelled or branch read")),
                "{selector}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_a_grant_does_not_cross_into_a_travelled_or_branch_view() {
        let table = crate::table::rest_query_auth_table().await;
        let grant = super::QueryAuthGrant::new(
            crate::api::AuthTableQueryResponse::default(),
            table.query_auth_session().unwrap(),
        );
        assert!(grant.matches_table(&table));

        let mut travelled = table.copy_with_options(std::collections::HashMap::new());
        travelled.time_traveled = true;
        assert!(
            !grant.matches_table(&travelled),
            "an older schema is not the one the server ruled on"
        );

        let assembled = crate::table::Table::new(
            table.file_io().clone(),
            table.identifier().clone(),
            "/tmp/somewhere-else".to_string(),
            table.schema().clone(),
            table.rest_env().cloned(),
        );
        assert!(
            !grant.matches_table(&assembled),
            "an assembled handle must not replay a grant"
        );

        let mut branch = table.copy_with_options(std::collections::HashMap::new());
        branch.branch_reference = true;
        assert!(
            !grant.matches_table(&branch),
            "a branch view is refused even when its schema id coincides"
        );
    }

    fn data_file_for_stats(
        schema_id: i64,
        cols: Option<Vec<&str>>,
        written: Option<Vec<&str>>,
    ) -> crate::spec::DataFileMeta {
        crate::spec::DataFileMeta {
            file_name: "f.parquet".to_string(),
            file_size: 1,
            row_count: 1,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: crate::spec::stats::BinaryTableStats::empty(),
            value_stats: crate::spec::stats::BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: Some(0),
            embedded_index: None,
            file_source: None,
            value_stats_cols: cols.map(|c| c.iter().map(|s| s.to_string()).collect()),
            external_path: None,
            first_row_id: None,
            write_cols: written.map(|c| c.iter().map(|s| s.to_string()).collect()),
            column_max_sequence_numbers: None,
        }
    }

    fn plan_of(meta: crate::spec::DataFileMeta) -> crate::table::Plan {
        crate::table::Plan::new(vec![crate::table::DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRowBuilder::new(0).build())
            .with_bucket(0)
            .with_bucket_path("p".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![meta])
            .with_raw_convertible(false)
            .build()
            .unwrap()])
    }

    #[tokio::test]
    async fn test_stats_for_a_dropped_column_are_refused() {
        let table = query_auth_table();
        let schemas = table.schema_manager();

        for meta in [
            data_file_for_stats(table.schema().id(), Some(vec!["id", "gone"]), None),
            data_file_for_stats(table.schema().id(), None, Some(vec!["id", "gone"])),
            data_file_for_stats(
                table.schema().id(),
                Some(vec!["id"]),
                Some(vec!["id", "gone"]),
            ),
        ] {
            let err = super::reject_unauthorized_stats(&plan_of(meta), table.schema(), schemas)
                .await
                .unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("statistics for 'gone'")),
                "{err:?}"
            );
        }

        assert!(super::reject_unauthorized_stats(
            &plan_of(data_file_for_stats(
                table.schema().id(),
                Some(vec!["id"]),
                Some(vec!["id"])
            )),
            table.schema(),
            schemas
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn test_an_old_schema_whose_column_changed_type_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let location = tmp.path().display().to_string();
        let column = |ty| {
            crate::spec::Schema::builder()
                .column("id", ty)
                .option("query-auth.enabled", "true")
                .build()
                .unwrap()
        };
        let table = crate::table::Table::new(
            crate::io::FileIOBuilder::new("file").build().unwrap(),
            crate::catalog::Identifier::new("default", "evolved"),
            location,
            crate::spec::TableSchema::new(
                0,
                &column(crate::spec::DataType::Int(crate::spec::IntType::new())),
            ),
            None,
        );

        // Same field id and name, a different type: the server ruled on the
        // current one, so the older file's stats are not covered.
        let older = crate::spec::TableSchema::new(
            1,
            &column(crate::spec::DataType::BigInt(crate::spec::BigIntType::new())),
        );
        let schemas = table.schema_manager();
        table
            .file_io()
            .new_output(&schemas.schema_path(1))
            .unwrap()
            .write(serde_json::to_vec(&older).unwrap().into())
            .await
            .unwrap();

        let err = super::reject_unauthorized_stats(
            &plan_of(data_file_for_stats(1, None, None)),
            table.schema(),
            schemas,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("statistics for 'id'")),
            "{err:?}"
        );
    }

    #[test]
    fn test_a_comment_only_change_is_not_a_different_column() {
        use crate::spec::{DataField, DataType, IntType, RowType};
        let child = |desc: Option<&str>| {
            let f = DataField::new(1, "a".to_string(), DataType::Int(IntType::new()));
            match desc {
                Some(d) => f.with_description(Some(d.to_string())),
                None => f,
            }
        };
        let row = |desc| DataType::Row(RowType::new(vec![child(desc)]));
        assert_ne!(
            row(None),
            row(Some("why")),
            "equality includes descriptions"
        );
        assert_eq!(
            super::shape(&row(None)),
            super::shape(&row(Some("why"))),
            "but a comment is not a column the server did not authorize"
        );
    }

    #[test]
    fn test_a_system_column_read_is_refused() {
        let err = reject_system_columns(["id", crate::spec::ROW_ID_FIELD_NAME]).unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("system column '_ROW_ID'")),
            "{err:?}"
        );
        assert!(reject_system_columns(["id", "name"]).is_ok());
    }

    #[tokio::test]
    async fn test_time_travelled_or_branch_read_is_refused() {
        let mut travelled = query_auth_table();
        travelled.time_traveled = true;
        let err = travelled.authorize_read(true).await.unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("time-travelled or branch read")),
            "got {err:?}"
        );

        let mut branch = query_auth_table();
        branch.branch_reference = true;
        assert!(branch.authorize_read(true).await.is_err());
    }
}
