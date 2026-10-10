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

mod rules;

use crate::api::AuthTableQueryResponse;
pub(crate) use rules::{filter_batch, mask_batch, mask_inputs, ColumnMask, Rules};

/// The server's answer for one user on one table; `session` ties it to the
/// handle that asked, as the response names no table or user.
#[derive(Debug, PartialEq)]
pub(crate) struct QueryAuthGrant {
    session: u64,
    /// The columns the request asked about; `None` asked about the whole table.
    select: Option<Vec<String>>,
    rules: Rules,
}

impl QueryAuthGrant {
    /// Parses the rules against `fields`, the schema the server ruled on.
    pub(crate) fn parse(
        response: &AuthTableQueryResponse,
        session: u64,
        select: Option<Vec<String>>,
        fields: &[crate::spec::DataField],
    ) -> crate::Result<Self> {
        Ok(Self::new(session, select, Rules::parse(response, fields)?))
    }

    pub(crate) fn new(session: u64, select: Option<Vec<String>>, rules: Rules) -> Self {
        Self {
            session,
            select,
            rules,
        }
    }

    /// No row filter and no masking.
    pub(crate) fn is_unrestricted(&self) -> bool {
        self.rules.is_empty()
    }

    pub(crate) fn rules(&self) -> &Rules {
        &self.rules
    }

    pub(crate) fn select(&self) -> Option<&[String]> {
        self.select.as_deref()
    }

    /// A view of another schema is not the one the server ruled on.
    pub(crate) fn matches_table(&self, table: &super::Table) -> bool {
        !table.reads_another_schema().unwrap_or(true)
            && table.query_auth_session() == Some(self.session)
    }
}

/// An older file can name a dropped column; refused rather than scrubbed.
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
            // The file's schema decides: a name can be re-added under a new id, and
            // either list may be absent.
            if file.schema_id == current.id() || !checked.insert(file.schema_id) {
                continue;
            }
            let older = schemas.schema(file.schema_id).await?;
            if let Some(gone) = older.fields().iter().find(|f| {
                !current
                    .fields()
                    .iter()
                    .any(|c| c.id() == f.id() && c.name() == f.name() && contains_field(c, f))
            }) {
                return refuse(gone.name());
            }
        }
    }
    Ok(())
}

fn contains_field(wide: &crate::spec::DataField, narrow: &crate::spec::DataField) -> bool {
    if let (crate::spec::DataType::Map(map), crate::spec::DataType::Row(row)) =
        (wide.data_type(), narrow.data_type())
    {
        return matches!(map.key_type(), crate::spec::DataType::VarChar(_))
            && crate::spec::map_selected_keys(narrow).is_ok_and(|keys| keys.is_some())
            && row
                .fields()
                .iter()
                .all(|child| map.value_type().equals_ignore_nullable(child.data_type()));
    }
    contains(wide.data_type(), narrow.data_type())
}

/// Whether `narrow` reads nothing `wide` lacks: nested children match by id
/// and name, so a `ROW` projection passes and a re-added child does not.
fn contains(wide: &crate::spec::DataType, narrow: &crate::spec::DataType) -> bool {
    use crate::spec::DataType;
    match (wide, narrow) {
        (DataType::Row(w), DataType::Row(n)) => n.fields().iter().all(|nf| {
            w.fields()
                .iter()
                .any(|wf| wf.id() == nf.id() && wf.name() == nf.name() && contains_field(wf, nf))
        }),
        (DataType::Array(w), DataType::Array(n)) => contains(w.element_type(), n.element_type()),
        (DataType::Multiset(w), DataType::Multiset(n)) => {
            contains(w.element_type(), n.element_type())
        }
        (DataType::Map(w), DataType::Map(n)) => {
            contains(w.key_type(), n.key_type()) && contains(w.value_type(), n.value_type())
        }
        // A `variant_get` pushdown reads a `VARIANT` column as a `ROW` of paths.
        (DataType::Variant(_), DataType::Row(_)) => {
            crate::spec::is_variant_extraction_row_type(narrow)
        }
        (w, n) => w == n,
    }
}

/// Every leaf's column name, system columns included.
pub(crate) fn leaf_names(
    predicates: &[crate::spec::Predicate],
) -> std::collections::HashSet<String> {
    fn collect(predicate: &crate::spec::Predicate, out: &mut std::collections::HashSet<String>) {
        use crate::spec::Predicate;
        match predicate {
            Predicate::Leaf { column, .. } => {
                out.insert(column.clone());
            }
            Predicate::And(children) | Predicate::Or(children) => {
                children.iter().for_each(|child| collect(child, out));
            }
            Predicate::Not(inner) => collect(inner, out),
            Predicate::AlwaysTrue | Predicate::AlwaysFalse => {}
        }
    }
    let mut out = std::collections::HashSet::new();
    predicates.iter().for_each(|p| collect(p, &mut out));
    out
}

/// A refusal naming the option, so callers never match on prose.
pub(crate) fn unsupported(reason: &str) -> crate::Error {
    crate::Error::Unsupported {
        message: format!(
            "reading a table with 'query-auth.enabled' = true is not supported: {reason}"
        ),
    }
}

/// Column permissions cover schema fields only, never `_ROW_ID` and friends.
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

/// Older files resolve by id, so a non-canonical `(id, name)` pair reads
/// something no grant covered. System fields have no entry.
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
        let canonical = schema_fields
            .iter()
            .any(|f| f.id() == field.id() && f.name() == field.name() && contains_field(f, field));
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

/// A strict `variant_get` in the read type casts every stored row, the ones the
/// rules drop included; Java Spark keeps it above the scan.
pub(crate) fn reject_throwing_extractions(
    read_type: &[crate::spec::DataField],
) -> crate::Result<()> {
    use crate::spec::{is_variant_extraction_row, parse_variant_metadata, DataType};
    fn throws(data_type: &DataType) -> bool {
        match data_type {
            DataType::Row(row) if is_variant_extraction_row(row) => {
                row.fields().iter().any(|field| {
                    field.description().is_none_or(|description| {
                        parse_variant_metadata(description).map_or(true, |m| m.fail_on_error())
                    })
                })
            }
            DataType::Row(row) => row.fields().iter().any(|field| throws(field.data_type())),
            // Nested evolution reaches through collections too.
            DataType::Array(array) => throws(array.element_type()),
            DataType::Multiset(multiset) => throws(multiset.element_type()),
            DataType::Map(map) => throws(map.key_type()) || throws(map.value_type()),
            _ => false,
        }
    }
    match read_type.iter().find(|field| throws(field.data_type())) {
        Some(field) => Err(unsupported(&format!(
            "a Variant extraction on '{}' that can fail would run before the server's rules",
            field.name()
        ))),
        None => Ok(()),
    }
}

/// Readers resolve a leaf by `index` or by `column`, so a scope checked by name
/// holds only if both, and the type, name one field of `fields`.
pub(crate) fn reject_noncanonical_leaves(
    predicates: &[crate::spec::Predicate],
    fields: &[crate::spec::DataField],
) -> crate::Result<()> {
    use crate::spec::Predicate;
    fn check(predicate: &Predicate, fields: &[crate::spec::DataField]) -> crate::Result<()> {
        match predicate {
            Predicate::Leaf {
                column,
                index,
                data_type,
                ..
            } => {
                if fields
                    .get(*index)
                    .is_some_and(|f| f.name() == column && f.data_type() == data_type)
                {
                    Ok(())
                } else {
                    Err(unsupported(&format!(
                        "the filter on '{column}' points at another field by index or type; \
                         build filters with PredicateBuilder"
                    )))
                }
            }
            Predicate::And(children) | Predicate::Or(children) => {
                children.iter().try_for_each(|child| check(child, fields))
            }
            Predicate::Not(inner) => check(inner, fields),
            Predicate::AlwaysTrue | Predicate::AlwaysFalse => Ok(()),
        }
    }
    predicates.iter().try_for_each(|p| check(p, fields))
}

#[cfg(test)]
mod tests {
    use super::reject_system_columns;
    use crate::table::{query_auth_table, rest_query_auth_table};

    #[test]
    fn authorized_map_allows_only_valid_selected_value_types() {
        use crate::spec::*;
        let source = DataField::new(
            1,
            "attrs".into(),
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::Int(IntType::new()),
            )),
        );
        let selected = map_selected_keys_field(&source, &["key".into()]).unwrap();
        super::reject_noncanonical_fields(
            std::slice::from_ref(&selected),
            std::slice::from_ref(&source),
        )
        .unwrap();
        let forged = selected.clone().with_description(None);
        assert!(
            super::reject_noncanonical_fields(&[forged], std::slice::from_ref(&source)).is_err()
        );
        let changed = DataField::new(
            selected.id(),
            selected.name().into(),
            DataType::Row(RowType::new(vec![DataField::new(
                0,
                "key".into(),
                DataType::BigInt(BigIntType::new()),
            )])),
        )
        .with_description(selected.description().map(str::to_string));
        assert!(super::reject_noncanonical_fields(&[changed], &[source]).is_err());
    }

    #[tokio::test]
    async fn test_a_grant_is_pinned_to_the_handle_that_obtained_it() {
        let a = crate::table::rest_query_auth_table().await;
        let b = crate::table::rest_query_auth_table().await;
        let grant =
            super::QueryAuthGrant::new(a.query_auth_session().unwrap(), None, Default::default());
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
            "scan.timestamp",
            "scan.watermark",
        ] {
            let table =
                rest_query_auth_table()
                    .await
                    .copy_with_options(std::collections::HashMap::from([(
                        selector.to_string(),
                        "1".to_string(),
                    )]));
            assert!(!table.is_time_traveled(), "{selector} sets no flag");
            let err = table.authorize_read(true, None).await.unwrap_err();
            assert!(
                matches!(err, crate::Error::Unsupported { ref message }
                    if message.contains("time-travelled or branch read")),
                "{selector}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_a_conflicting_selector_pair_is_planning_business_not_authorization() {
        // `scan.version` is adapted before the one-selector rule, so an ordinary
        // table must reach planning rather than fail here.
        let table = crate::table::Table::new(
            crate::io::FileIOBuilder::new("file").build().unwrap(),
            crate::catalog::Identifier::new("default", "plain"),
            "/tmp/test-plain-selector".to_string(),
            crate::spec::TableSchema::new(
                0,
                &crate::spec::Schema::builder()
                    .column(
                        "id",
                        crate::spec::DataType::Int(crate::spec::IntType::new()),
                    )
                    .build()
                    .unwrap(),
            ),
            None,
        )
        .copy_with_options(std::collections::HashMap::from([
            ("scan.version".to_string(), "1".to_string()),
            ("scan.snapshot-id".to_string(), "invalid".to_string()),
        ]));
        assert!(table.reads_another_schema().unwrap());
        assert!(table.authorize_read(false, None).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_a_schema_replaced_copy_loses_its_session() {
        let table = crate::table::rest_query_auth_table().await;
        assert!(table.query_auth_session().is_some());
        let copy = table
            .copy_with_resolved_schema(table.schema().clone(), "main")
            .unwrap();
        assert!(copy.query_auth_session().is_none());
    }

    #[tokio::test]
    async fn test_a_grant_does_not_cross_into_a_travelled_or_branch_view() {
        let table = crate::table::rest_query_auth_table().await;
        let grant = super::QueryAuthGrant::new(
            table.query_auth_session().unwrap(),
            None,
            Default::default(),
        );
        assert!(grant.matches_table(&table));

        let mut travelled = table.copy_with_options(std::collections::HashMap::new());
        travelled.time_traveled = true;
        assert!(
            !grant.matches_table(&travelled),
            "an older schema is not the one the server ruled on"
        );
        let selected = table.copy_with_options(std::collections::HashMap::from([(
            "scan.snapshot-id".to_string(),
            "1".to_string(),
        )]));
        assert!(
            !grant.matches_table(&selected),
            "a selector travels without setting the flag"
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
    fn test_containment_ignores_comments_and_allows_narrowing() {
        use crate::spec::{DataField, DataType, IntType, RowType};
        let int = || DataType::Int(IntType::new());
        let child = |name: &str, desc: Option<&str>| {
            let f = DataField::new(1, name.to_string(), int());
            match desc {
                Some(d) => f.with_description(Some(d.to_string())),
                None => f,
            }
        };
        let row = |children: Vec<DataField>| DataType::Row(RowType::new(children));
        let wide = row(vec![child("a", None), child("b", None)]);

        // A comment is not a column.
        assert!(super::contains(
            &wide,
            &row(vec![child("a", Some("why")), child("b", None)])
        ));
        // Projecting a subset of the children reads nothing extra.
        assert!(super::contains(&wide, &row(vec![child("a", None)])));
        // An extra child would.
        assert!(!super::contains(
            &wide,
            &row(vec![
                child("a", None),
                child("b", None),
                child("hidden", None)
            ])
        ));
        // And so would a child under another name.
        assert!(!super::contains(&wide, &row(vec![child("c", None)])));
        // Or the same name and type re-added under a new id: older files still
        // resolve the old id.
        let readded = DataField::new(9, "a".to_string(), int());
        assert!(!super::contains(&wide, &row(vec![readded])));
    }

    #[test]
    fn test_a_variant_extraction_is_the_one_shape_change_allowed() {
        use crate::spec::{DataField, DataType, IntType, RowType, VariantType};
        let int = || DataType::Int(IntType::new());
        let row = || {
            DataType::Row(RowType::new(vec![DataField::new(0, "p".into(), int())
                .with_description(Some(
                    crate::spec::build_variant_metadata("$.p", true, "UTC").unwrap(),
                ))]))
        };
        let schema = vec![
            DataField::new(1, "v".into(), DataType::Variant(VariantType::new())),
            DataField::new(2, "n".into(), int()),
        ];
        let read = |id, name: &str, ty| vec![DataField::new(id, name.into(), ty)];

        assert!(super::reject_noncanonical_fields(&read(1, "v", row()), &schema).is_ok());
        assert!(super::reject_noncanonical_fields(&read(1, "v", int()), &schema).is_err());
        assert!(super::reject_noncanonical_fields(&read(2, "n", row()), &schema).is_err());
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
        let mut travelled = rest_query_auth_table().await;
        travelled.time_traveled = true;
        let err = travelled.authorize_read(true, None).await.unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message }
                if message.contains("time-travelled or branch read")),
            "got {err:?}"
        );

        let mut branch = rest_query_auth_table().await;
        branch.branch_reference = true;
        assert!(branch.authorize_read(true, None).await.is_err());
    }
}
