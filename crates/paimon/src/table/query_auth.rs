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

#[cfg(test)]
mod numeric_cast_java_test;
mod row_filter;
mod transform;
pub(crate) use row_filter::RowFilter;
mod mask_output;
mod rules;

use crate::api::AuthTableQueryResponse;
pub(crate) use rules::{filter_batch, mask_batch, mask_inputs, ColumnMask, Rules};

/// The server's answer for one user on one table; `session` ties it to the
/// handle that asked, as the response names no table or user.
#[derive(Debug)]
pub(crate) struct QueryAuthGrant {
    session: u64,
    /// The columns the request asked about; `None` asked about the whole table.
    select: Option<Vec<String>>,
    rules: Rules,
    /// Original wire rules for Java QueryAuthSplit metadata transport.
    response: Option<AuthTableQueryResponse>,
    /// The read schema and branch for which the rules were compiled.
    binding: Option<(i64, Vec<crate::spec::DataField>, String)>,
    /// Historical reads can need NULL columns introduced after their snapshot.
    fields: Option<Vec<crate::spec::DataField>>,
    current_schema: Option<(i64, Vec<crate::spec::DataField>)>,
}

impl PartialEq for QueryAuthGrant {
    fn eq(&self, other: &Self) -> bool {
        if std::ptr::eq(self, other) {
            return true;
        }
        self.session == other.session
            && self.select == other.select
            && self.rules.same_policy(&other.rules)
            && self.binding == other.binding
            && self.fields == other.fields
    }
}

impl QueryAuthGrant {
    /// Parses the rules against `fields`, the schema the server ruled on.
    pub(crate) fn parse(
        response: &AuthTableQueryResponse,
        session: u64,
        select: Option<Vec<String>>,
        fields: &[crate::spec::DataField],
    ) -> crate::Result<Self> {
        let mut grant = Self::new(session, select, Rules::parse(response, fields)?);
        grant.response = Some(response.clone());
        Ok(grant)
    }

    pub(crate) fn new(session: u64, select: Option<Vec<String>>, rules: Rules) -> Self {
        Self {
            session,
            select,
            rules,
            response: None,
            binding: None,
            fields: None,
            current_schema: None,
        }
    }

    pub(crate) fn response(&self) -> Option<&AuthTableQueryResponse> {
        self.response.as_ref()
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

    pub(crate) fn bind(
        mut self,
        table: &super::Table,
        fields: Vec<crate::spec::DataField>,
        current: (i64, &[crate::spec::DataField]),
    ) -> Self {
        self.binding = Some((
            table.schema().id(),
            table.schema().fields().to_vec(),
            table.branch().into(),
        ));
        self.fields = Some(fields);
        self.current_schema = Some((current.0, current.1.to_vec()));
        self
    }

    pub(crate) fn current_schema(&self) -> Option<(i64, &[crate::spec::DataField])> {
        self.current_schema
            .as_ref()
            .map(|(id, fields)| (*id, fields.as_slice()))
    }

    pub(crate) fn fields<'a>(&'a self, table: &'a super::Table) -> &'a [crate::spec::DataField] {
        self.fields
            .as_deref()
            .unwrap_or_else(|| table.schema().fields())
    }

    pub(crate) fn matches_table(&self, table: &super::Table) -> bool {
        table.query_auth_session() == Some(self.session)
            && match &self.binding {
                Some((id, fields, branch)) => {
                    *id == table.schema().id()
                        && fields == table.schema().fields()
                        && branch == table.branch()
                }
                None => !table.reads_another_schema().unwrap_or(true),
            }
    }
}

/// Java validates rules against the latest schema, then binds them by name to
/// the snapshot. A rename or a dropped-and-recreated rule operand is unrelated
/// data, even when one of its name and id still matches.
pub(crate) fn historical_rule_fields(
    response: &AuthTableQueryResponse,
    current: &[crate::spec::DataField],
    read: &[crate::spec::DataField],
) -> crate::Result<Vec<crate::spec::DataField>> {
    let rules = Rules::parse(response, current)?;
    let mut required = rules.filter_columns();
    for mask in &rules.masks {
        required.insert(mask.column);
        if read
            .iter()
            .any(|field| field.name() == current[mask.column].name())
        {
            required.extend(mask_inputs(mask));
        }
    }
    for index in required {
        let field = &current[index];
        if let Some(old) = read.iter().find(|old| old.name() == field.name()) {
            if old.id() != field.id() {
                return Err(unsupported(&format!(
                    "rule column '{}' was dropped and re-added",
                    field.name()
                )));
            }
        } else if read.iter().any(|old| old.id() == field.id()) {
            return Err(unsupported(&format!(
                "rule column '{}' was renamed",
                field.name()
            )));
        }
    }
    // Append missing operands without disturbing the historical indices used
    // by user predicates. Schema evolution supplies NULL for these new fields.
    let mut fields = read.to_vec();
    for field in current {
        if !fields.iter().any(|old| old.name() == field.name()) {
            fields.push(field.clone());
        }
    }
    Ok(fields)
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
            Predicate::NestedLeaf { root, .. } => {
                out.insert(root.name().to_string());
            }
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
            Predicate::NestedLeaf { .. } => predicate.validate_nested_fields(fields),
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
    use crate::table::rest_query_auth_table;

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
    async fn test_options_only_schema_replacement_keeps_its_session() {
        let table = crate::table::rest_query_auth_table().await;
        assert!(table.query_auth_session().is_some());
        let mut json = serde_json::to_value(table.schema()).unwrap();
        json["timeMillis"] = serde_json::json!(1);
        let copy = table
            .copy_with_resolved_schema(serde_json::from_value(json).unwrap(), "main")
            .unwrap();
        assert_eq!(copy.query_auth_session(), table.query_auth_session());
        let table = table.copy_with_options(std::collections::HashMap::from([
            ("scan.snapshot-id".into(), "7".into()),
            ("read.batch-size".into(), "99".into()),
        ]));
        let schema = table
            .schema()
            .copy_with_replaced_options(std::collections::HashMap::from([
                ("query-auth.enabled".into(), "false".into()),
                ("type".into(), "format".into()),
                ("data-file.path-directory".into(), "/tmp/other".into()),
            ]));
        let copy = table.copy_with_resolved_schema(schema, "main").unwrap();
        assert_eq!(copy.query_auth_session(), table.query_auth_session());
        assert!(copy.schema().core_options().query_auth_enabled());
        for key in [
            "scan.snapshot-id",
            "read.batch-size",
            "type",
            "data-file.path-directory",
        ] {
            assert!(!copy.schema().options().contains_key(key), "{key}");
        }
        assert!(!copy.reads_another_schema().unwrap());
    }

    #[tokio::test]
    async fn test_changed_schema_or_branch_loses_its_session() {
        let table = crate::table::rest_query_auth_table().await;
        let mut json = serde_json::to_value(table.schema()).unwrap();
        json["id"] = serde_json::json!(table.schema().id() + 1);
        let changed = serde_json::from_value(json).unwrap();
        assert!(table
            .copy_with_resolved_schema(changed, "main")
            .unwrap()
            .query_auth_session()
            .is_none());
        assert!(table
            .copy_with_resolved_schema(table.schema().clone(), "test")
            .unwrap()
            .query_auth_session()
            .is_none());
        let assembled = crate::table::query_auth_table();
        assert!(assembled
            .copy_with_resolved_schema(assembled.schema().clone(), "main")
            .unwrap()
            .query_auth_session()
            .is_none());
    }

    #[tokio::test]
    async fn test_resolved_schema_copy_preserves_verified_historical_session() {
        let table = crate::table::rest_query_auth_table().await;
        let grant = super::QueryAuthGrant::new(
            table.query_auth_session().unwrap(),
            None,
            Default::default(),
        );
        let mut travelled = table.clone();
        travelled.time_traveled = true;
        let pinned = table.copy_with_pinned_snapshot(None);
        for view in [travelled, pinned] {
            let schema = view
                .schema()
                .copy_with_replaced_options(table.schema().options().clone());
            let copy = view.copy_with_resolved_schema(schema, "main").unwrap();
            assert_eq!(copy.query_auth_session(), table.query_auth_session());
            assert!(copy.is_time_traveled());
            assert!(!grant.matches_table(&copy));
        }
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
    async fn a_grant_is_bound_to_the_verified_read_schema_and_branch() {
        let table = rest_query_auth_table()
            .await
            .copy_with_pinned_snapshot(None);
        let grant = super::QueryAuthGrant::new(
            table.query_auth_session().unwrap(),
            None,
            Default::default(),
        )
        .bind(
            &table,
            table.schema().fields().to_vec(),
            (table.schema().id(), table.schema().fields()),
        );
        assert!(grant.matches_table(&table));
        let mut other = table.clone();
        other.branch = "other".into();
        assert!(!grant.matches_table(&other));
        let mut value = serde_json::to_value(table.schema()).unwrap();
        value["fields"][0]["name"] = serde_json::json!("renamed");
        other = table.clone();
        other.schema = serde_json::from_value(value).unwrap();
        assert!(!grant.matches_table(&other));
    }

    #[test]
    fn historical_rules_reject_renames_and_readded_columns() {
        use crate::spec::{DataField, DataType, IntType};
        let field = |id, name: &str| DataField::new(id, name.into(), DataType::Int(IntType::new()));
        let current = [field(1, "id"), field(2, "n")];
        let response = crate::api::AuthTableQueryResponse {
            filter: Some(vec![
                serde_json::json!({"kind":"LEAF", "function":"GREATER_THAN", "literals":[0],
                "transform":{"name":"FIELD_REF", "fieldRef":{"name":"n", "type":"INT", "index":1}}})
                .to_string(),
            ]),
            column_masking: None,
        };
        for read in [
            vec![field(1, "id"), field(2, "old_n")],
            vec![field(1, "id"), field(3, "n")],
        ] {
            assert!(super::historical_rule_fields(&response, &current, &read).is_err());
        }
        let read = vec![field(1, "id")];
        let fields = super::historical_rule_fields(&response, &current, &read).unwrap();
        assert_eq!(fields, current);
        // Unrelated schema renames are legal when no rule references the field.
        let unrestricted = crate::api::AuthTableQueryResponse {
            filter: None,
            column_masking: None,
        };
        assert!(
            super::historical_rule_fields(&unrestricted, &current, &[field(1, "renamed")]).is_ok()
        );
    }
}
