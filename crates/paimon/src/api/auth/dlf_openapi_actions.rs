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

//! Java's DlfNext 2026-01-18 REST route to OpenAPI action mapping.

const OPERATIONS: &[(&str, &str, &str)] = &[
    ("GET", "/v1/config", "GetConfig"),
    ("GET", "/v1/{prefix}/databases", "ListDatabases"),
    ("POST", "/v1/{prefix}/databases", "CreateDatabase"),
    ("GET", "/v1/{prefix}/databases/{database}", "GetDatabase"),
    ("POST", "/v1/{prefix}/databases/{database}", "AlterDatabase"),
    (
        "DELETE",
        "/v1/{prefix}/databases/{database}",
        "DropDatabase",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/register",
        "RegisterTable",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables",
        "ListTables",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables",
        "CreateTable",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/table-details",
        "ListTableDetails",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}",
        "GetTable",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}",
        "AlterTable",
    ),
    (
        "DELETE",
        "/v1/{prefix}/databases/{database}/tables/{table}",
        "DropTable",
    ),
    ("POST", "/v1/{prefix}/tables/rename", "RenameTable"),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/auth",
        "AuthTableQuery",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/commit",
        "CommitTable",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}/token",
        "GetTableToken",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/rollback",
        "RollbackToSnapshot",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}/snapshot",
        "GetTableSnapshot",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}/snapshots",
        "ListSnapshots",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}/snapshots/{version}",
        "GetVersionSnapshot",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}/partitions",
        "ListPartitions",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/partitions",
        "CreatePartitions",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/partitions/drop",
        "DropPartitions",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/partitions/mark",
        "MarkDonePartitions",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/partitions/list-by-names",
        "ListPartitionsByNames",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/partitions/list-by-filter",
        "ListPartitionsByFilter",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}/branches",
        "ListBranches",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/branches",
        "CreateBranch",
    ),
    (
        "DELETE",
        "/v1/{prefix}/databases/{database}/tables/{table}/branches/{branch}",
        "DropBranch",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/branches/{branch}/forward",
        "FastForwardBranch",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}/tags",
        "ListTags",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/tags",
        "CreateTag",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}/tags/{tag}",
        "GetTag",
    ),
    (
        "DELETE",
        "/v1/{prefix}/databases/{database}/tables/{table}/tags/{tag}",
        "DropTag",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/tables/{table}/policies",
        "ListPolicies",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/policies",
        "CreatePolicy",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/tables/{table}/policies/drop",
        "DropPolicy",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/views",
        "ListViews",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/views",
        "CreateView",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/view-details",
        "ListViewDetails",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/views/{view}",
        "GetView",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/views/{view}",
        "AlterView",
    ),
    (
        "DELETE",
        "/v1/{prefix}/databases/{database}/views/{view}",
        "DropView",
    ),
    ("POST", "/v1/{prefix}/views/rename", "RenameView"),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/semantic-views",
        "ListSemanticViews",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/semantic-views/{semanticView}",
        "GetSemanticView",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/semantic-views/{semanticView}",
        "UpsertSemanticView",
    ),
    (
        "DELETE",
        "/v1/{prefix}/databases/{database}/semantic-views/{semanticView}",
        "DeleteSemanticView",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/functions",
        "ListFunctions",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/functions",
        "CreateFunction",
    ),
    (
        "GET",
        "/v1/{prefix}/databases/{database}/functions/{function}",
        "GetFunction",
    ),
    (
        "POST",
        "/v1/{prefix}/databases/{database}/functions/{function}",
        "AlterFunction",
    ),
    (
        "DELETE",
        "/v1/{prefix}/databases/{database}/functions/{function}",
        "DropFunction",
    ),
    (
        "GET",
        "/v1/{prefix}/permissions",
        "ListPermissionAssignments",
    ),
    (
        "POST",
        "/v1/{prefix}/permissions/grant",
        "GrantPermissionAssignment",
    ),
    (
        "POST",
        "/v1/{prefix}/permissions/revoke",
        "RevokePermissionAssignment",
    ),
    ("GET", "/v1/{prefix}/labels", "ListLabels"),
    ("POST", "/v1/{prefix}/labels/get", "GetLabel"),
    ("POST", "/v1/{prefix}/labels/upsert", "UpsertLabel"),
    ("POST", "/v1/{prefix}/labels/delete", "DeleteLabel"),
];

pub(super) fn resolve_action(method: &str, path: &str) -> Option<&'static str> {
    OPERATIONS.iter().find_map(|(verb, template, action)| {
        (method.eq_ignore_ascii_case(verb)
            && template.split('/').count() == path.split('/').count()
            && template
                .split('/')
                .zip(path.split('/'))
                .all(|(part, segment)| {
                    if part.starts_with('{') {
                        !segment.is_empty()
                    } else {
                        part == segment
                    }
                }))
        .then_some(*action)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_java_operation_resolves_without_collisions() {
        for (method, template, action) in OPERATIONS {
            let path = template
                .split('/')
                .map(|part| if part.starts_with('{') { "x1" } else { part })
                .collect::<Vec<_>>()
                .join("/");
            assert_eq!(
                resolve_action(method, &path),
                Some(*action),
                "{method} {path}"
            );
        }
    }

    #[test]
    fn action_resolution_preserves_encoded_names_and_literal_boundaries() {
        assert_eq!(
            resolve_action("get", "/v1/rename/databases/tables"),
            Some("GetDatabase")
        );
        assert_eq!(
            resolve_action("GET", "/v1/p/databases/a%2Fb/tables/c%20d"),
            Some("GetTable")
        );
        assert_eq!(
            resolve_action("DELETE", "/v1/p/databases/db/tables/t/branches/forward"),
            Some("DropBranch")
        );
        for (method, path) in [
            ("GET", "/v1/p/tables"),
            ("GET", "/v1/config/"),
            ("GET", "/v1//databases"),
            ("PUT", "/v1/p/databases/db/tables/t"),
            ("POST", "/v1/p/databases/db/tables/t/branches/b/rename"),
        ] {
            assert_eq!(resolve_action(method, path), None, "{method} {path}");
        }
    }
}
