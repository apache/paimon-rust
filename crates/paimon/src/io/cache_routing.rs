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

//! Per-request choice between io-cache targets and the origin OSS endpoint, following the
//! `io-cache.*` options that a REST catalog may vend with a table token.

#![cfg_attr(not(feature = "storage-oss"), allow(dead_code))]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use opendal::Operator;

use crate::common::CatalogOptions;
use crate::io::cache::FileType;

/// OSS endpoint, compatible with paimon-java's `fs.oss.endpoint`.
pub(crate) const OSS_ENDPOINT: &str = "fs.oss.endpoint";
pub(crate) const IO_CACHE_ENABLED: &str = "io-cache.enabled";
pub(crate) const IO_CACHE_ORIGIN_ENDPOINT: &str = "io-cache.origin.endpoint";
pub(crate) const IO_CACHE_POLICY: &str = "io-cache.policy";
pub(crate) const IO_CACHE_WHITELIST: &str = "io-cache.whitelist";
/// Single-target shorthand for a target named `default`, ignored when `io-cache.targets` is set.
pub(crate) const IO_CACHE_ENDPOINT: &str = "io-cache.endpoint";
pub(crate) const IO_CACHE_TARGETS: &str = "io-cache.targets";
/// Prefix of `io-cache.target.<name>.{endpoint,path-style-access,region}`.
pub(crate) const IO_CACHE_TARGET_PREFIX: &str = "io-cache.target.";
pub(crate) const IO_CACHE_ROUTES: &str = "io-cache.routes";

const DEFAULT_TARGET: &str = "default";
const DATA_FILE_PREFIX: &str = "data-file.prefix";
const CHANGELOG_FILE_PREFIX: &str = "changelog-file.prefix";
const DEFAULT_DATA_PREFIXES: [&str; 2] = ["data-", "changelog-"];

/// Operation class of a FileIO request; only `Meta` and `Read` can leave origin.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum OpClass {
    /// File status, including the size lookup before a cached read.
    Meta,
    Read,
    /// Existence checks, writes, listing, deletes, renames, mkdirs, copies and atomic writes.
    Origin,
}

impl OpClass {
    fn bit(self) -> u8 {
        match self {
            Self::Meta => 1,
            Self::Read => 2,
            Self::Origin => 0,
        }
    }
}

/// A set of [`OpClass`]es; `Origin` is never a member.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OpClasses(u8);

impl OpClasses {
    fn insert(&mut self, op: OpClass) {
        self.0 |= op.bit();
    }

    pub(crate) fn contains(self, op: OpClass) -> bool {
        self.0 & op.bit() != 0
    }

    pub(crate) fn is_empty(self) -> bool {
        self.0 == 0
    }
}

/// Endpoint chosen for a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Target {
    /// `fs.oss.endpoint`, what clients without io-cache routing use.
    Default,
    /// `dlf.oss-endpoint` set on the client.
    Override,
    /// `io-cache.origin.endpoint`, defaulting to `fs.oss.endpoint`.
    Origin,
    /// The io-cache target at this index of [`IoCacheRouting::targets`].
    Cache(usize),
}

/// A declared io-cache target.
#[derive(Debug)]
pub(crate) struct CacheTarget {
    pub(crate) name: String,
    /// `None` when the endpoint is missing or empty.
    pub(crate) endpoint: Option<String>,
    pub(crate) path_style_access: bool,
}

/// The `io-cache.*` keys of merged table-token options.
#[derive(Debug)]
pub(crate) struct IoCacheRouting {
    override_endpoint: Option<String>,
    default_endpoint: Option<String>,
    origin_endpoint: Option<String>,
    enabled: bool,
    policy: OpClasses,
    whitelist: HashSet<FileType>,
    targets: Vec<CacheTarget>,
    /// Ordered `(file types, target name)` rules, when `io-cache.routes` is set.
    routes: Option<Vec<(HashSet<FileType>, String)>>,
    data_prefixes: Vec<String>,
}

impl IoCacheRouting {
    pub(crate) fn from_props(props: &HashMap<String, String>) -> Self {
        let non_empty = |key: &str| props.get(key).filter(|v| !v.trim().is_empty()).cloned();
        let default_endpoint = non_empty(OSS_ENDPOINT);
        Self {
            override_endpoint: non_empty(CatalogOptions::DLF_OSS_ENDPOINT),
            origin_endpoint: non_empty(IO_CACHE_ORIGIN_ENDPOINT)
                .or_else(|| default_endpoint.clone()),
            default_endpoint,
            enabled: flag(props, IO_CACHE_ENABLED, false),
            policy: parse_policy(props.get(IO_CACHE_POLICY).map(String::as_str)),
            whitelist: FileType::parse_whitelist(
                &props
                    .get(IO_CACHE_WHITELIST)
                    .map_or("*".to_string(), |value| value.to_ascii_lowercase()),
            ),
            targets: parse_targets(props),
            routes: props.get(IO_CACHE_ROUTES).map(|value| parse_routes(value)),
            data_prefixes: data_prefixes(props),
        }
    }

    /// Whether requests may go anywhere other than the endpoint of clients without routing.
    pub(crate) fn enabled(&self) -> bool {
        self.override_endpoint.is_none() && self.has_targets()
    }

    fn has_targets(&self) -> bool {
        self.enabled && !self.targets.is_empty() && !self.policy.is_empty()
    }

    pub(crate) fn targets(&self) -> &[CacheTarget] {
        &self.targets
    }

    /// Applies the routing rules in order, the first match winning.
    pub(crate) fn route(&self, op: OpClass, path: &str) -> Target {
        if self.override_endpoint.is_some() {
            return Target::Override;
        }
        if !self.has_targets() {
            return Target::Default;
        }
        if !self.policy.contains(op) {
            return Target::Origin;
        }
        let Some(file_type) =
            routable_type(path, &self.data_prefixes).filter(|t| self.whitelist.contains(t))
        else {
            return Target::Origin;
        };
        let chosen = match &self.routes {
            Some(routes) => routes
                .iter()
                .find(|(types, _)| types.contains(&file_type))
                .and_then(|(_, name)| self.targets.iter().position(|t| t.name == *name)),
            None => self.targets.iter().position(|t| t.endpoint.is_some()),
        };
        match chosen {
            Some(index) if self.targets[index].endpoint.is_some() => Target::Cache(index),
            _ => Target::Origin,
        }
    }

    /// The target that [`Self::route`] picks for `path`, and the operation classes it serves.
    pub(crate) fn cache_target(&self, path: &str) -> Option<(usize, OpClasses)> {
        let mut found = None;
        let mut classes = OpClasses::default();
        for op in [OpClass::Meta, OpClass::Read] {
            if let Target::Cache(index) = self.route(op, path) {
                found = Some(index);
                classes.insert(op);
            }
        }
        found.map(|index| (index, classes))
    }

    /// Endpoint of requests that never leave origin.
    pub(crate) fn origin(&self) -> Option<&str> {
        self.endpoint(self.route(OpClass::Origin, ""))
    }

    pub(crate) fn endpoint(&self, target: Target) -> Option<&str> {
        match target {
            Target::Default => self.default_endpoint.as_deref(),
            Target::Override => self.override_endpoint.as_deref(),
            Target::Origin => self.origin_endpoint.as_deref(),
            Target::Cache(index) => self.targets.get(index)?.endpoint.as_deref(),
        }
    }
}

/// The type a cache may serve; `None` for mutable, sequential or unknown files.
pub(crate) fn routable_type(path: &str, data_prefixes: &[String]) -> Option<FileType> {
    let path = path.trim_end_matches('/');
    if FileType::is_mutable(path) || is_sequential(path) {
        return None;
    }
    let file_type = FileType::classify(path);
    let name = path.rsplit('/').next().unwrap_or(path);
    (file_type != FileType::Data || data_prefixes.iter().any(|p| name.starts_with(p.as_str())))
        .then_some(file_type)
}

// Sequential metadata may be read before it exists; cached NotFound would hide new commits.
fn is_sequential(path: &str) -> bool {
    let mut segments = path.rsplit('/');
    let name = segments.next().unwrap_or(path);
    name.starts_with("snapshot-")
        || name.starts_with("schema-")
        || (name.starts_with("changelog-") && segments.next() == Some("changelog"))
}

fn flag(props: &HashMap<String, String>, key: &str, default: bool) -> bool {
    props
        .get(key)
        .map_or(default, |value| value.trim().eq_ignore_ascii_case("true"))
}

/// Unknown tokens, including `write`, are ignored, and `none` empties the policy.
fn parse_policy(value: Option<&str>) -> OpClasses {
    let mut policy = OpClasses::default();
    for token in value.unwrap_or_default().split(',') {
        match token.trim().to_ascii_lowercase().as_str() {
            "none" => return OpClasses::default(),
            "meta" => policy.insert(OpClass::Meta),
            "read" => policy.insert(OpClass::Read),
            _ => {}
        }
    }
    policy
}

/// Declared targets in order; `io-cache.targets` wins over the `io-cache.endpoint` shorthand.
fn parse_targets(props: &HashMap<String, String>) -> Vec<CacheTarget> {
    // OpenDAL's OSS service signs without a region, so `io-cache.target.<name>.region` is ignored.
    let target = |name: String, endpoint_key: &str| {
        let path_style_key = format!("{IO_CACHE_TARGET_PREFIX}{name}.path-style-access");
        CacheTarget {
            endpoint: props
                .get(endpoint_key)
                .map(|endpoint| endpoint.trim())
                .filter(|endpoint| !endpoint.is_empty())
                .map(String::from),
            path_style_access: flag(props, &path_style_key, false),
            name,
        }
    };
    let Some(names) = props.get(IO_CACHE_TARGETS) else {
        return match props.get(IO_CACHE_ENDPOINT) {
            Some(_) => vec![target(DEFAULT_TARGET.to_string(), IO_CACHE_ENDPOINT)],
            None => Vec::new(),
        };
    };
    let mut unique: Vec<String> = Vec::new();
    for name in names.split(',') {
        let name = name.trim().to_ascii_lowercase();
        if is_valid_target_name(&name) && !unique.contains(&name) {
            unique.push(name);
        }
    }
    unique
        .into_iter()
        .map(|name| {
            let endpoint_key = format!("{IO_CACHE_TARGET_PREFIX}{name}.endpoint");
            target(name, &endpoint_key)
        })
        .collect()
}

/// `[a-z][a-z0-9-]*`.
fn is_valid_target_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// Rules `types=target` separated by `;`; malformed rules are skipped.
fn parse_routes(value: &str) -> Vec<(HashSet<FileType>, String)> {
    value
        .split(';')
        .filter_map(|rule| {
            let (types, target) = rule.trim().split_once('=')?;
            let target = target.trim().to_ascii_lowercase();
            let types = FileType::parse_whitelist(&types.to_ascii_lowercase());
            (!target.is_empty() && !types.is_empty()).then_some((types, target))
        })
        .collect()
}

/// `data-` and `changelog-`, plus the table's own prefixes when they are in the options.
fn data_prefixes(props: &HashMap<String, String>) -> Vec<String> {
    let mut prefixes: Vec<String> = DEFAULT_DATA_PREFIXES.map(String::from).to_vec();
    for key in [DATA_FILE_PREFIX, CHANGELOG_FILE_PREFIX] {
        if let Some(prefix) = props.get(key).filter(|prefix| !prefix.trim().is_empty()) {
            if !prefixes.contains(prefix) {
                prefixes.push(prefix.clone());
            }
        }
    }
    prefixes
}

/// Operators serving one path: origin, plus the io-cache target when the path may use one.
#[derive(Clone, Debug)]
pub struct RoutedOperator {
    origin: Operator,
    target: Option<Arc<TargetOperator>>,
}

#[derive(Debug)]
struct TargetOperator {
    operator: Operator,
    classes: OpClasses,
}

impl RoutedOperator {
    pub(crate) fn origin(op: Operator) -> Self {
        Self {
            origin: op,
            target: None,
        }
    }

    pub(crate) fn with_target(origin: Operator, target: Operator, classes: OpClasses) -> Self {
        Self {
            origin,
            target: (!classes.is_empty()).then(|| {
                Arc::new(TargetOperator {
                    operator: target,
                    classes,
                })
            }),
        }
    }

    pub(crate) fn without_cache(self) -> Self {
        Self::origin(self.origin)
    }

    pub(crate) fn origin_operator(&self) -> &Operator {
        &self.origin
    }

    /// The operator for a request of class `op`: the io-cache target when it serves `op`.
    pub(crate) fn operator(&self, op: OpClass) -> &Operator {
        match self.target.as_deref() {
            Some(target) if target.classes.contains(op) => &target.operator,
            _ => &self.origin,
        }
    }

    #[cfg(test)]
    pub(crate) fn target(&self) -> Option<&Operator> {
        self.target.as_deref().map(|target| &target.operator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Vectors<T> {
        cases: Vec<T>,
    }

    #[derive(Deserialize)]
    struct RoutingCase {
        name: String,
        options: HashMap<String, String>,
        op: String,
        path: String,
        expect: String,
    }

    #[derive(Deserialize)]
    struct RoutableTypeCase {
        path: String,
        options: HashMap<String, String>,
        #[serde(rename = "type")]
        file_type: Option<String>,
    }

    fn load<T: serde::de::DeserializeOwned>(name: &str) -> Vectors<T> {
        let path = format!("{}/testdata/io_cache/{name}", env!("CARGO_MANIFEST_DIR"));
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
    }

    /// Maps the operation names of the vectors to classes.
    fn op_class(name: &str) -> OpClass {
        match name {
            "read" => OpClass::Read,
            "meta" => OpClass::Meta,
            "exists" | "write" | "list" | "delete" | "rename" | "mkdirs" | "copy"
            | "atomic-write" | "two-phase-write" | "presign" => OpClass::Origin,
            other => panic!("unknown operation {other}"),
        }
    }

    fn routing(options: &[(&str, &str)]) -> IoCacheRouting {
        IoCacheRouting::from_props(
            &options
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    fn route(routing: &IoCacheRouting, op: OpClass, path: &str) -> Option<String> {
        routing.endpoint(routing.route(op, path)).map(String::from)
    }

    #[test]
    fn test_routing_vectors() {
        let vectors: Vectors<RoutingCase> = load("routing.json");
        assert!(!vectors.cases.is_empty());
        for case in vectors.cases {
            let routing = IoCacheRouting::from_props(&case.options);
            let target = routing.route(op_class(&case.op), &case.path);
            assert_eq!(
                routing.endpoint(target),
                Some(case.expect.as_str()),
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn test_routable_type_vectors() {
        let vectors: Vectors<RoutableTypeCase> = load("routable-types.json");
        assert!(!vectors.cases.is_empty());
        for case in vectors.cases {
            let expected = case
                .file_type
                .as_deref()
                .and_then(|name| FileType::parse_whitelist(name).into_iter().next());
            assert_eq!(
                routable_type(&case.path, &data_prefixes(&case.options)),
                expected,
                "{}",
                case.path
            );
        }
    }

    #[test]
    fn test_rust_temp_names_are_not_routable() {
        let prefixes = data_prefixes(&HashMap::new());
        for path in [
            "oss://b/t/snapshot/snapshot-1.tmp-123e4567-e89b-12d3-a456-426614174000",
            "oss://b/t/bucket-0/data-1.parquet.tmp-123e4567-e89b-12d3-a456-426614174000",
        ] {
            assert_eq!(routable_type(path, &prefixes), None, "{path}");
        }
        assert_eq!(
            routable_type("oss://b/t/bucket-0/data-1.parquet/", &prefixes),
            Some(FileType::Data)
        );
    }

    #[test]
    fn test_policy_whitelist_and_flags_are_case_insensitive() {
        let routing = routing(&[
            (OSS_ENDPOINT, "origin"),
            (IO_CACHE_ENABLED, " TRUE "),
            (IO_CACHE_ENDPOINT, "cache"),
            (IO_CACHE_POLICY, " READ , Meta , write"),
            (IO_CACHE_WHITELIST, "DATA"),
        ]);
        let data = "oss://b/t/bucket-0/data-1.parquet";
        assert_eq!(route(&routing, OpClass::Read, data).unwrap(), "cache");
        assert_eq!(route(&routing, OpClass::Meta, data).unwrap(), "cache");
        assert_eq!(route(&routing, OpClass::Origin, data).unwrap(), "origin");
        assert_eq!(
            route(&routing, OpClass::Read, "oss://b/t/manifest/manifest-1").unwrap(),
            "origin"
        );
    }

    #[test]
    fn test_cache_target_is_shared_by_read_and_meta() {
        let routing = routing(&[
            (OSS_ENDPOINT, "origin"),
            (IO_CACHE_ENABLED, "true"),
            (IO_CACHE_TARGETS, "accel, cluster"),
            ("io-cache.target.accel.endpoint", "accel"),
            ("io-cache.target.cluster.endpoint", "cluster"),
            ("io-cache.target.cluster.path-style-access", "true"),
            (IO_CACHE_POLICY, "meta,read"),
            (IO_CACHE_ROUTES, "meta=accel;data=cluster"),
        ]);
        let both = OpClasses(OpClass::Meta.bit() | OpClass::Read.bit());
        assert_eq!(
            routing.cache_target("oss://b/t/manifest/manifest-1"),
            Some((0, both))
        );
        assert_eq!(
            routing.cache_target("oss://b/t/bucket-0/data-1.parquet"),
            Some((1, both))
        );
        assert_eq!(routing.cache_target("oss://b/t/snapshot/snapshot-1"), None);
        assert_eq!(routing.cache_target("oss://b/t/index/index-1"), None);
        assert!(!routing.targets()[0].path_style_access);
        assert!(routing.targets()[1].path_style_access);
    }

    #[test]
    fn test_shorthand_target_settings() {
        let routing = routing(&[
            (OSS_ENDPOINT, "origin"),
            (IO_CACHE_ENABLED, "true"),
            (IO_CACHE_ENDPOINT, "http://cache"),
            ("io-cache.target.default.path-style-access", "true"),
            (IO_CACHE_POLICY, "read"),
        ]);
        let [target] = routing.targets() else {
            panic!("expected one target");
        };
        assert_eq!(target.name, DEFAULT_TARGET);
        assert_eq!(target.endpoint.as_deref(), Some("http://cache"));
        assert!(target.path_style_access);
    }

    #[test]
    fn test_routing_is_off_without_enabled_targets_or_with_override() {
        let base = [
            (OSS_ENDPOINT, "default"),
            (IO_CACHE_ORIGIN_ENDPOINT, "origin"),
            (IO_CACHE_ENDPOINT, "cache"),
            (IO_CACHE_POLICY, "read"),
        ];
        let data = "oss://b/t/bucket-0/data-1.parquet";

        let disabled = routing(&base);
        assert!(!disabled.enabled());
        assert_eq!(route(&disabled, OpClass::Read, data).unwrap(), "default");
        assert_eq!(disabled.origin(), Some("default"));

        let enabled = routing(&[&base[..], &[(IO_CACHE_ENABLED, "true")]].concat());
        assert!(enabled.enabled());
        assert_eq!(enabled.origin(), Some("origin"));

        let without_targets = routing(&[
            (OSS_ENDPOINT, "default"),
            (IO_CACHE_ENABLED, "true"),
            (IO_CACHE_POLICY, "read"),
        ]);
        assert!(!without_targets.enabled());

        let overridden = routing(
            &[
                &base[..],
                &[
                    (IO_CACHE_ENABLED, "true"),
                    (CatalogOptions::DLF_OSS_ENDPOINT, "override"),
                ],
            ]
            .concat(),
        );
        assert!(!overridden.enabled());
        assert_eq!(route(&overridden, OpClass::Read, data).unwrap(), "override");
        assert_eq!(overridden.origin(), Some("override"));
    }
}
