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
use std::sync::{Arc, LazyLock};

use opendal::Operator;
use regex::Regex;

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
const MANIFEST_SIDECAR_SUFFIX: &str = ".avro.sidecar";

// Data files are named {prefix}{uuid}-{count}.{extension}; this matches what follows the prefix.
const UUID: &str = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
static DATA_FILE_SUFFIX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^{UUID}-[0-9]+\..+$")).expect("valid data file pattern")
});
// Other types are routed only under the names Paimon writes; Format Table files may be replaced
// in place. TableCommit names changelog manifests manifest-{uuid}-changelog-{count}.
static META_FILE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(
        r"^(?:(?:manifest-list|index-manifest|stat)-{UUID}-[0-9]+|manifest-{UUID}(?:-changelog)?-[0-9]+(?:\.avro\.sidecar)?)$"
    ))
    .expect("valid metadata file pattern")
});
static BUCKET_INDEX_FILE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^index-{UUID}-[0-9]+$")).expect("valid bucket index file pattern")
});
static GLOBAL_INDEX_FILE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(&format!(r"^[a-z0-9_-]+-global-index-{UUID}\.index$"))
        .expect("valid global index file pattern")
});

/// Operation class of a FileIO request; only `Meta`, `Exists`, `Read` and `Write` can leave origin.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum OpClass {
    /// File status lookups, including the size lookup before a cached read.
    Meta,
    /// Existence checks, which ask whether a file is still there.
    Exists,
    Read,
    /// Writes of new files, including streaming and multipart writes.
    Write,
    /// A writer's existence checks, listing, deletes, renames, mkdirs, copies.
    Origin,
}

impl OpClass {
    fn bit(self) -> u8 {
        match self {
            Self::Meta => 1,
            Self::Read => 2,
            Self::Write => 4,
            Self::Exists => 8,
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
        // Only OSS paths can use cache targets.
        if !self.policy.contains(op) || !path.starts_with("oss://") {
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
        for op in [
            OpClass::Meta,
            OpClass::Exists,
            OpClass::Read,
            OpClass::Write,
        ] {
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

/// The type a cache may serve, or `None` when the file must be read from the origin.
pub(crate) fn routable_type(path: &str, data_prefixes: &[String]) -> Option<FileType> {
    let path = path.trim_end_matches('/');
    let name = path.rsplit('/').next().unwrap_or(path);
    // Temp files are renamed away once written, whatever prefix they carry.
    let temp = name.ends_with(".tmp") || name.contains(".tmp-") || name.contains(".tmp.");
    if temp || FileType::is_mutable(path) {
        return None;
    }
    if is_data_file_name(name, data_prefixes) {
        return Some(if name.ends_with(".index") {
            FileType::FileIndex
        } else {
            FileType::Data
        });
    }
    if is_sequential(path) {
        return None;
    }
    let file_type = FileType::classify(path);
    let paimon_name = match file_type {
        FileType::Meta => META_FILE.is_match(name),
        FileType::BucketIndex => BUCKET_INDEX_FILE.is_match(name),
        FileType::GlobalIndex => GLOBAL_INDEX_FILE.is_match(name),
        // Data files and their file indexes are recognized by `is_data_file_name`.
        _ => false,
    };
    paimon_name.then_some(file_type)
}

// Manifests, indexes and statistics share the uuid-count shape but have no extension.
fn is_data_file_name(name: &str, data_prefixes: &[String]) -> bool {
    !name.ends_with(MANIFEST_SIDECAR_SUFFIX)
        && data_prefixes.iter().any(|prefix| {
            name.strip_prefix(prefix.as_str())
                .is_some_and(|rest| DATA_FILE_SUFFIX.is_match(rest))
        })
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
            "exists" => policy.insert(OpClass::Exists),
            "read" => policy.insert(OpClass::Read),
            "write" => policy.insert(OpClass::Write),
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

    use std::collections::BTreeMap;

    const TABLE_ROOT: &str = "oss://bkt/db1.db/t1";
    const UUID: &str = "8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b";

    // paths below are relative to TABLE_ROOT, and {uuid} stands for UUID
    const DATA_PATH: &str = "dt=1/bucket-0/data-{uuid}-0.parquet";
    const MANIFEST_PATH: &str = "manifest/manifest-{uuid}-0";
    const INDEX_PATH: &str = "index/index-{uuid}-0";
    const GLOBAL_INDEX_PATH: &str = "index/btree-global-index-{uuid}.index";
    const TEMP_PATH: &str = "dt=1/bucket-0/.data-{uuid}-0.parquet.{uuid}.tmp";
    const SNAPSHOT_PATH: &str = "snapshot/snapshot-12";
    const LATEST_PATH: &str = "snapshot/LATEST";

    const OSS: &str = "https://oss-cn-hangzhou-internal.aliyuncs.com";
    const CACHE: &str = "http://cache.example.com";
    const ACCEL: &str = "https://accelerator.example.com";
    const CLUSTER: &str = "http://cluster.example.com";
    const WRITE: &str = "io-cache.policy=meta,read,write";
    const EXISTS: &str = "io-cache.policy=meta,read,exists";

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
    fn test_one_cache_target() {
        let options = single(&[]);
        assert_endpoint(&options, OpClass::Read, DATA_PATH, CACHE);
        assert_endpoint(&options, OpClass::Meta, DATA_PATH, CACHE);
        // exists asks whether a file is still there, so it needs its own policy token
        assert_endpoint(&options, OpClass::Exists, DATA_PATH, OSS);
        assert_endpoint(&single(&[EXISTS]), OpClass::Exists, DATA_PATH, CACHE);
        assert_endpoint(&single(&[EXISTS]), OpClass::Exists, SNAPSHOT_PATH, OSS);
        assert_endpoint(&options, OpClass::Read, MANIFEST_PATH, CACHE);
        assert_endpoint(
            &options,
            OpClass::Read,
            "manifest/manifest-list-{uuid}-1",
            CACHE,
        );
        let other_bucket = format!("oss://other-bkt/db1.db/t1/{DATA_PATH}");
        assert_endpoint(&options, OpClass::Read, &other_bucket, CACHE);
        assert_endpoint(&options, OpClass::Read, SNAPSHOT_PATH, OSS);
        assert_endpoint(&options, OpClass::Meta, SNAPSHOT_PATH, OSS);
        assert_endpoint(&options, OpClass::Read, LATEST_PATH, OSS);
        assert_endpoint(&options, OpClass::Meta, LATEST_PATH, OSS);
        assert_endpoint(&options, OpClass::Read, TEMP_PATH, OSS);
        assert_endpoint(&options, OpClass::Read, "dt=1/bucket-0/000000_0", OSS);
        let dls = format!("dls://bkt/db1.db/t1/{DATA_PATH}");
        assert_endpoint(&options, OpClass::Read, &dls, OSS);
        // the whitelist has no index
        assert_endpoint(&options, OpClass::Read, INDEX_PATH, OSS);
        // a Format Table file named like a manifest may be replaced in place
        let external = "review_external/manifest.parquet";
        assert_endpoint(&options, OpClass::Read, external, OSS);
        assert_endpoint(&options, OpClass::Meta, external, OSS);
        assert_endpoint(&options, OpClass::Write, DATA_PATH, OSS);
        assert_endpoint(&options, OpClass::Origin, DATA_PATH, OSS);
    }

    #[test]
    fn test_policy_and_whitelist() {
        assert_endpoint(
            &single(&["-io-cache.enabled"]),
            OpClass::Read,
            DATA_PATH,
            CACHE,
        );
        assert_endpoint(
            &single(&["io-cache.enabled=false"]),
            OpClass::Meta,
            DATA_PATH,
            CACHE,
        );
        assert_endpoint(
            &single(&["-io-cache.policy"]),
            OpClass::Meta,
            DATA_PATH,
            CACHE,
        );
        assert_endpoint(
            &single(&["io-cache.policy=none"]),
            OpClass::Meta,
            DATA_PATH,
            CACHE,
        );
        // policy tokens are trimmed, case-insensitive and whole words
        let tokens = single(&["io-cache.policy= READ , Meta "]);
        assert_endpoint(&tokens, OpClass::Meta, DATA_PATH, CACHE);
        assert_endpoint(
            &single(&["io-cache.policy=read,NONE"]),
            OpClass::Meta,
            DATA_PATH,
            CACHE,
        );
        let words = single(&["io-cache.policy=thread,metadata"]);
        assert_endpoint(&words, OpClass::Meta, DATA_PATH, CACHE);
        let unknown = single(&["io-cache.policy=read,nonetheless"]);
        assert_endpoint(&unknown, OpClass::Meta, DATA_PATH, OSS);

        let read_only = single(&["io-cache.policy=read"]);
        assert_endpoint(&read_only, OpClass::Read, DATA_PATH, CACHE);
        assert_endpoint(&read_only, OpClass::Meta, DATA_PATH, OSS);
        let write_only = single(&["io-cache.policy=write"]);
        assert_endpoint(&write_only, OpClass::Write, DATA_PATH, CACHE);
        assert_endpoint(&write_only, OpClass::Meta, DATA_PATH, OSS);
        let exists_only = single(&["io-cache.policy=exists"]);
        assert_endpoint(&exists_only, OpClass::Exists, DATA_PATH, CACHE);
        assert_endpoint(&exists_only, OpClass::Meta, DATA_PATH, OSS);

        let any_type = single(&["-io-cache.whitelist"]);
        assert_endpoint(&any_type, OpClass::Read, INDEX_PATH, CACHE);
        let star = single(&["io-cache.whitelist=*"]);
        assert_endpoint(&star, OpClass::Read, GLOBAL_INDEX_PATH, CACHE);
        let file_index = format!("{DATA_PATH}.index");
        assert_endpoint(&star, OpClass::Read, &file_index, CACHE);
    }

    #[test]
    fn test_endpoints() {
        let blank = single(&["io-cache.endpoint=  "]);
        assert_endpoint(&blank, OpClass::Read, DATA_PATH, OSS);
        let empty = single(&["io-cache.endpoint=", "io-cache.routes=*=default"]);
        assert_endpoint(&empty, OpClass::Read, DATA_PATH, OSS);
        // the origin defaults to fs.oss.endpoint
        let oss = format!("fs.oss.endpoint={OSS}");
        let without_origin = single(&[oss.as_str(), "-io-cache.origin.endpoint"]);
        assert_endpoint(&without_origin, OpClass::Origin, DATA_PATH, OSS);
        let blank_origin = single(&[oss.as_str(), "io-cache.origin.endpoint=  "]);
        assert_endpoint(&blank_origin, OpClass::Origin, DATA_PATH, OSS);
        // a client-side endpoint turns routing off
        let overridden = single(&["dlf.oss-endpoint=https://oss-cn-hangzhou.aliyuncs.com"]);
        let override_endpoint = "https://oss-cn-hangzhou.aliyuncs.com";
        assert_endpoint(&overridden, OpClass::Read, DATA_PATH, override_endpoint);
    }

    #[test]
    fn test_write_policy() {
        let options = single(&[WRITE]);
        assert_endpoint(&options, OpClass::Write, DATA_PATH, CACHE);
        assert_endpoint(&options, OpClass::Write, MANIFEST_PATH, CACHE);
        assert_endpoint(&options, OpClass::Write, SNAPSHOT_PATH, OSS);
        assert_endpoint(&options, OpClass::Write, LATEST_PATH, OSS);
        assert_endpoint(&options, OpClass::Write, TEMP_PATH, OSS);
        // atomic writes, copies and the existence check before a write stay at origin
        assert_endpoint(&options, OpClass::Origin, DATA_PATH, OSS);
        let meta_only = single(&[WRITE, "io-cache.whitelist=meta"]);
        assert_endpoint(&meta_only, OpClass::Write, DATA_PATH, OSS);

        assert_endpoint(&multi(&[WRITE]), OpClass::Write, DATA_PATH, CLUSTER);
        assert_endpoint(&multi(&[WRITE]), OpClass::Write, MANIFEST_PATH, ACCEL);
    }

    #[test]
    fn test_two_cache_targets() {
        let options = multi(&[]);
        assert_endpoint(&options, OpClass::Read, MANIFEST_PATH, ACCEL);
        assert_endpoint(&options, OpClass::Meta, MANIFEST_PATH, ACCEL);
        assert_endpoint(&options, OpClass::Exists, MANIFEST_PATH, OSS);
        assert_endpoint(&multi(&[EXISTS]), OpClass::Exists, MANIFEST_PATH, ACCEL);
        assert_endpoint(&multi(&[EXISTS]), OpClass::Exists, DATA_PATH, CLUSTER);
        assert_endpoint(&options, OpClass::Read, DATA_PATH, CLUSTER);
        assert_endpoint(&options, OpClass::Meta, DATA_PATH, CLUSTER);
        assert_endpoint(&options, OpClass::Read, INDEX_PATH, CLUSTER);
        assert_endpoint(&options, OpClass::Read, GLOBAL_INDEX_PATH, CLUSTER);
        assert_endpoint(&options, OpClass::Read, SNAPSHOT_PATH, OSS);
        assert_endpoint(&options, OpClass::Write, MANIFEST_PATH, OSS);
        assert_endpoint(&options, OpClass::Origin, MANIFEST_PATH, OSS);
        let meta_only = multi(&["io-cache.whitelist=meta"]);
        assert_endpoint(&meta_only, OpClass::Read, DATA_PATH, OSS);
    }

    #[test]
    fn test_targets_and_routes() {
        // without routes the first target with an endpoint takes every type
        let no_routes = "-io-cache.routes";
        assert_endpoint(&multi(&[no_routes]), OpClass::Read, DATA_PATH, ACCEL);
        let shorthand = format!("io-cache.endpoint={CACHE}");
        let with_shorthand = multi(&[no_routes, shorthand.as_str()]);
        assert_endpoint(&with_shorthand, OpClass::Read, DATA_PATH, ACCEL);
        let without_accel = multi(&[no_routes, "-io-cache.target.accel.endpoint"]);
        assert_endpoint(&without_accel, OpClass::Read, DATA_PATH, CLUSTER);
        let blank_accel = multi(&[no_routes, "io-cache.target.accel.endpoint=  "]);
        assert_endpoint(&blank_accel, OpClass::Read, DATA_PATH, CLUSTER);
        let bad_name = multi(&[no_routes, "io-cache.targets=Bad_Name,cluster"]);
        assert_endpoint(&bad_name, OpClass::Read, DATA_PATH, CLUSTER);
        let padded = format!("io-cache.target.cluster.endpoint=  {CLUSTER}  ");
        assert_endpoint(
            &multi(&[padded.as_str()]),
            OpClass::Read,
            DATA_PATH,
            CLUSTER,
        );

        // a type without a usable route goes to the origin
        let undeclared = multi(&["io-cache.routes=data=x"]);
        assert_endpoint(&undeclared, OpClass::Read, DATA_PATH, OSS);
        let without_cluster = multi(&["-io-cache.target.cluster.endpoint"]);
        assert_endpoint(&without_cluster, OpClass::Read, DATA_PATH, OSS);
        let meta_and_data = multi(&["io-cache.routes=meta=accel;data=cluster"]);
        let file_index = format!("{DATA_PATH}.index");
        assert_endpoint(&meta_and_data, OpClass::Read, &file_index, OSS);

        let star = multi(&["io-cache.routes=*=cluster"]);
        assert_endpoint(&star, OpClass::Read, MANIFEST_PATH, CLUSTER);
        let first_wins = multi(&["io-cache.routes=data=accel;data,meta=cluster"]);
        assert_endpoint(&first_wins, OpClass::Read, DATA_PATH, ACCEL);
        let malformed = multi(&["io-cache.routes==cluster;meta=accel;junk"]);
        assert_endpoint(&malformed, OpClass::Read, MANIFEST_PATH, ACCEL);
        assert_endpoint(&malformed, OpClass::Read, DATA_PATH, OSS);
    }

    #[test]
    fn test_data_files_named_like_metadata() {
        let manifest = multi(&["data-file.prefix=manifest-"]);
        let named_like_manifest = "dt=1/bucket-0/manifest-{uuid}-0.orc";
        assert_endpoint(&manifest, OpClass::Read, named_like_manifest, CLUSTER);
        let entropy = "dt=1/bucket-0/7f3a/manifest-{uuid}-0.orc";
        assert_endpoint(&manifest, OpClass::Read, entropy, CLUSTER);
        assert_endpoint(&manifest, OpClass::Read, MANIFEST_PATH, ACCEL);
        let sidecar = format!("{MANIFEST_PATH}.avro.sidecar");
        assert_endpoint(&manifest, OpClass::Read, &sidecar, ACCEL);
        let snapshot = multi(&["data-file.prefix=snapshot-"]);
        let named_like_snapshot = "dt=1/bucket-0/snapshot-{uuid}-0.orc";
        assert_endpoint(&snapshot, OpClass::Read, named_like_snapshot, CLUSTER);
        let stat = multi(&["data-file.prefix=stat-"]);
        assert_endpoint(
            &stat,
            OpClass::Read,
            "dt=1/bucket-0/stat-{uuid}-0.orc",
            CLUSTER,
        );
        let index = multi(&[
            "io-cache.routes=data=cluster;bucket-index=accel",
            "data-file.prefix=index-",
        ]);
        assert_endpoint(
            &index,
            OpClass::Read,
            "dt=1/bucket-0/index-{uuid}-0.orc",
            CLUSTER,
        );
        assert_endpoint(&index, OpClass::Read, INDEX_PATH, ACCEL);

        let custom = "dt=1/bucket-0/custom-{uuid}-0.orc";
        assert_endpoint(&single(&[]), OpClass::Read, custom, OSS);
        let custom_prefix = single(&["data-file.prefix=custom-"]);
        assert_endpoint(&custom_prefix, OpClass::Read, custom, CACHE);
    }

    #[test]
    fn test_cacheable_type() {
        // named by sequence id or rewritten in place
        for path in [
            "snapshot/snapshot-12",
            "snapshot/LATEST",
            "snapshot/EARLIEST",
            "branch/branch-dev/snapshot/LATEST",
            "schema/schema-3",
            "changelog/changelog-5",
            "changelog/LATEST",
            "tag/tag-2026-09-30",
            "tag/tag-success-file/t1_SUCCESS",
            "consumer/consumer-job1",
            "service/service-primary-key-lookup",
            "dt=1/_SUCCESS",
            "oss://bkt/bucket-0/db/t/changelog/changelog-5",
        ] {
            assert_type(path, None, &[]);
        }
        // temporary or not written by Paimon
        for path in [
            "snapshot/.snapshot-13.{uuid}.tmp",
            "dt=1/bucket-0/.data-{uuid}-0.parquet.{uuid}.tmp",
            "dt=1/bucket-0/data-{uuid}-0.parquet.tmp-{uuid}",
            "dt=1/bucket-0/data-{uuid}-0.parquet.tmp.{uuid}",
            "metadata/version-hint.text",
            "metadata/v3.metadata.json",
            "dt=1/bucket-0/000000_0",
            "dt=1/part-00000-1a2b.snappy.parquet",
            "dt=1/bucket-0/data-1.parquet",
            "dt=1/bucket-0/custom-{uuid}-0.orc",
            "README",
        ] {
            assert_type(path, None, &[]);
        }
        // Format Table files named like metadata or indexes, which may be replaced in place
        for path in [
            "dt=1/manifest.parquet",
            "dt=1/stat-2024.parquet",
            "dt=1/index-a.csv",
            "dt=1/foo.index",
            "review_external/manifest.parquet",
            "manifest/manifest-old",
            "manifest/manifest-old.avro.sidecar",
            "manifest/manifest-list-{uuid}-1.avro.sidecar",
            "manifest/manifest-list-{uuid}-changelog-1",
            "manifest/manifest-{uuid}-changelog",
            "statistics/stat-old",
            "index/index-old",
            "index/my-global-index.index",
            "index/btree-global-index-{uuid}-0.index",
        ] {
            assert_type(path, None, &[]);
        }

        assert_type("manifest/manifest-{uuid}-0", Some(FileType::Meta), &[]);
        assert_type("manifest/manifest-list-{uuid}-1", Some(FileType::Meta), &[]);
        assert_type(
            "manifest/index-manifest-{uuid}-0",
            Some(FileType::Meta),
            &[],
        );
        let sidecar = "manifest/manifest-{uuid}-0.avro.sidecar";
        assert_type(sidecar, Some(FileType::Meta), &[]);
        let changelog = "manifest/manifest-{uuid}-changelog-0";
        assert_type(changelog, Some(FileType::Meta), &[]);
        let changelog_sidecar = "manifest/manifest-{uuid}-changelog-0.avro.sidecar";
        assert_type(changelog_sidecar, Some(FileType::Meta), &[]);
        assert_type("statistics/stat-{uuid}-0", Some(FileType::Meta), &[]);
        assert_type(
            "dt=1/bucket-0/data-{uuid}-0.parquet",
            Some(FileType::Data),
            &[],
        );
        assert_type(
            "dt=1/bucket-0/changelog-{uuid}-0.parquet",
            Some(FileType::Data),
            &[],
        );
        assert_type(
            "dt=1/bucket-0/data-{uuid}-0.blob",
            Some(FileType::Data),
            &[],
        );
        assert_type(
            "dt=1/8b1f7c2e/data-{uuid}-0.parquet",
            Some(FileType::Data),
            &[],
        );
        let file_index = "dt=1/bucket-0/data-{uuid}-0.parquet.index";
        assert_type(file_index, Some(FileType::FileIndex), &[]);
        assert_type("index/index-{uuid}-0", Some(FileType::BucketIndex), &[]);
        let btree = "index/btree-global-index-{uuid}.index";
        assert_type(btree, Some(FileType::GlobalIndex), &[]);
        let lumina = "index/lumina-global-index-{uuid}.index";
        assert_type(lumina, Some(FileType::GlobalIndex), &[]);
    }

    #[test]
    fn test_cacheable_type_with_file_prefixes() {
        let data = Some(FileType::Data);
        let custom = &["data-file.prefix=custom-"];
        assert_type("dt=1/bucket-0/custom-{uuid}-0.orc", data, custom);
        assert_type("dt=1/bucket-0/data-{uuid}-0.orc", data, custom);
        let changelog = &["changelog-file.prefix=cl-"];
        assert_type("dt=1/bucket-0/cl-{uuid}-0.orc", data, changelog);
        assert_type(
            "dt=1/bucket-0/  unknown.parquet",
            None,
            &["data-file.prefix=  "],
        );

        // the name decides, so data files may be named like metadata
        let manifest = &["data-file.prefix=manifest-"];
        assert_type("dt=1/bucket-0/manifest-{uuid}-0.orc", data, manifest);
        assert_type("dt=1/bucket-0/7f3a/manifest-{uuid}-0.orc", data, manifest);
        let file_index = Some(FileType::FileIndex);
        assert_type(
            "dt=1/bucket-0/manifest-{uuid}-0.orc.index",
            file_index,
            manifest,
        );
        let meta = Some(FileType::Meta);
        assert_type("manifest/manifest-{uuid}-0", meta, manifest);
        assert_type("manifest/manifest-{uuid}-0.avro.sidecar", meta, manifest);
        let index = &["data-file.prefix=index-"];
        let bucket_index = Some(FileType::BucketIndex);
        assert_type("dt=1/bucket-0/index-{uuid}-0.orc", data, index);
        assert_type("bucket-postpone/index-{uuid}-0.orc", data, index);
        assert_type("index/index-{uuid}-0", bucket_index, index);
        assert_type("dt=1/bucket-0/index-{uuid}-0", bucket_index, index);
        let stat = &["data-file.prefix=stat-"];
        assert_type("dt=1/bucket-0/stat-{uuid}-0.orc", data, stat);
        assert_type("postpone/stat-{uuid}-0.orc", data, stat);
        assert_type("statistics/stat-{uuid}-0", meta, stat);
        let snapshot = &["data-file.prefix=snapshot-"];
        assert_type("dt=1/bucket-0/snapshot-{uuid}-0.orc", data, snapshot);
        assert_type(
            "dt=1/bucket-0/snapshot-{uuid}-0.orc.index",
            file_index,
            snapshot,
        );
        let schema = &["data-file.prefix=schema-"];
        assert_type("dt=1/bucket-0/schema-{uuid}-0.orc", data, schema);
        let global_index = &["data-file.prefix=global-index-"];
        assert_type(
            "dt=1/bucket-0/global-index-{uuid}-0.orc.index",
            file_index,
            global_index,
        );

        // names rewritten in place never use a cache, whatever the prefix
        assert_type(
            "dt=1/bucket-0/tag-{uuid}-0.orc",
            None,
            &["data-file.prefix=tag-"],
        );
        let consumer = &["data-file.prefix=consumer-"];
        assert_type("dt=1/bucket-0/consumer-{uuid}-0.orc", None, consumer);
        for (name, prefix) in [
            ("tag/tag-file", "data-file.prefix=tag-"),
            ("consumer/consumer-file", "data-file.prefix=consumer-"),
            ("service/service-file", "data-file.prefix=service-"),
            ("metadata/version-hint.text", "data-file.prefix=version-"),
            ("branch/branch-feature", "data-file.prefix=branch-"),
        ] {
            let path = format!("oss://bkt/warehouse/bucket-0/db/t1/{name}");
            assert_type(&path, None, &[prefix]);
        }
    }

    #[test]
    fn test_rust_temp_names_are_not_routable() {
        let prefixes = data_prefixes(&HashMap::new());
        for path in [
            "oss://b/t/snapshot/snapshot-1.tmp-123e4567-e89b-12d3-a456-426614174000",
            "oss://b/t/bucket-0/data-123e4567-e89b-12d3-a456-426614174000-1.parquet.tmp-123e4567-e89b-12d3-a456-426614174000",
        ] {
            assert_eq!(routable_type(path, &prefixes), None, "{path}");
        }
        assert_eq!(
            routable_type(
                "oss://b/t/bucket-0/data-123e4567-e89b-12d3-a456-426614174000-0.parquet/",
                &prefixes
            ),
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
        let data = "oss://b/t/bucket-0/data-123e4567-e89b-12d3-a456-426614174000-1.parquet";
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
            routing
                .cache_target("oss://b/t/manifest/manifest-123e4567-e89b-12d3-a456-426614174000-1"),
            Some((0, both))
        );
        assert_eq!(
            routing.cache_target(
                "oss://b/t/bucket-0/data-123e4567-e89b-12d3-a456-426614174000-1.parquet"
            ),
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
        let data = "oss://b/t/bucket-0/data-123e4567-e89b-12d3-a456-426614174000-1.parquet";

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

    /// One cache target, which older clients also get as fs.oss.endpoint.
    fn single(changes: &[&str]) -> HashMap<String, String> {
        let base = [
            (OSS_ENDPOINT, CACHE),
            (IO_CACHE_ENABLED, "true"),
            (IO_CACHE_ENDPOINT, CACHE),
            (IO_CACHE_ORIGIN_ENDPOINT, OSS),
            (IO_CACHE_POLICY, "meta,read"),
            (IO_CACHE_WHITELIST, "meta,data"),
        ];
        with(&base, changes)
    }

    /// Metadata on an accelerator, data and indexes on a cache cluster.
    fn multi(changes: &[&str]) -> HashMap<String, String> {
        let base = [
            (OSS_ENDPOINT, ACCEL),
            (IO_CACHE_ENABLED, "true"),
            (IO_CACHE_ORIGIN_ENDPOINT, OSS),
            (IO_CACHE_TARGETS, "accel,cluster"),
            ("io-cache.target.accel.endpoint", ACCEL),
            ("io-cache.target.accel.region", "cn-hangzhou"),
            ("io-cache.target.cluster.endpoint", CLUSTER),
            ("io-cache.target.cluster.path-style-access", "true"),
            (IO_CACHE_POLICY, "meta,read"),
            (IO_CACHE_WHITELIST, "*"),
            (
                IO_CACHE_ROUTES,
                "meta=accel;data,bucket-index,global-index,file-index=cluster",
            ),
        ];
        with(&base, changes)
    }

    // "key=value" sets an option and "-key" removes it
    fn with(base: &[(&str, &str)], changes: &[&str]) -> HashMap<String, String> {
        let mut options: HashMap<String, String> = base
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        for change in changes {
            if let Some(key) = change.strip_prefix('-') {
                options.remove(key);
            } else {
                let (key, value) = change.split_once('=').unwrap();
                options.insert(key.to_string(), value.to_string());
            }
        }
        options
    }

    fn resolve(path: &str) -> String {
        let path = path.replace("{uuid}", UUID);
        if path.contains("://") {
            path
        } else {
            format!("{TABLE_ROOT}/{path}")
        }
    }

    #[track_caller]
    fn assert_endpoint(options: &HashMap<String, String>, op: OpClass, path: &str, expect: &str) {
        let routing = IoCacheRouting::from_props(options);
        let sorted: BTreeMap<_, _> = options.iter().collect();
        assert_eq!(
            routing.endpoint(routing.route(op, &resolve(path))),
            Some(expect),
            "{op:?} {path} with {sorted:?}"
        );
    }

    #[track_caller]
    fn assert_type(path: &str, expect: Option<FileType>, changes: &[&str]) {
        let prefixes = data_prefixes(&with(&[], changes));
        assert_eq!(
            routable_type(&resolve(path), &prefixes),
            expect,
            "{path} with {changes:?}"
        );
    }
}
