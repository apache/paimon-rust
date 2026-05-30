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

use std::collections::HashMap;

use opendal::services::GcsConfig;
use opendal::{Configurator, Operator};
use url::Url;

use crate::error::Error;
use crate::Result;

use super::storage_config::normalize_storage_config;

const GCS_ENDPOINT: &str = "gcs.endpoint";
const GCS_CREDENTIAL: &str = "gcs.credential";
const GCS_CREDENTIAL_PATH: &str = "gcs.credential-path";
const GCS_SERVICE_ACCOUNT: &str = "gcs.service-account";
const GCS_ALLOW_ANONYMOUS: &str = "gcs.allow-anonymous";

const CONFIG_PREFIXES: &[&str] = &["fs.gs.", "fs.gcs.", "gs.", "gcs."];
const MIRRORED_KEYS: &[(&str, &str)] = &[
    ("gcs.credential-path", "gcs.google_application_credentials"),
    ("gcs.credential-path", "gcs.google-application-credentials"),
    ("gcs.credential-path", "gcs.application-credentials"),
    ("gcs.credential", "gcs.google_service_account_key"),
    ("gcs.credential", "gcs.google-service-account-key"),
    ("gcs.credential", "gcs.service-account-key"),
    ("gcs.credential", "gcs.service_account_key"),
    ("gcs.service-account", "gcs.google_service_account"),
    ("gcs.service-account", "gcs.google-service-account"),
    ("gcs.service-account", "gcs.service_account"),
    ("gcs.predefined-acl", "gcs.predefined_acl"),
    ("gcs.default-storage-class", "gcs.default_storage_class"),
    ("gcs.allow-anonymous", "gcs.google_skip_signature"),
    ("gcs.allow-anonymous", "gcs.google-skip-signature"),
    ("gcs.allow_anonymous", "gcs.google_skip_signature"),
    ("gcs.allow-anonymous", "gcs.allow_anonymous"),
    ("gcs.allow-anonymous", "gcs.skip-signature"),
    ("gcs.allow-anonymous", "gcs.skip_signature"),
    ("gcs.skip-signature", "gcs.google_skip_signature"),
    ("gcs.skip_signature", "gcs.google_skip_signature"),
    ("gcs.disable-vm-metadata", "gcs.disable_vm_metadata"),
    ("gcs.disable-config-load", "gcs.disable_config_load"),
];

#[allow(clippy::field_reassign_with_default)]
pub(crate) fn gcs_config_parse(props: HashMap<String, String>) -> Result<GcsConfig> {
    let normalized = normalize_storage_config(props, CONFIG_PREFIXES, "gcs.", MIRRORED_KEYS);

    let mut cfg = GcsConfig::default();
    cfg.endpoint = normalized.get(GCS_ENDPOINT).cloned();
    cfg.credential = normalized.get(GCS_CREDENTIAL).cloned();
    cfg.credential_path = normalized.get(GCS_CREDENTIAL_PATH).cloned();
    cfg.service_account = normalized.get(GCS_SERVICE_ACCOUNT).cloned();
    cfg.scope = normalized.get("gcs.scope").cloned();
    cfg.predefined_acl = normalized.get("gcs.predefined-acl").cloned();
    cfg.default_storage_class = normalized.get("gcs.default-storage-class").cloned();
    cfg.token = normalized.get("gcs.token").cloned();
    cfg.allow_anonymous = normalized
        .get(GCS_ALLOW_ANONYMOUS)
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    cfg.disable_vm_metadata = normalized
        .get("gcs.disable-vm-metadata")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    cfg.disable_config_load = normalized
        .get("gcs.disable-config-load")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));

    Ok(cfg)
}

pub(crate) fn gcs_config_build(cfg: &GcsConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid GCS url: {path}"),
    })?;

    let bucket = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid GCS url: {path}, missing bucket"),
    })?;

    let builder = cfg.clone().into_builder().bucket(bucket);
    Ok(Operator::new(builder)?.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_gcs_config_parse_keys() {
        let props = make_props(&[
            ("fs.gs.endpoint", "https://storage.googleapis.com"),
            ("fs.gs.google_application_credentials", "/tmp/gcs.json"),
            ("fs.gs.google_service_account_key", "credential-json"),
            (
                "fs.gs.google_service_account",
                "sa@example.iam.gserviceaccount.com",
            ),
            ("fs.gs.predefined_acl", "bucketOwnerFullControl"),
            ("fs.gs.default_storage_class", "NEARLINE"),
        ]);

        let cfg = gcs_config_parse(props).unwrap();
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("https://storage.googleapis.com")
        );
        assert_eq!(cfg.credential_path.as_deref(), Some("/tmp/gcs.json"));
        assert_eq!(cfg.credential.as_deref(), Some("credential-json"));
        assert_eq!(
            cfg.service_account.as_deref(),
            Some("sa@example.iam.gserviceaccount.com")
        );
        assert_eq!(
            cfg.predefined_acl.as_deref(),
            Some("bucketOwnerFullControl")
        );
        assert_eq!(cfg.default_storage_class.as_deref(), Some("NEARLINE"));
    }

    #[test]
    fn test_gcs_config_parse_canonical_aliases() {
        let props = make_props(&[
            ("gcs.credential-path", "/tmp/gcs.json"),
            ("gcs.allow-anonymous", "true"),
        ]);

        let cfg = gcs_config_parse(props).unwrap();
        assert_eq!(cfg.credential_path.as_deref(), Some("/tmp/gcs.json"));
        assert!(cfg.allow_anonymous);
    }

    #[test]
    fn test_gcs_config_parse_opendal_aliases() {
        let props = make_props(&[
            (
                "gcs.google_application_credentials",
                "/tmp/opendal-gcs.json",
            ),
            ("gcs.google_service_account_key", "credential-json"),
            (
                "gcs.google_service_account",
                "opendal-sa@example.iam.gserviceaccount.com",
            ),
            ("gcs.google_skip_signature", "true"),
            ("gcs.disable_vm_metadata", "true"),
            ("gcs.disable_config_load", "true"),
        ]);

        let cfg = gcs_config_parse(props).unwrap();
        assert_eq!(
            cfg.credential_path.as_deref(),
            Some("/tmp/opendal-gcs.json")
        );
        assert_eq!(cfg.credential.as_deref(), Some("credential-json"));
        assert_eq!(
            cfg.service_account.as_deref(),
            Some("opendal-sa@example.iam.gserviceaccount.com")
        );
        assert!(cfg.allow_anonymous);
        assert!(cfg.disable_vm_metadata);
        assert!(cfg.disable_config_load);
    }

    #[test]
    fn test_gcs_config_build_extracts_bucket() {
        let cfg = GcsConfig::default();

        let op = gcs_config_build(&cfg, "gs://my-bucket/some/path").unwrap();
        assert_eq!(op.info().name(), "my-bucket");
    }

    #[test]
    fn test_gcs_config_build_missing_bucket() {
        let cfg = GcsConfig::default();
        let result = gcs_config_build(&cfg, "gs:///path/without/bucket");
        assert!(result.is_err());
    }
}
