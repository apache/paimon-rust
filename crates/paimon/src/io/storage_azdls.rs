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

use opendal::services::AzdlsConfig;
use opendal::{Configurator, Operator};
use url::Url;

use crate::error::Error;
use crate::Result;

use super::storage_config::normalize_storage_config;

const AZURE_ENDPOINT: &str = "azure.endpoint";
const AZURE_ACCOUNT_NAME: &str = "azure.account-name";
const AZURE_ACCOUNT_KEY: &str = "azure.account-key";
const AZURE_SAS_TOKEN: &str = "azure.sas-token";

const CONFIG_PREFIXES: &[&str] = &["fs.azure.", "fs.abfs.", "abfs.", "abfss.", "azure."];
const MIRRORED_KEYS: &[(&str, &str)] = &[
    ("azure.account-name", "azure.account.name"),
    ("azure.account_name", "azure.account.name"),
    ("azure.account-key", "azure.account.key"),
    ("azure.account_key", "azure.account.key"),
    ("azure.sas-token", "azure.sas.token"),
    ("azure.sas_token", "azure.sas.token"),
    ("azure.client-id", "azure.client.id"),
    ("azure.client_id", "azure.client.id"),
    ("azure.client-secret", "azure.client.secret"),
    ("azure.client_secret", "azure.client.secret"),
    ("azure.tenant-id", "azure.tenant.id"),
    ("azure.tenant_id", "azure.tenant.id"),
    ("azure.authority-host", "azure.authority.host"),
    ("azure.authority_host", "azure.authority.host"),
];

#[derive(Debug, Clone)]
pub struct AzdlsStorageConfig {
    config: AzdlsConfig,
    normalized: HashMap<String, String>,
}

pub(crate) fn azdls_config_parse(props: HashMap<String, String>) -> Result<AzdlsStorageConfig> {
    let normalized = normalize_storage_config(props, CONFIG_PREFIXES, "azure.", MIRRORED_KEYS);
    let config = config_from_normalized(&normalized);

    Ok(AzdlsStorageConfig { config, normalized })
}

pub(crate) fn azdls_config_build(cfg: &AzdlsStorageConfig, path: &str) -> Result<Operator> {
    let (cfg, relative_path) = azdls_config_for_path(cfg, path)?;

    let builder = cfg.into_builder();
    let op = Operator::new(builder)?.finish();

    debug_assert_eq!(
        relative_path,
        azdls_relative_path(path).unwrap_or(relative_path)
    );
    Ok(op)
}

pub(crate) fn azdls_operator_cache_key(cfg: &AzdlsStorageConfig, path: &str) -> Result<String> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid Azure url: {path}"),
    })?;
    let filesystem = if cfg.config.filesystem.is_empty() {
        filesystem_from_url(&url, path)?
    } else {
        cfg.config.filesystem.clone()
    };
    let endpoint = effective_endpoint(&cfg.config, &url)?;

    Ok(format!("{}|{}", endpoint.trim_end_matches('/'), filesystem))
}

fn azdls_config_for_path<'a>(
    storage_cfg: &AzdlsStorageConfig,
    path: &'a str,
) -> Result<(AzdlsConfig, &'a str)> {
    let (filesystem, relative_path) = azdls_filesystem_and_relative_path(path)?;
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid Azure url: {path}"),
    })?;

    let mut cfg = storage_cfg.config.clone();
    if cfg.filesystem.is_empty() {
        cfg.filesystem = filesystem;
    }

    let endpoint = effective_endpoint(&cfg, &url)?;
    apply_account_scoped_config(&mut cfg, &storage_cfg.normalized, &endpoint);
    cfg.endpoint = Some(endpoint);
    cfg.root = Some("/".to_string());

    Ok((cfg, relative_path))
}

fn config_from_normalized(normalized: &HashMap<String, String>) -> AzdlsConfig {
    AzdlsConfig {
        endpoint: normalized.get(AZURE_ENDPOINT).cloned(),
        account_name: normalized.get(AZURE_ACCOUNT_NAME).cloned(),
        account_key: normalized.get(AZURE_ACCOUNT_KEY).cloned(),
        sas_token: normalized.get(AZURE_SAS_TOKEN).cloned(),
        client_id: normalized.get("azure.client-id").cloned(),
        client_secret: normalized.get("azure.client-secret").cloned(),
        tenant_id: normalized.get("azure.tenant-id").cloned(),
        authority_host: normalized.get("azure.authority-host").cloned(),
        ..Default::default()
    }
}

fn effective_endpoint(cfg: &AzdlsConfig, url: &Url) -> Result<String> {
    cfg.endpoint
        .as_ref()
        .map(|endpoint| endpoint.trim_end_matches('/').to_string())
        .map(Ok)
        .unwrap_or_else(|| default_endpoint(url))
}

pub(crate) fn azdls_filesystem_and_relative_path(path: &str) -> Result<(String, &str)> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid Azure url: {path}"),
    })?;

    let filesystem = filesystem_from_url(&url, path)?;

    Ok((filesystem, azdls_relative_path(path)?))
}

pub(crate) fn azdls_relative_path(path: &str) -> Result<&str> {
    let url = Url::parse(path).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid Azure url: {path}"),
    })?;

    let path_start = path
        .find("://")
        .map(|pos| pos + 3)
        .ok_or_else(|| Error::ConfigInvalid {
            message: format!("Invalid Azure url: {path}"),
        })?;
    let after_scheme = &path[path_start..];
    let path_start = after_scheme.find('/').map(|pos| path_start + pos + 1);
    let url_path = path_start.map(|pos| &path[pos..]).unwrap_or("");

    if !url.username().is_empty()
        || !url
            .host_str()
            .ok_or_else(|| Error::ConfigInvalid {
                message: format!("Invalid Azure url: {path}, missing filesystem"),
            })?
            .contains('.')
    {
        Ok(url_path)
    } else {
        let (_filesystem, relative_path) = url_path.split_once('/').unwrap_or((url_path, ""));
        Ok(relative_path)
    }
}

fn filesystem_from_url(url: &Url, path: &str) -> Result<String> {
    if !url.username().is_empty() {
        return Ok(url.username().to_string());
    }

    let host = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid Azure url: {path}, missing filesystem"),
    })?;

    if !host.contains('.') {
        return Ok(host.to_string());
    }

    url.path()
        .strip_prefix('/')
        .unwrap_or(url.path())
        .split('/')
        .next()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Error::ConfigInvalid {
            message: format!("Invalid Azure url: {path}, missing filesystem"),
        })
        .map(|v| v.to_string())
}

fn default_endpoint(url: &Url) -> Result<String> {
    if !url.username().is_empty() {
        let host = url.host_str().ok_or_else(|| Error::ConfigInvalid {
            message: format!("Invalid Azure url: {url}, missing account host"),
        })?;
        return Ok(format!("https://{host}"));
    }

    let host = url.host_str().ok_or_else(|| Error::ConfigInvalid {
        message: format!("Invalid Azure url: {url}, missing account"),
    })?;

    if host.contains('.') {
        Ok(format!("https://{host}"))
    } else {
        Err(Error::ConfigInvalid {
            message: format!(
                "Invalid Azure url: {url}, missing account host; set azure.endpoint for {host}"
            ),
        })
    }
}

fn apply_account_scoped_config(
    cfg: &mut AzdlsConfig,
    normalized: &HashMap<String, String>,
    endpoint: &str,
) {
    let Some(host) = endpoint_host(endpoint) else {
        return;
    };
    let account = host.split('.').next().unwrap_or(host.as_str());

    if cfg.account_key.is_none() {
        cfg.account_key = first_scoped_value(
            normalized,
            &[
                "azure.account.key",
                "azure.account-key",
                "azure.account_key",
            ],
            &[host.as_str(), account],
        );
    }

    if cfg.sas_token.is_none() {
        cfg.sas_token = first_scoped_value(
            normalized,
            &[
                "azure.sas.token",
                "azure.sas-token",
                "azure.sas_token",
                "azure.sas.fixed.token",
                "azure.fixed.sas.token",
            ],
            &[host.as_str(), account],
        );
    }
}

fn endpoint_host(endpoint: &str) -> Option<String> {
    Url::parse(endpoint)
        .ok()
        .and_then(|url| url.host_str().map(|host| host.to_string()))
}

fn first_scoped_value(
    normalized: &HashMap<String, String>,
    prefixes: &[&str],
    suffixes: &[&str],
) -> Option<String> {
    prefixes.iter().find_map(|prefix| {
        suffixes
            .iter()
            .find_map(|suffix| normalized.get(&format!("{prefix}.{suffix}")).cloned())
    })
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
    fn test_azdls_config_parse_keys() {
        let props = make_props(&[
            ("fs.azure.account.key", "key"),
            ("fs.azure.sas.token", "sas"),
            ("azure.endpoint", "https://account.dfs.core.windows.net"),
        ]);

        let cfg = azdls_config_parse(props).unwrap();
        assert_eq!(
            cfg.config.endpoint.as_deref(),
            Some("https://account.dfs.core.windows.net")
        );
        assert_eq!(cfg.config.account_key.as_deref(), Some("key"));
        assert_eq!(cfg.config.sas_token.as_deref(), Some("sas"));
    }

    #[test]
    fn test_azdls_config_parse_aliases() {
        let props = make_props(&[
            ("azure.account-name", "account"),
            ("azure.client-secret", "secret"),
            ("azure.tenant-id", "tenant"),
        ]);

        let cfg = azdls_config_parse(props).unwrap();
        assert_eq!(cfg.config.account_name.as_deref(), Some("account"));
        assert_eq!(cfg.config.client_secret.as_deref(), Some("secret"));
        assert_eq!(cfg.config.tenant_id.as_deref(), Some("tenant"));
    }

    #[test]
    fn test_azdls_config_uses_account_scoped_hadoop_key() {
        let cfg = azdls_config_parse(make_props(&[(
            "fs.azure.account.key.account.dfs.core.windows.net",
            "account-key",
        )]))
        .unwrap();

        let (cfg, _) =
            azdls_config_for_path(&cfg, "abfs://fs@account.dfs.core.windows.net/path/to/file")
                .unwrap();

        assert_eq!(cfg.account_key.as_deref(), Some("account-key"));
    }

    #[test]
    fn test_azdls_path_hadoop_authority_form() {
        let (filesystem, relative_path) =
            azdls_filesystem_and_relative_path("abfs://fs@account.dfs.core.windows.net/a/b")
                .unwrap();
        assert_eq!(filesystem, "fs");
        assert_eq!(relative_path, "a/b");
    }

    #[test]
    fn test_azdls_path_fsspec_form() {
        let (filesystem, relative_path) =
            azdls_filesystem_and_relative_path("abfs://fs/a/b").unwrap();
        assert_eq!(filesystem, "fs");
        assert_eq!(relative_path, "a/b");
    }

    #[test]
    fn test_azdls_config_build_hadoop_form() {
        let cfg = azdls_config_parse(HashMap::new()).unwrap();

        let op = azdls_config_build(&cfg, "abfs://fs@account.dfs.core.windows.net/a/b").unwrap();
        assert_eq!(op.info().name(), "fs");
    }

    #[test]
    fn test_azdls_config_build_fsspec_form_requires_endpoint() {
        let cfg = azdls_config_parse(HashMap::new()).unwrap();
        let result = azdls_config_build(&cfg, "abfs://fs/a/b");
        assert!(result.is_err());
    }

    #[test]
    fn test_azdls_config_build_fsspec_form_with_endpoint() {
        let cfg = azdls_config_parse(make_props(&[(
            "azure.endpoint",
            "https://account.dfs.core.windows.net",
        )]))
        .unwrap();

        let op = azdls_config_build(&cfg, "abfs://fs/a/b").unwrap();
        assert_eq!(op.info().name(), "fs");
    }

    #[test]
    fn test_azdls_cache_key_includes_account_host() {
        let cfg = azdls_config_parse(HashMap::new()).unwrap();

        let account_a = azdls_operator_cache_key(
            &cfg,
            "abfs://fs@account-a.dfs.core.windows.net/path/to/file",
        )
        .unwrap();
        let account_b = azdls_operator_cache_key(
            &cfg,
            "abfs://fs@account-b.dfs.core.windows.net/path/to/file",
        )
        .unwrap();

        assert_ne!(account_a, account_b);
        assert_eq!(account_a, "https://account-a.dfs.core.windows.net|fs");
    }

    #[test]
    fn test_azdls_config_build_missing_filesystem() {
        let cfg = azdls_config_parse(HashMap::new()).unwrap();
        let result = azdls_config_build(&cfg, "abfs:///path/without/filesystem");
        assert!(result.is_err());
    }
}
