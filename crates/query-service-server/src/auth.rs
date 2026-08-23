// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use paimon_query_service::{TableLookupPolicy, TableRef};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

const MIN_TOKEN_BYTES: usize = 16;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PrincipalConfig {
    pub name: String,
    #[serde(default)]
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub bearer_token_env: Option<String>,
    pub grants: Vec<TableGrant>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TableGrant {
    pub table: TableRef,
    #[serde(default)]
    pub blob_fields: Option<BTreeSet<String>>,
}

#[derive(Debug, Clone)]
pub(crate) struct Principal {
    name: String,
    grants: Option<BTreeMap<TableRef, BTreeSet<String>>>,
}

impl Principal {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn allows(&self, table: &TableRef, blob_fields: &[String]) -> bool {
        let Some(grants) = &self.grants else {
            return true;
        };
        grants
            .get(table)
            .is_some_and(|allowed| blob_fields.iter().all(|field| allowed.contains(field)))
    }
}

#[derive(Clone)]
pub(crate) struct Credential {
    token_hash: [u8; 32],
    principal: Principal,
}

#[derive(Clone)]
pub(crate) enum AuthPolicy {
    Anonymous(Principal),
    Credentials(Vec<Credential>),
}

impl AuthPolicy {
    pub(crate) fn build(
        allow_anonymous: bool,
        legacy_token: Option<&str>,
        legacy_token_env: Option<&str>,
        principals: &[PrincipalConfig],
        policies: &[TableLookupPolicy],
    ) -> Result<Self, AuthConfigError> {
        if allow_anonymous
            && (!principals.is_empty() || legacy_token.is_some() || legacy_token_env.is_some())
        {
            return Err(AuthConfigError(
                "allowAnonymous cannot be combined with bearer-token authentication".to_string(),
            ));
        }
        if !principals.is_empty() && (legacy_token.is_some() || legacy_token_env.is_some()) {
            return Err(AuthConfigError(
                "principals cannot be combined with bearerToken or bearerTokenEnv".to_string(),
            ));
        }

        if principals.is_empty() {
            let Some(token) = resolve_token(legacy_token, legacy_token_env, "legacy bearer token")?
            else {
                if !allow_anonymous {
                    return Err(AuthConfigError(
                        "authentication is required unless allowAnonymous is explicitly true"
                            .to_string(),
                    ));
                }
                return Ok(Self::Anonymous(Principal {
                    name: "anonymous".to_string(),
                    grants: None,
                }));
            };
            validate_token(&token, "legacy bearer token")?;
            return Ok(Self::Credentials(vec![Credential {
                token_hash: hash_token(&token),
                principal: Principal {
                    name: "legacy".to_string(),
                    grants: None,
                },
            }]));
        }

        let policy_by_table = policies
            .iter()
            .map(|policy| (&policy.table, policy))
            .collect::<BTreeMap<_, _>>();
        let mut names = HashSet::new();
        let mut token_hashes = HashSet::new();
        let mut credentials = Vec::with_capacity(principals.len());
        for config in principals {
            if config.name.trim().is_empty() || !names.insert(config.name.clone()) {
                return Err(AuthConfigError(format!(
                    "principal name must be non-empty and unique: {:?}",
                    config.name
                )));
            }
            let token = resolve_token(
                config.bearer_token.as_deref(),
                config.bearer_token_env.as_deref(),
                &format!("principal '{}'", config.name),
            )?
            .ok_or_else(|| {
                AuthConfigError(format!(
                    "principal '{}' must configure bearerToken or bearerTokenEnv",
                    config.name
                ))
            })?;
            validate_token(&token, &format!("principal '{}'", config.name))?;
            let token_hash = hash_token(&token);
            if !token_hashes.insert(token_hash) {
                return Err(AuthConfigError(
                    "bearer tokens must be unique across principals".to_string(),
                ));
            }
            if config.grants.is_empty() {
                return Err(AuthConfigError(format!(
                    "principal '{}' has no table grants",
                    config.name
                )));
            }

            let mut grants = BTreeMap::new();
            for grant in &config.grants {
                let policy = policy_by_table.get(&grant.table).ok_or_else(|| {
                    AuthConfigError(format!(
                        "principal '{}' grants unknown table {}.{}",
                        config.name, grant.table.database, grant.table.table
                    ))
                })?;
                let allowed = match &grant.blob_fields {
                    None => policy.blob_fields.clone(),
                    Some(fields) if fields.is_empty() => {
                        return Err(AuthConfigError(format!(
                            "principal '{}' has an empty BLOB field grant for {}.{}",
                            config.name, grant.table.database, grant.table.table
                        )))
                    }
                    Some(fields) if fields.is_subset(&policy.blob_fields) => fields.clone(),
                    Some(fields) => {
                        let invalid = fields
                            .difference(&policy.blob_fields)
                            .cloned()
                            .collect::<Vec<_>>();
                        return Err(AuthConfigError(format!(
                            "principal '{}' grants disallowed BLOB fields {:?} for {}.{}",
                            config.name, invalid, grant.table.database, grant.table.table
                        )));
                    }
                };
                if grants.insert(grant.table.clone(), allowed).is_some() {
                    return Err(AuthConfigError(format!(
                        "principal '{}' has duplicate grants for {}.{}",
                        config.name, grant.table.database, grant.table.table
                    )));
                }
            }
            credentials.push(Credential {
                token_hash,
                principal: Principal {
                    name: config.name.clone(),
                    grants: Some(grants),
                },
            });
        }
        Ok(Self::Credentials(credentials))
    }

    pub(crate) fn authenticate(&self, token: Option<&str>) -> Option<Principal> {
        match self {
            Self::Anonymous(principal) => Some(principal.clone()),
            Self::Credentials(credentials) => {
                let candidate = hash_token(token?);
                let mut matched = None;
                for credential in credentials {
                    if bool::from(candidate.ct_eq(&credential.token_hash)) {
                        matched = Some(credential.principal.clone());
                    }
                }
                matched
            }
        }
    }
}

fn resolve_token(
    inline: Option<&str>,
    environment: Option<&str>,
    owner: &str,
) -> Result<Option<String>, AuthConfigError> {
    match (inline, environment) {
        (Some(_), Some(_)) => Err(AuthConfigError(format!(
            "{owner} must configure only one of bearerToken and bearerTokenEnv"
        ))),
        (Some(token), None) => Ok(Some(token.to_string())),
        (None, Some(variable)) if variable.trim().is_empty() => Err(AuthConfigError(format!(
            "{owner} bearerTokenEnv must not be empty"
        ))),
        (None, Some(variable)) => std::env::var(variable).map(Some).map_err(|error| {
            AuthConfigError(format!(
                "failed to read bearer token for {owner} from environment variable '{variable}': {error}"
            ))
        }),
        (None, None) => Ok(None),
    }
}

fn validate_token(token: &str, owner: &str) -> Result<(), AuthConfigError> {
    if token.len() < MIN_TOKEN_BYTES {
        return Err(AuthConfigError(format!(
            "{owner} bearer token must contain at least {MIN_TOKEN_BYTES} bytes"
        )));
    }
    Ok(())
}

fn hash_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

#[derive(Debug)]
pub(crate) struct AuthConfigError(String);

impl std::fmt::Display for AuthConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid authentication configuration: {}", self.0)
    }
}

impl std::error::Error for AuthConfigError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use paimon_query_service::{LookupStrategy, QueryBudget};

    use super::*;

    fn policy() -> TableLookupPolicy {
        TableLookupPolicy {
            table: TableRef::new("db", "assets"),
            key_fields: vec!["id".to_string()],
            blob_fields: BTreeSet::from(["picture".to_string(), "thumbnail".to_string()]),
            strategy: LookupStrategy::GlobalBtree,
            budget: QueryBudget::default(),
        }
    }

    #[test]
    fn principal_is_limited_to_granted_blob_fields() {
        let auth = AuthPolicy::build(
            false,
            None,
            None,
            &[PrincipalConfig {
                name: "image-reader".to_string(),
                bearer_token: Some("long-test-token-1234".to_string()),
                bearer_token_env: None,
                grants: vec![TableGrant {
                    table: TableRef::new("db", "assets"),
                    blob_fields: Some(BTreeSet::from(["picture".to_string()])),
                }],
            }],
            &[policy()],
        )
        .unwrap();

        let principal = auth.authenticate(Some("long-test-token-1234")).unwrap();
        assert_eq!(principal.name(), "image-reader");
        assert!(principal.allows(&TableRef::new("db", "assets"), &["picture".to_string()]));
        assert!(!principal.allows(&TableRef::new("db", "assets"), &["thumbnail".to_string()]));
        assert!(auth.authenticate(Some("invalid-test-token")).is_none());
    }

    #[test]
    fn rejects_unknown_tables_and_duplicate_tokens() {
        let unknown = match AuthPolicy::build(
            false,
            None,
            None,
            &[PrincipalConfig {
                name: "reader".to_string(),
                bearer_token: Some("long-test-token-1234".to_string()),
                bearer_token_env: None,
                grants: vec![TableGrant {
                    table: TableRef::new("db", "missing"),
                    blob_fields: None,
                }],
            }],
            &[policy()],
        ) {
            Ok(_) => panic!("unknown grant table must be rejected"),
            Err(error) => error,
        };
        assert!(unknown.to_string().contains("unknown table"));

        let duplicate = match AuthPolicy::build(
            false,
            None,
            None,
            &[
                PrincipalConfig {
                    name: "one".to_string(),
                    bearer_token: Some("long-test-token-1234".to_string()),
                    bearer_token_env: None,
                    grants: vec![TableGrant {
                        table: TableRef::new("db", "assets"),
                        blob_fields: None,
                    }],
                },
                PrincipalConfig {
                    name: "two".to_string(),
                    bearer_token: Some("long-test-token-1234".to_string()),
                    bearer_token_env: None,
                    grants: vec![TableGrant {
                        table: TableRef::new("db", "assets"),
                        blob_fields: None,
                    }],
                },
            ],
            &[policy()],
        ) {
            Ok(_) => panic!("duplicate bearer tokens must be rejected"),
            Err(error) => error,
        };
        assert!(duplicate.to_string().contains("unique"));
    }

    #[test]
    fn anonymous_access_requires_explicit_opt_in() {
        let error = match AuthPolicy::build(false, None, None, &[], &[policy()]) {
            Ok(_) => panic!("implicit anonymous access must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("allowAnonymous"));

        let auth = AuthPolicy::build(true, None, None, &[], &[policy()]).unwrap();
        assert_eq!(auth.authenticate(None).unwrap().name(), "anonymous");
    }
}
