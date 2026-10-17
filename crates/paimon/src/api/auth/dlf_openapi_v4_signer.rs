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

//! Java's Aliyun OpenAPI V4 (ACS4-HMAC-SHA256) request signer.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::base::RESTAuthParameter;
use super::dlf_openapi_actions::resolve_action;
use super::dlf_provider::DLFToken;
use super::dlf_signer::{hmac_sha256, sha256_hex, DLFRequestSigner};
use crate::api::rest_util::RESTUtil;

pub(super) struct DLFOpenApiV4Signer {
    region: String,
}

impl DLFOpenApiV4Signer {
    pub const IDENTIFIER: &'static str = "openapi-v4";
    const ALGORITHM: &'static str = "ACS4-HMAC-SHA256";
    const PRODUCT: &'static str = "DlfNext";
    const REQUEST_TYPE: &'static str = "aliyun_v4_request";

    pub(super) fn new(region: &str) -> Self {
        Self {
            region: region.to_string(),
        }
    }

    fn percent_encode(value: &str) -> String {
        RESTUtil::encode_string(value)
            .replace('+', "%20")
            .replace('*', "%2A")
            .replace("%7E", "~")
    }

    fn canonical_query(parameters: &HashMap<String, String>) -> String {
        let sorted: BTreeMap<_, _> = parameters.iter().collect();
        sorted
            .into_iter()
            .map(|(key, value)| {
                // RESTAuthParameter has already encoded each value once, as in Java.
                format!(
                    "{}={}",
                    Self::percent_encode(key),
                    Self::percent_encode(&RESTUtil::decode_string(value))
                )
            })
            .collect::<Vec<_>>()
            .join("&")
    }

    fn canonical_headers(headers: &HashMap<String, String>) -> (String, String) {
        let sorted: BTreeMap<_, _> = headers
            .iter()
            .filter_map(|(key, value)| {
                let key = key.to_ascii_lowercase();
                (key.starts_with("x-acs-") || key == "host" || key == "content-type")
                    .then(|| (key, value.trim()))
            })
            .collect();
        let canonical = sorted
            .iter()
            .map(|(key, value)| format!("{key}:{value}\n"))
            .collect::<String>();
        let signed = sorted.keys().cloned().collect::<Vec<_>>().join(";");
        (canonical, signed)
    }
}

impl DLFRequestSigner for DLFOpenApiV4Signer {
    fn sign_headers(
        &self,
        body: Option<&str>,
        now: &DateTime<Utc>,
        security_token: Option<&str>,
        host: &str,
    ) -> HashMap<String, String> {
        let mut headers = HashMap::from([
            (
                "x-acs-date".to_string(),
                now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            ),
            ("host".to_string(), host.to_string()),
            (
                "x-acs-content-sha256".to_string(),
                sha256_hex(body.unwrap_or_default()),
            ),
            (
                "x-acs-signature-nonce".to_string(),
                Uuid::new_v4().to_string(),
            ),
            ("x-acs-version".to_string(), "2026-01-18".to_string()),
        ]);
        if body.is_some_and(|body| !body.is_empty()) {
            headers.insert("content-type".to_string(), "application/json".to_string());
        }
        if let Some(token) = security_token {
            headers.insert("x-acs-security-token".to_string(), token.to_string());
        }
        headers
    }

    fn authorization(
        &self,
        parameter: &RESTAuthParameter,
        token: &DLFToken,
        _host: &str,
        headers: &mut HashMap<String, String>,
    ) -> String {
        // The action has to be both signed and included in the outgoing headers.
        if let Some(action) = resolve_action(&parameter.method, &parameter.path) {
            headers.insert("x-acs-action".to_string(), action.to_string());
        }
        let (canonical_headers, signed_headers) = Self::canonical_headers(headers);
        let uri = if parameter.path.trim().is_empty() {
            "/"
        } else {
            &parameter.path
        };
        let empty_hash = sha256_hex("");
        let payload = headers.get("x-acs-content-sha256").unwrap_or(&empty_hash);
        let canonical = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            parameter.method,
            uri,
            Self::canonical_query(&parameter.parameters),
            canonical_headers,
            signed_headers,
            payload
        );
        let to_sign = format!("{}\n{}", Self::ALGORITHM, sha256_hex(&canonical));
        let date = headers["x-acs-date"][..10].replace('-', "");
        let date_key = hmac_sha256(
            format!("aliyun_v4{}", token.access_key_secret).as_bytes(),
            &date,
        );
        let region_key = hmac_sha256(&date_key, &self.region);
        let product_key = hmac_sha256(&region_key, Self::PRODUCT);
        let signing_key = hmac_sha256(&product_key, Self::REQUEST_TYPE);
        let signature = hex::encode(hmac_sha256(&signing_key, &to_sign));
        format!(
            "{} Credential={}/{}/{}/{}/{},SignedHeaders={},Signature={}",
            Self::ALGORITHM,
            token.access_key_id,
            date,
            self.region,
            Self::PRODUCT,
            Self::REQUEST_TYPE,
            signed_headers,
            signature
        )
    }

    fn identifier(&self) -> &str {
        Self::IDENTIFIER
    }
}

#[cfg(test)]
mod tests {
    use super::super::dlf_signer::DLFSignerFactory;
    use super::*;

    const HOST: &str = "dlfnext.cn-hangzhou.aliyuncs.com";

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2025-04-16T03:44:46Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn java_known_signatures_cover_encoded_queries_and_actions() {
        let signer = DLFSignerFactory::create_signer("openapi-v4", "cn-hangzhou");
        assert_eq!(signer.identifier(), "openapi-v4");
        let token = DLFToken::new("TestAKId", "TestAKSecret", None, None, None);
        let parameter = RESTAuthParameter::for_get(
            "/v1/prefix/functions",
            HashMap::from([
                ("pageToken".to_string(), "a b/c".to_string()),
                ("functionNamePattern".to_string(), "func%".to_string()),
            ]),
        );
        let mut headers = signer.sign_headers(None, &now(), None, HOST);
        headers.insert(
            "x-acs-signature-nonce".to_string(),
            "fixed-nonce-for-test".to_string(),
        );
        assert_eq!(signer.authorization(&parameter, &token, HOST, &mut headers), concat!(
            "ACS4-HMAC-SHA256 Credential=TestAKId/20250416/cn-hangzhou/DlfNext/aliyun_v4_request,",
            "SignedHeaders=host;x-acs-content-sha256;x-acs-date;x-acs-signature-nonce;x-acs-version,",
            "Signature=9812bacd92dd1152e7ea32aacfad0545835d6a18543852364f1a1ea0cba6219a"));

        let body = r#"{"identifier":{"database":"db","object":"t"}}"#;
        let parameter =
            RESTAuthParameter::for_post("/v1/clg-paimon-1/databases/db/tables", body.to_string());
        let mut headers = signer.sign_headers(Some(body), &now(), None, HOST);
        headers.insert(
            "x-acs-signature-nonce".to_string(),
            "fixed-nonce-for-test".to_string(),
        );
        assert_eq!(
            signer.authorization(&parameter, &token, HOST, &mut headers),
            concat!(
            "ACS4-HMAC-SHA256 Credential=TestAKId/20250416/cn-hangzhou/DlfNext/aliyun_v4_request,",
            "SignedHeaders=content-type;host;x-acs-action;x-acs-content-sha256;",
            "x-acs-date;x-acs-signature-nonce;x-acs-version,",
            "Signature=420206b5263536e6bc271a7a32582b4a820e23c8c58ae692b708c69f9d19e51d")
        );
        assert_eq!(headers["x-acs-action"], "CreateTable");
    }

    #[test]
    fn canonical_query_uses_rfc3986_and_decodes_values_once() {
        let parameter = RESTAuthParameter::for_get(
            "/",
            HashMap::from([
                ("z".to_string(), "".to_string()),
                ("a b*~".to_string(), "a+b /%一".to_string()),
            ]),
        );
        assert_eq!(
            DLFOpenApiV4Signer::canonical_query(&parameter.parameters),
            "a%20b%2A~=a%2Bb%20%2F%25%E4%B8%80&z="
        );
    }

    #[test]
    fn payload_token_and_region_are_covered_by_signature() {
        let signer = DLFOpenApiV4Signer::new("cn-hangzhou");
        let token = DLFToken::new("key", "secret", Some("sts".to_string()), None, None);
        let parameter = RESTAuthParameter::for_get("/test", HashMap::new());
        let mut headers = signer.sign_headers(None, &now(), Some("sts"), HOST);
        assert_eq!(headers["x-acs-date"], "2025-04-16T03:44:46Z");
        assert_eq!(
            headers["x-acs-content-sha256"],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert!(!headers.contains_key("content-type"));
        let original = signer.authorization(&parameter, &token, HOST, &mut headers);
        assert!(original.contains(";x-acs-security-token;"));
        headers.insert("x-acs-security-token".to_string(), "changed".to_string());
        assert_ne!(
            original,
            signer.authorization(&parameter, &token, HOST, &mut headers)
        );
        headers.insert("x-acs-security-token".to_string(), "sts".to_string());
        let elsewhere = DLFOpenApiV4Signer::new("cn-beijing");
        assert_ne!(
            original,
            elsewhere.authorization(&parameter, &token, HOST, &mut headers)
        );
        let body = signer.sign_headers(Some("一"), &now(), None, HOST);
        assert_eq!(body["x-acs-content-sha256"], sha256_hex("一"));
        assert_eq!(body["content-type"], "application/json");
        assert_ne!(
            headers["x-acs-signature-nonce"],
            body["x-acs-signature-nonce"]
        );
    }
}
