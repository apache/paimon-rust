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

//! DLF Request Signer implementations for Alibaba Cloud Data Lake Formation.
//!
//! This module provides two signature algorithms for authenticating requests
//! to Alibaba Cloud Data Lake Formation (DLF) service:
//!
//! # Signature Algorithms
//!
//! ## 1. DLF4-HMAC-SHA256 (Default Signer)
//!
//! Used for VPC endpoints (e.g., `*-vpc.dlf.aliyuncs.com`).
//!
//! **Algorithm Overview:**
//! 1. Build a canonical request string from HTTP method, path, query params, and headers
//! 2. Create a string-to-sign with algorithm, timestamp, credential scope, and hashed canonical request
//! 3. Derive a signing key through multiple HMAC-SHA256 operations
//! 4. Calculate the signature and construct the Authorization header
//!
//! **Signing Key Derivation:**
//! ```text
//! kSecret = "aliyun_v4" + AccessKeySecret
//! kDate = HMAC-SHA256(kSecret, Date)
//! kRegion = HMAC-SHA256(kDate, Region)
//! kService = HMAC-SHA256(kRegion, "DlfNext")
//! kSigning = HMAC-SHA256(kService, "aliyun_v4_request")
//! ```
//!
//! **Authorization Header Format:**
//! ```text
//! DLF4-HMAC-SHA256 Credential=AccessKeyId/Date/Region/DlfNext/aliyun_v4_request,Signature=hex_signature
//! ```
//!
//! ## 2. HMAC-SHA1 (OpenAPI Signer)
//!
//! Used for public network endpoints (e.g., `dlfnext.*.aliyuncs.com`).
//! Follows Alibaba Cloud ROA v2 signature style.
//!
//! **Algorithm Overview:**
//! 1. Build canonicalized headers from x-acs-* headers
//! 2. Build canonicalized resource from path and query params
//! 3. Create string-to-sign with method, headers, and resource
//! 4. Calculate signature using HMAC-SHA1
//!
//! **Authorization Header Format:**
//! ```text
//! acs AccessKeyId:base64_signature
//! ```
//!
//! # References
//!
//! - [Alibaba Cloud API Signature](https://help.aliyun.com/document_detail/315526.html)
//! - [DLF OpenAPI](https://help.aliyun.com/document_detail/197826.html)
//!
//! # Usage
//!
//! The signer is automatically selected based on the endpoint URI:
//! - VPC endpoints → `DLFDefaultSigner` (DLF4-HMAC-SHA256)
//! - Public endpoints with "dlfnext" or "openapi" → `DLFOpenApiSigner` (HMAC-SHA1)

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::base::RESTAuthParameter;
use super::dlf_provider::DLFToken;

type HmacSha256 = Hmac<Sha256>;
type HmacSha1 = Hmac<Sha1>;

/// Trait for DLF request signers.
///
/// Different signers implement different signature algorithms for
/// authenticating requests to Alibaba Cloud DLF service.
///
/// # Implementations
///
/// - [`DLFDefaultSigner`]: Uses DLF4-HMAC-SHA256 for VPC endpoints
/// - [`DLFOpenApiSigner`]: Uses HMAC-SHA1 for public endpoints
pub trait DLFRequestSigner: Send + Sync {
    /// Generate signature headers for the request.
    fn sign_headers(
        &self,
        body: Option<&str>,
        now: &DateTime<Utc>,
        security_token: Option<&str>,
        host: &str,
    ) -> HashMap<String, String>;

    /// Generate the Authorization header value.
    fn authorization(
        &self,
        rest_auth_parameter: &RESTAuthParameter,
        token: &DLFToken,
        host: &str,
        sign_headers: &HashMap<String, String>,
    ) -> String;
    #[allow(dead_code)]
    /// Get the identifier for this signer.
    fn identifier(&self) -> &str;
}

/// Default DLF signer using DLF4-HMAC-SHA256 algorithm.
///
/// This signer is used for VPC endpoints (e.g., `cn-hangzhou-vpc.dlf.aliyuncs.com`).
///
/// # Algorithm Details
///
/// The DLF4-HMAC-SHA256 algorithm is similar to AWS Signature Version 4:
///
/// 1. **Canonical Request**: Combine HTTP method, URI, query string, headers, and payload hash
/// 2. **String-to-Sign**: Include algorithm, timestamp, credential scope, and canonical request hash
/// 3. **Signing Key**: Derived through chained HMAC operations
/// 4. **Signature**: HMAC-SHA256 of string-to-sign using the signing key
///
/// # Required Headers
///
/// The following headers are included in the signature calculation:
/// - `content-md5`: MD5 hash of request body (if present)
/// - `content-type`: Media type (if body present)
/// - `x-dlf-content-sha256`: Always "UNSIGNED-PAYLOAD"
/// - `x-dlf-date`: Request timestamp in format `%Y%m%dT%H%M%SZ`
/// - `x-dlf-version`: API version ("v1")
/// - `x-dlf-security-token`: Security token for temporary credentials (optional)
///
/// # Example
///
/// ```ignore
/// use paimon::api::auth::{DLFDefaultSigner, DLFRequestSigner};
///
/// let signer = DLFDefaultSigner::new("cn-hangzhou");
/// let headers = signer.sign_headers(Some(r#"{"key":"value"}"#), &Utc::now(), None, "dlf.aliyuncs.com");
/// ```
pub struct DLFDefaultSigner {
    region: String,
}

impl DLFDefaultSigner {
    pub const IDENTIFIER: &'static str = "default";
    const VERSION: &'static str = "v1";
    const SIGNATURE_ALGORITHM: &'static str = "DLF4-HMAC-SHA256";
    const PRODUCT: &'static str = "DlfNext";
    const REQUEST_TYPE: &'static str = "aliyun_v4_request";
    const SIGNATURE_KEY: &'static str = "Signature";
    const NEW_LINE: &'static str = "\n";

    // Header keys
    const DLF_CONTENT_MD5_HEADER_KEY: &'static str = "Content-MD5";
    const DLF_CONTENT_TYPE_KEY: &'static str = "Content-Type";
    const DLF_DATE_HEADER_KEY: &'static str = "x-dlf-date";
    const DLF_SECURITY_TOKEN_HEADER_KEY: &'static str = "x-dlf-security-token";
    const DLF_AUTH_VERSION_HEADER_KEY: &'static str = "x-dlf-version";
    const DLF_CONTENT_SHA256_HEADER_KEY: &'static str = "x-dlf-content-sha256";
    const DLF_CONTENT_SHA256_VALUE: &'static str = "UNSIGNED-PAYLOAD";

    const AUTH_DATE_TIME_FORMAT: &'static str = "%Y%m%dT%H%M%SZ";
    const MEDIA_TYPE: &'static str = "application/json";

    const SIGNED_HEADERS: &'static [&'static str] = &[
        "content-md5",
        "content-type",
        "x-dlf-content-sha256",
        "x-dlf-date",
        "x-dlf-version",
        "x-dlf-security-token",
    ];

    /// Create a new DLFDefaultSigner with the given region.
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
        }
    }

    fn md5_base64(raw: &str) -> String {
        let mut hasher = Md5::new();
        hasher.update(raw.as_bytes());
        let hash = hasher.finalize();
        BASE64_STANDARD.encode(hash)
    }

    fn hmac_sha256(key: &[u8], data: &str) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
        mac.update(data.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }

    fn sha256_hex(raw: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(raw.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn hex_encode(raw: &[u8]) -> String {
        hex::encode(raw)
    }

    fn trim(value: &str) -> &str {
        value.trim()
    }

    fn get_canonical_request(
        &self,
        rest_auth_parameter: &RESTAuthParameter,
        headers: &HashMap<String, String>,
    ) -> String {
        let mut parts = vec![
            rest_auth_parameter.method.clone(),
            rest_auth_parameter.path.clone(),
        ];

        let canonical_query_string =
            self.build_canonical_query_string(&rest_auth_parameter.parameters);
        parts.push(canonical_query_string);

        let sorted_headers = self.build_sorted_signed_headers_map(headers);
        for (key, value) in sorted_headers {
            parts.push(format!("{}:{}", key, value));
        }

        let content_sha256 = headers
            .get(Self::DLF_CONTENT_SHA256_HEADER_KEY)
            .map(|s| s.as_str())
            .unwrap_or(Self::DLF_CONTENT_SHA256_VALUE);
        parts.push(content_sha256.to_string());

        parts.join(Self::NEW_LINE)
    }

    fn build_canonical_query_string(&self, parameters: &HashMap<String, String>) -> String {
        if parameters.is_empty() {
            return String::new();
        }

        let mut sorted_params: Vec<_> = parameters.iter().collect();
        sorted_params.sort_by(|a, b| a.0.cmp(b.0));

        let query_parts: Vec<String> = sorted_params
            .iter()
            .map(|(key, value)| {
                let key = Self::trim(key);
                if !value.is_empty() {
                    let value = Self::trim(value);
                    format!("{}={}", key, value)
                } else {
                    key.to_string()
                }
            })
            .collect();

        query_parts.join("&")
    }

    fn build_sorted_signed_headers_map(
        &self,
        headers: &HashMap<String, String>,
    ) -> Vec<(String, String)> {
        let mut sorted_headers: Vec<(String, String)> = headers
            .iter()
            .filter(|(key, _)| {
                let lower_key = key.to_lowercase();
                Self::SIGNED_HEADERS.contains(&lower_key.as_str())
            })
            .map(|(key, value)| (key.to_lowercase(), Self::trim(value).to_string()))
            .collect();

        sorted_headers.sort_by(|a, b| a.0.cmp(&b.0));
        sorted_headers
    }
}

impl DLFRequestSigner for DLFDefaultSigner {
    fn sign_headers(
        &self,
        body: Option<&str>,
        now: &DateTime<Utc>,
        security_token: Option<&str>,
        _host: &str,
    ) -> HashMap<String, String> {
        let mut sign_headers = HashMap::new();

        let date_time = now.format(Self::AUTH_DATE_TIME_FORMAT).to_string();
        sign_headers.insert(Self::DLF_DATE_HEADER_KEY.to_string(), date_time);
        sign_headers.insert(
            Self::DLF_CONTENT_SHA256_HEADER_KEY.to_string(),
            Self::DLF_CONTENT_SHA256_VALUE.to_string(),
        );
        sign_headers.insert(
            Self::DLF_AUTH_VERSION_HEADER_KEY.to_string(),
            Self::VERSION.to_string(),
        );

        if let Some(body_content) = body {
            if !body_content.is_empty() {
                sign_headers.insert(
                    Self::DLF_CONTENT_TYPE_KEY.to_string(),
                    Self::MEDIA_TYPE.to_string(),
                );
                sign_headers.insert(
                    Self::DLF_CONTENT_MD5_HEADER_KEY.to_string(),
                    Self::md5_base64(body_content),
                );
            }
        }

        if let Some(token) = security_token {
            sign_headers.insert(
                Self::DLF_SECURITY_TOKEN_HEADER_KEY.to_string(),
                token.to_string(),
            );
        }

        sign_headers
    }

    fn authorization(
        &self,
        rest_auth_parameter: &RESTAuthParameter,
        token: &DLFToken,
        _host: &str,
        sign_headers: &HashMap<String, String>,
    ) -> String {
        let date_time = sign_headers.get(Self::DLF_DATE_HEADER_KEY).unwrap();
        let date = &date_time[..8];

        let canonical_request = self.get_canonical_request(rest_auth_parameter, sign_headers);

        let string_to_sign = [
            Self::SIGNATURE_ALGORITHM.to_string(),
            date_time.clone(),
            format!(
                "{}/{}/{}/{}",
                date,
                self.region,
                Self::PRODUCT,
                Self::REQUEST_TYPE
            ),
            Self::sha256_hex(&canonical_request),
        ]
        .join(Self::NEW_LINE);

        // Derive signing key
        let date_key = Self::hmac_sha256(
            format!("aliyun_v4{}", token.access_key_secret).as_bytes(),
            date,
        );
        let date_region_key = Self::hmac_sha256(&date_key, &self.region);
        let date_region_service_key = Self::hmac_sha256(&date_region_key, Self::PRODUCT);
        let signing_key = Self::hmac_sha256(&date_region_service_key, Self::REQUEST_TYPE);

        let signature_bytes = Self::hmac_sha256(&signing_key, &string_to_sign);
        let signature = Self::hex_encode(&signature_bytes);

        format!(
            "{} Credential={}/{}/{}/{}/{},{}={}",
            Self::SIGNATURE_ALGORITHM,
            token.access_key_id,
            date,
            self.region,
            Self::PRODUCT,
            Self::REQUEST_TYPE,
            Self::SIGNATURE_KEY,
            signature
        )
    }

    fn identifier(&self) -> &str {
        Self::IDENTIFIER
    }
}

/// DLF OpenAPI signer using HMAC-SHA1 algorithm.
///
/// This signer follows the Alibaba Cloud ROA v2 signature style and is used
/// for public network endpoints (e.g., `dlfnext.cn-asdnbhwf.aliyuncs.com`).
///
/// # Algorithm Details
///
/// The HMAC-SHA1 algorithm is the traditional Alibaba Cloud API signature method:
///
/// 1. **Canonicalized Headers**: Sort and format all `x-acs-*` headers
/// 2. **Canonicalized Resource**: URL-decoded path with sorted query parameters
/// 3. **String-to-Sign**: Combine HTTP method, standard headers, canonicalized headers, and resource
/// 4. **Signature**: Base64-encoded HMAC-SHA1 of string-to-sign
///
/// # Required Headers
///
/// The following headers are included in requests:
/// - `Date`: Request timestamp in RFC 1123 format
/// - `Accept`: Always "application/json"
/// - `Content-MD5`: MD5 hash of request body (if present)
/// - `Content-Type`: Always "application/json" (if body present)
/// - `Host`: Endpoint host
/// - `x-acs-signature-method`: Always "HMAC-SHA1"
/// - `x-acs-signature-nonce`: Unique UUID for each request
/// - `x-acs-signature-version`: Always "1.0"
/// - `x-acs-version`: API version ("2026-01-18")
/// - `x-acs-security-token`: Security token for temporary credentials (optional)
///
/// # Example
///
/// ```ignore
/// use paimon::api::auth::{DLFOpenApiSigner, DLFRequestSigner};
///
/// let signer = DLFOpenApiSigner;
/// let headers = signer.sign_headers(Some(r#"{"key":"value"}"#), &Utc::now(), None, "dlfnext.aliyuncs.com");
/// ```
pub struct DLFOpenApiSigner;

impl DLFOpenApiSigner {
    pub const IDENTIFIER: &'static str = "openapi";

    // Header constants
    const DATE_HEADER: &'static str = "Date";
    const ACCEPT_HEADER: &'static str = "Accept";
    const CONTENT_MD5_HEADER: &'static str = "Content-MD5";
    const CONTENT_TYPE_HEADER: &'static str = "Content-Type";
    const HOST_HEADER: &'static str = "Host";
    const X_ACS_SIGNATURE_METHOD: &'static str = "x-acs-signature-method";
    const X_ACS_SIGNATURE_NONCE: &'static str = "x-acs-signature-nonce";
    const X_ACS_SIGNATURE_VERSION: &'static str = "x-acs-signature-version";
    const X_ACS_VERSION: &'static str = "x-acs-version";
    const X_ACS_SECURITY_TOKEN: &'static str = "x-acs-security-token";

    // Values
    const DATE_FORMAT: &'static str = "%a, %d %b %Y %H:%M:%S GMT";
    const ACCEPT_VALUE: &'static str = "application/json";
    const CONTENT_TYPE_VALUE: &'static str = "application/json";
    const SIGNATURE_METHOD_VALUE: &'static str = "HMAC-SHA1";
    const SIGNATURE_VERSION_VALUE: &'static str = "1.0";
    const API_VERSION: &'static str = "2026-01-18";

    fn md5_base64(data: &str) -> String {
        let mut hasher = Md5::new();
        hasher.update(data.as_bytes());
        let hash = hasher.finalize();
        BASE64_STANDARD.encode(hash)
    }

    fn hmac_sha1_base64(key: &str, data: &str) -> String {
        let mut mac =
            HmacSha1::new_from_slice(key.as_bytes()).expect("HMAC can take key of any size");
        mac.update(data.as_bytes());
        BASE64_STANDARD.encode(mac.finalize().into_bytes())
    }

    fn trim(value: &str) -> &str {
        value.trim()
    }

    fn build_canonicalized_headers(&self, headers: &HashMap<String, String>) -> String {
        let mut sorted_headers: Vec<(String, String)> = headers
            .iter()
            .filter(|(key, _)| key.to_lowercase().starts_with("x-acs-"))
            .map(|(key, value)| (key.to_lowercase(), Self::trim(value).to_string()))
            .collect();

        sorted_headers.sort_by(|a, b| a.0.cmp(&b.0));

        let mut result = String::new();
        for (key, value) in sorted_headers {
            result.push_str(&format!("{}:{}\n", key, value));
        }
        result
    }

    fn build_canonicalized_resource(&self, rest_auth_parameter: &RESTAuthParameter) -> String {
        let path = urlencoding::decode(&rest_auth_parameter.path).unwrap_or_default();

        if rest_auth_parameter.parameters.is_empty() {
            return path.to_string();
        }

        let mut sorted_params: Vec<_> = rest_auth_parameter.parameters.iter().collect();
        sorted_params.sort_by(|a, b| a.0.cmp(b.0));

        let query_parts: Vec<String> = sorted_params
            .iter()
            .map(|(key, value)| {
                let decoded_value = urlencoding::decode(value).unwrap_or_default();
                if !decoded_value.is_empty() {
                    format!("{}={}", key, decoded_value)
                } else {
                    key.to_string()
                }
            })
            .collect();

        format!("{}?{}", path, query_parts.join("&"))
    }

    fn build_string_to_sign(
        &self,
        rest_auth_parameter: &RESTAuthParameter,
        headers: &HashMap<String, String>,
        canonicalized_headers: &str,
        canonicalized_resource: &str,
    ) -> String {
        let parts = [
            rest_auth_parameter.method.clone(),
            headers
                .get(Self::ACCEPT_HEADER)
                .cloned()
                .unwrap_or_default(),
            headers
                .get(Self::CONTENT_MD5_HEADER)
                .cloned()
                .unwrap_or_default(),
            headers
                .get(Self::CONTENT_TYPE_HEADER)
                .cloned()
                .unwrap_or_default(),
            headers.get(Self::DATE_HEADER).cloned().unwrap_or_default(),
            canonicalized_headers.to_string(),
        ];

        parts.join("\n") + canonicalized_resource
    }
}

impl DLFRequestSigner for DLFOpenApiSigner {
    fn sign_headers(
        &self,
        body: Option<&str>,
        now: &DateTime<Utc>,
        security_token: Option<&str>,
        host: &str,
    ) -> HashMap<String, String> {
        let mut headers = HashMap::new();

        // Date header in RFC 1123 format
        headers.insert(
            Self::DATE_HEADER.to_string(),
            now.format(Self::DATE_FORMAT).to_string(),
        );

        // Accept header
        headers.insert(
            Self::ACCEPT_HEADER.to_string(),
            Self::ACCEPT_VALUE.to_string(),
        );

        // Content-MD5 and Content-Type (if body exists)
        if let Some(body_content) = body {
            if !body_content.is_empty() {
                headers.insert(
                    Self::CONTENT_MD5_HEADER.to_string(),
                    Self::md5_base64(body_content),
                );
                headers.insert(
                    Self::CONTENT_TYPE_HEADER.to_string(),
                    Self::CONTENT_TYPE_VALUE.to_string(),
                );
            }
        }

        // Host header
        headers.insert(Self::HOST_HEADER.to_string(), host.to_string());

        // x-acs-* headers
        headers.insert(
            Self::X_ACS_SIGNATURE_METHOD.to_string(),
            Self::SIGNATURE_METHOD_VALUE.to_string(),
        );
        headers.insert(
            Self::X_ACS_SIGNATURE_NONCE.to_string(),
            Uuid::new_v4().to_string(),
        );
        headers.insert(
            Self::X_ACS_SIGNATURE_VERSION.to_string(),
            Self::SIGNATURE_VERSION_VALUE.to_string(),
        );
        headers.insert(
            Self::X_ACS_VERSION.to_string(),
            Self::API_VERSION.to_string(),
        );

        // Security token (if present)
        if let Some(token) = security_token {
            headers.insert(Self::X_ACS_SECURITY_TOKEN.to_string(), token.to_string());
        }

        headers
    }

    fn authorization(
        &self,
        rest_auth_parameter: &RESTAuthParameter,
        token: &DLFToken,
        _host: &str,
        sign_headers: &HashMap<String, String>,
    ) -> String {
        let canonicalized_headers = self.build_canonicalized_headers(sign_headers);
        let canonicalized_resource = self.build_canonicalized_resource(rest_auth_parameter);
        let string_to_sign = self.build_string_to_sign(
            rest_auth_parameter,
            sign_headers,
            &canonicalized_headers,
            &canonicalized_resource,
        );

        let signature = Self::hmac_sha1_base64(&token.access_key_secret, &string_to_sign);
        format!("acs {}:{}", token.access_key_id, signature)
    }

    fn identifier(&self) -> &str {
        Self::IDENTIFIER
    }
}

/// use paimon::api::auth::DLFSignerFactory;
///
/// // Auto-detect from URI
/// let signer = DLFSignerFactory::create_signer("default", "cn-hangzhou");
/// let algo = DLFSignerFactory::parse_signing_algo_from_uri(Some("http://dlfnext.ajinnbjug.aliyuncs.com"));
/// assert_eq!(algo, "openapi");
/// ```
pub struct DLFSignerFactory;

impl DLFSignerFactory {
    /// Create a signer based on the signing algorithm.
    pub fn create_signer(signing_algorithm: &str, region: &str) -> Box<dyn DLFRequestSigner> {
        if signing_algorithm == DLFOpenApiSigner::IDENTIFIER {
            Box::new(DLFOpenApiSigner)
        } else {
            Box::new(DLFDefaultSigner::new(region))
        }
    }
}
