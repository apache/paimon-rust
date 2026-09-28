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

//! Paimon's unified User-Agent for object storage requests:
//! `<module>(<transport>[;feature...])[ <extended>]`.

use std::collections::HashMap;

use http::header::{HeaderValue, USER_AGENT};
use http::{Request, Response};
use opendal::{Buffer, HttpBody, HttpTransport};
use opendal_http_transport_reqwest::ReqwestTransport;

/// Replaces the default `paimon-rust/<version>` module; shared with REST requests.
pub(crate) const USER_AGENT_MODULE: &str = "user-agent.module";

/// Space-separated features, rendered after the transport and separated by `;`.
pub(crate) const USER_AGENT_FEATURES: &str = "user-agent.features";

/// Free text after the parentheses.
pub(crate) const USER_AGENT_EXTENDED: &str = "user-agent.extended";

/// OSS-only keys, each overriding the matching `user-agent.*` key.
pub(crate) const OSS_USER_AGENT_MODULE: &str = "fs.oss.user.agent.module";
pub(crate) const OSS_USER_AGENT_FEATURES: &str = "fs.oss.user.agent.features";
pub(crate) const OSS_USER_AGENT_EXTENDED: &str = "fs.oss.user.agent.extended";

/// Set by the DLF data token and appended after the extended part.
pub(crate) const DLF_ACCESS_TRACKING_EXTENDED_INFO: &str = "dlf.access-tracking.extended-info";

/// The User-Agent sent when no `user-agent.*` option is set.
pub(crate) fn default_user_agent() -> String {
    storage_user_agent(&HashMap::new())
}

/// Builds the User-Agent from the `user-agent.*` options.
pub(crate) fn storage_user_agent(props: &HashMap<String, String>) -> String {
    build_user_agent(props, None)
}

/// Builds the OSS User-Agent, where `fs.oss.user.agent.*` overrides `user-agent.*` per part.
pub(crate) fn oss_user_agent(props: &HashMap<String, String>) -> String {
    build_user_agent(
        props,
        Some([
            OSS_USER_AGENT_MODULE,
            OSS_USER_AGENT_FEATURES,
            OSS_USER_AGENT_EXTENDED,
        ]),
    )
}

fn build_user_agent(props: &HashMap<String, String>, overrides: Option<[&str; 3]>) -> String {
    let non_blank = |key: &str| {
        props
            .get(key)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    };
    let part = |index: usize, key: &str| {
        overrides
            .and_then(|keys| non_blank(keys[index]))
            .or_else(|| non_blank(key))
    };

    let mut user_agent = match part(0, USER_AGENT_MODULE) {
        Some(module) => module.to_string(),
        None => format!("paimon-rust/{}", env!("CARGO_PKG_VERSION")),
    };
    user_agent.push_str("(opendal/");
    user_agent.push_str(opendal::raw::VERSION);
    for feature in part(1, USER_AGENT_FEATURES)
        .into_iter()
        .flat_map(|features| features.split_whitespace())
    {
        user_agent.push(';');
        user_agent.push_str(feature);
    }
    user_agent.push(')');

    let extended: Vec<&str> = [
        part(2, USER_AGENT_EXTENDED),
        non_blank(DLF_ACCESS_TRACKING_EXTENDED_INFO),
    ]
    .into_iter()
    .flatten()
    .collect();
    if !extended.is_empty() {
        user_agent.push(' ');
        user_agent.push_str(&extended.join(" "));
    }
    user_agent
}

/// Sends every request through the shared reqwest client with a User-Agent, unless one is set.
#[derive(Clone)]
pub(crate) struct UserAgentTransport {
    user_agent: HeaderValue,
    inner: ReqwestTransport,
}

impl UserAgentTransport {
    pub(crate) fn new(user_agent: &str) -> Self {
        let user_agent = HeaderValue::from_bytes(user_agent.as_bytes()).unwrap_or_else(|_| {
            log::warn!("Invalid object storage User-Agent {user_agent:?}, using the default");
            HeaderValue::from_str(&default_user_agent())
                .expect("the default User-Agent is visible ASCII")
        });
        Self {
            user_agent,
            inner: ReqwestTransport::default(),
        }
    }

    fn apply(&self, req: &mut Request<Buffer>) {
        req.headers_mut()
            .entry(USER_AGENT)
            .or_insert_with(|| self.user_agent.clone());
    }
}

impl HttpTransport for UserAgentTransport {
    async fn fetch(&self, mut req: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        self.apply(&mut req);
        self.inner.fetch(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn default_prefix() -> String {
        format!(
            "paimon-rust/{}(opendal/{}",
            env!("CARGO_PKG_VERSION"),
            opendal::raw::VERSION
        )
    }

    #[test]
    fn test_default_user_agent() {
        assert_eq!(default_user_agent(), format!("{})", default_prefix()));
    }

    #[test]
    fn test_module_override() {
        let user_agent = oss_user_agent(&props(&[(OSS_USER_AGENT_MODULE, "MyApp/1.0")]));
        assert_eq!(
            user_agent,
            format!("MyApp/1.0(opendal/{})", opendal::raw::VERSION)
        );
    }

    #[test]
    fn test_features_are_split_and_joined() {
        let user_agent = oss_user_agent(&props(&[(OSS_USER_AGENT_FEATURES, " Flink  Paimon ")]));
        assert_eq!(user_agent, format!("{};Flink;Paimon)", default_prefix()));
    }

    #[test]
    fn test_access_tracking_is_appended_to_user_extended() {
        let user_agent = oss_user_agent(&props(&[
            (OSS_USER_AGENT_EXTENDED, "vvr"),
            (DLF_ACCESS_TRACKING_EXTENDED_INFO, "uid/123 user/alice"),
        ]));
        assert_eq!(
            user_agent,
            format!("{}) vvr uid/123 user/alice", default_prefix())
        );
    }

    #[test]
    fn test_access_tracking_only() {
        let user_agent = oss_user_agent(&props(&[(
            DLF_ACCESS_TRACKING_EXTENDED_INFO,
            "uid/123 user/alice",
        )]));
        assert_eq!(
            user_agent,
            format!("{}) uid/123 user/alice", default_prefix())
        );
    }

    #[test]
    fn test_blank_options_are_ignored() {
        let user_agent = oss_user_agent(&props(&[
            (OSS_USER_AGENT_MODULE, " "),
            (OSS_USER_AGENT_FEATURES, "  "),
            (OSS_USER_AGENT_EXTENDED, ""),
            (USER_AGENT_MODULE, " "),
            (USER_AGENT_FEATURES, ""),
            (USER_AGENT_EXTENDED, " "),
            (DLF_ACCESS_TRACKING_EXTENDED_INFO, " "),
        ]));
        assert_eq!(user_agent, default_user_agent());
    }

    #[test]
    fn test_generic_keys_apply_to_any_backend() {
        let user_agent = storage_user_agent(&props(&[
            (USER_AGENT_MODULE, "MyApp/1.0"),
            (USER_AGENT_FEATURES, "Flink"),
            (USER_AGENT_EXTENDED, "vvr"),
            (DLF_ACCESS_TRACKING_EXTENDED_INFO, "uid/123"),
            (OSS_USER_AGENT_EXTENDED, "ignored"),
        ]));
        assert_eq!(
            user_agent,
            format!(
                "MyApp/1.0(opendal/{};Flink) vvr uid/123",
                opendal::raw::VERSION
            )
        );
    }

    #[test]
    fn test_oss_falls_back_to_generic_keys() {
        let user_agent = oss_user_agent(&props(&[
            (USER_AGENT_FEATURES, "Flink"),
            (USER_AGENT_EXTENDED, "vvr"),
            (DLF_ACCESS_TRACKING_EXTENDED_INFO, "uid/123"),
        ]));
        assert_eq!(
            user_agent,
            format!("{};Flink) vvr uid/123", default_prefix())
        );
    }

    #[test]
    fn test_oss_keys_override_generic_keys_per_part() {
        let user_agent = oss_user_agent(&props(&[
            (USER_AGENT_MODULE, "Generic/1.0"),
            (USER_AGENT_FEATURES, "Flink"),
            (USER_AGENT_EXTENDED, "vvr"),
            (OSS_USER_AGENT_FEATURES, "Spark"),
            (OSS_USER_AGENT_EXTENDED, "oss/ext"),
            (DLF_ACCESS_TRACKING_EXTENDED_INFO, "uid/123"),
        ]));
        assert_eq!(
            user_agent,
            format!(
                "Generic/1.0(opendal/{};Spark) oss/ext uid/123",
                opendal::raw::VERSION
            )
        );
    }

    #[test]
    fn test_existing_user_agent_is_kept() {
        let transport = UserAgentTransport::new("paimon-rust/test");

        let mut req = Request::new(Buffer::new());
        transport.apply(&mut req);
        assert_eq!(req.headers()[USER_AGENT], "paimon-rust/test");

        let mut req = Request::builder()
            .header(USER_AGENT, "caller/1.0")
            .body(Buffer::new())
            .unwrap();
        transport.apply(&mut req);
        assert_eq!(req.headers()[USER_AGENT], "caller/1.0");
    }

    #[test]
    fn test_invalid_user_agent_falls_back_to_default() {
        let transport = UserAgentTransport::new("bad\nagent");
        assert_eq!(transport.user_agent, default_user_agent().as_str());
    }
}
