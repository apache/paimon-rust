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

//! Paimon's unified User-Agent for REST requests:
//! `<module>(<transport>[;feature...])[ <extended>]`.

use crate::common::Options;

/// Replaces the default `paimon-rust/<version>` module; shared with object storage requests.
pub(crate) const USER_AGENT_MODULE: &str = "user-agent.module";

/// Space-separated features, rendered after the transport and separated by `;`.
pub(crate) const USER_AGENT_FEATURES: &str = "user-agent.features";

/// Free text after the parentheses.
pub(crate) const USER_AGENT_EXTENDED: &str = "user-agent.extended";

/// The User-Agent of REST requests without options, `paimon-rust/<version>(reqwest)`.
pub(crate) fn default_rest_user_agent() -> String {
    rest_user_agent(&Options::new())
}

/// Builds the REST User-Agent from the `user-agent.*` catalog options.
pub(crate) fn rest_user_agent(options: &Options) -> String {
    let non_blank = |key: &str| {
        options
            .get(key)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    };

    let mut user_agent = match non_blank(USER_AGENT_MODULE) {
        Some(module) => module.to_string(),
        None => format!("paimon-rust/{}", env!("CARGO_PKG_VERSION")),
    };
    user_agent.push_str("(reqwest");
    for feature in non_blank(USER_AGENT_FEATURES)
        .into_iter()
        .flat_map(|features| features.split_whitespace())
    {
        user_agent.push(';');
        user_agent.push_str(feature);
    }
    user_agent.push(')');
    if let Some(extended) = non_blank(USER_AGENT_EXTENDED) {
        user_agent.push(' ');
        user_agent.push_str(extended);
    }
    user_agent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rest_user_agent() {
        let version = env!("CARGO_PKG_VERSION");
        assert_eq!(
            default_rest_user_agent(),
            format!("paimon-rust/{version}(reqwest)")
        );

        let mut options = Options::new();
        options.set(USER_AGENT_FEATURES, " Flink  PVFS ");
        options.set(USER_AGENT_EXTENDED, " vvr ");
        assert_eq!(
            rest_user_agent(&options),
            format!("paimon-rust/{version}(reqwest;Flink;PVFS) vvr")
        );

        options.set(USER_AGENT_MODULE, "MyApp/1.0");
        options.set(USER_AGENT_EXTENDED, " ");
        assert_eq!(rest_user_agent(&options), "MyApp/1.0(reqwest;Flink;PVFS)");
    }
}
