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

//! Cookie persistence and response observation for REST transport interoperability.

use std::sync::{Arc, Mutex, Weak};

use reqwest::cookie::Jar;
use reqwest::header::HeaderValue;
use reqwest::Url;

type Updates = Vec<(String, Vec<String>, i64)>;

/// Observes response Set-Cookie headers, their origin URLs and receipt times.
///
/// A language binding can feed these into its owning HTTP session's cookie jar,
/// preserving domain, path, expiry and deletion semantics across transports.
/// Dropping the observer stops collecting updates without retaining credentials.
#[derive(Clone, Default)]
pub struct ResponseCookies(Arc<Mutex<Updates>>);

impl ResponseCookies {
    /// Drain updates as (URL, Set-Cookie headers, Unix seconds received), in
    /// response order. Receipt time preserves Max-Age across delayed handoff.
    pub fn take(&self) -> Vec<(String, Vec<String>, i64)> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

/// The HTTP client owns its jar; an optional, weak observer copies wire updates.
#[derive(Default)]
pub(super) struct CookieStore {
    jar: Jar,
    observer: Mutex<Weak<Mutex<Updates>>>,
}

impl CookieStore {
    pub(super) fn observe(&self) -> ResponseCookies {
        let observer = ResponseCookies::default();
        *self.observer.lock().unwrap() = Arc::downgrade(&observer.0);
        observer
    }
}

impl reqwest::cookie::CookieStore for CookieStore {
    fn set_cookies(&self, cookies: &mut dyn Iterator<Item = &HeaderValue>, url: &Url) {
        let received_at = chrono::Utc::now().timestamp();
        let cookies: Vec<_> = cookies.collect();
        self.jar.set_cookies(&mut cookies.iter().copied(), url);
        if let Some(observer) = self.observer.lock().unwrap().upgrade() {
            let values = cookies
                .into_iter()
                .filter_map(|value| value.to_str().ok().map(str::to_string))
                .collect();
            observer
                .lock()
                .unwrap()
                .push((url.to_string(), values, received_at));
        }
    }

    fn cookies(&self, url: &Url) -> Option<HeaderValue> {
        self.jar.cookies(url)
    }
}

#[cfg(test)]
mod tests {
    use reqwest::cookie::CookieStore as _;

    use super::*;

    #[test]
    fn cookies_keep_scope_and_observers_drain_without_clearing_the_jar() {
        let store = CookieStore::default();
        let url = Url::parse("https://catalog.example/v1/branches").unwrap();
        let observer = store.observe();
        let cookie = HeaderValue::from_static("sid=accepted; Path=/v1/; Secure; HttpOnly");
        store.set_cookies(&mut [&cookie].into_iter(), &url);
        assert_eq!(store.cookies(&url).unwrap(), "sid=accepted");
        assert!(store
            .cookies(&Url::parse("http://catalog.example/v1/").unwrap())
            .is_none());
        assert!(store
            .cookies(&Url::parse("https://catalog.example/other").unwrap())
            .is_none());
        assert!(store
            .cookies(&Url::parse("https://other.example/v1/").unwrap())
            .is_none());
        let updates = observer.take();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].0, url.to_string());
        assert_eq!(updates[0].1, vec![cookie.to_str().unwrap()]);
        assert!((chrono::Utc::now().timestamp() - updates[0].2).abs() <= 1);
        assert!(observer.take().is_empty());
        assert_eq!(store.cookies(&url).unwrap(), "sid=accepted");

        let remove = HeaderValue::from_static("sid=; Path=/v1/; Max-Age=0");
        store.set_cookies(&mut [&remove].into_iter(), &url);
        assert!(store.cookies(&url).is_none());
        assert_eq!(observer.take()[0].1, vec![remove.to_str().unwrap()]);
        drop(observer);
        assert!(store.observer.lock().unwrap().upgrade().is_none());
    }
}
