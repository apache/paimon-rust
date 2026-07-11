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

//! Filter row kinds at write time (`ignore-delete`, `ignore-update-before`).
//!
//! Reference: Java `org.apache.paimon.utils.RowKindFilter`.

use crate::spec::{CoreOptions, RowKind};

pub struct RowKindFilter {
    ignore_all_retracts: bool,
    ignore_update_before: bool,
}

impl RowKindFilter {
    pub fn of(options: CoreOptions<'_>) -> Option<Self> {
        let ignore_all_retracts = options.ignore_delete();
        let ignore_update_before = options.ignore_update_before();
        if !ignore_all_retracts && !ignore_update_before {
            return None;
        }
        Some(Self {
            ignore_all_retracts,
            ignore_update_before,
        })
    }

    pub fn test(&self, row_kind: RowKind) -> bool {
        match row_kind {
            RowKind::Delete if self.ignore_all_retracts => false,
            RowKind::UpdateBefore if self.ignore_update_before || self.ignore_all_retracts => false,
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn ignore_delete_drops_delete_and_update_before() {
        let filter = RowKindFilter::of(CoreOptions::new(&HashMap::from([(
            "ignore-delete".to_string(),
            "true".to_string(),
        )])))
        .unwrap();
        assert!(!filter.test(RowKind::Delete));
        assert!(!filter.test(RowKind::UpdateBefore));
        assert!(filter.test(RowKind::Insert));
    }

    #[test]
    fn ignore_update_before_drops_only_update_before() {
        let filter = RowKindFilter::of(CoreOptions::new(&HashMap::from([(
            "ignore-update-before".to_string(),
            "true".to_string(),
        )])))
        .unwrap();
        assert!(filter.test(RowKind::Delete));
        assert!(!filter.test(RowKind::UpdateBefore));
        assert!(filter.test(RowKind::Insert));
    }
}
