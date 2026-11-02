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

//! Scoped access to the shared Java-written warehouse.

use std::sync::{Arc, Mutex, Weak};

use paimon::{CatalogOptions, FileSystemCatalog, Options};

pub enum TestWarehouse {
    External(String),
    Golden(Arc<tempfile::TempDir>),
}

impl TestWarehouse {
    pub fn path(&self) -> &str {
        match self {
            Self::External(path) => path,
            Self::Golden(directory) => directory.path().to_str().expect("UTF-8 warehouse path"),
        }
    }
}

pub fn test_catalog() -> (TestWarehouse, FileSystemCatalog) {
    let warehouse = test_warehouse();
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse.path());
    let catalog = FileSystemCatalog::new(options).expect("Failed to create FileSystemCatalog");
    (warehouse, catalog)
}

pub fn test_warehouse() -> TestWarehouse {
    if let Ok(path) = std::env::var("PAIMON_TEST_WAREHOUSE") {
        return TestWarehouse::External(path);
    }

    static WAREHOUSE: Mutex<Weak<tempfile::TempDir>> = Mutex::new(Weak::new());
    unpack_warehouse(&WAREHOUSE)
}

fn unpack_warehouse(cache: &Mutex<Weak<tempfile::TempDir>>) -> TestWarehouse {
    let mut cached = cache.lock().expect("Golden warehouse cache poisoned");
    let directory = cached.upgrade().unwrap_or_else(|| {
        let temp = tempfile::tempdir().expect("Failed to create golden warehouse directory");
        let decoder = flate2::read::GzDecoder::new(include_bytes!("warehouse.tar.gz").as_slice());
        tar::Archive::new(decoder)
            .unpack(temp.path())
            .expect("Failed to extract golden warehouse");
        let directory = Arc::new(temp);
        *cached = Arc::downgrade(&directory);
        directory
    });
    TestWarehouse::Golden(directory)
}

#[cfg(test)]
mod tests {
    use super::{test_warehouse, unpack_warehouse};
    use std::sync::{Mutex, Weak};

    #[test]
    fn warehouse_directory_is_removed_on_exit() {
        const PROBE: &str = "PAIMON_GOLDEN_WAREHOUSE_CLEANUP_PROBE";
        if let Some(output) = std::env::var_os(PROBE) {
            let warehouse = test_warehouse();
            std::fs::write(output, warehouse.path()).unwrap();
            return;
        }

        let probe = tempfile::tempdir().unwrap();
        let output = probe.path().join("warehouse-path");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "golden_warehouse::tests::warehouse_directory_is_removed_on_exit",
            ])
            .env(PROBE, &output)
            .env_remove("PAIMON_TEST_WAREHOUSE")
            .status()
            .unwrap();
        assert!(status.success());
        let warehouse = std::fs::read_to_string(output).unwrap();
        let leaked = std::path::Path::new(&warehouse).exists();
        if leaked {
            std::fs::remove_dir_all(&warehouse).unwrap();
        }
        assert!(
            !leaked,
            "Golden warehouse leaked after process exit: {warehouse}"
        );
    }
    #[test]
    fn warehouse_lives_until_the_last_reader_finishes() {
        let cache = Mutex::new(Weak::new());
        let first = unpack_warehouse(&cache);
        let second = unpack_warehouse(&cache);
        assert_eq!(first.path(), second.path());
        let path = std::path::PathBuf::from(first.path());
        let schema = path.join("default.db/simple_log_table/schema/schema-0");
        assert!(schema.is_file());
        drop(first);
        assert!(schema.is_file(), "Another reader still owns the warehouse");
        drop(second);
        assert!(!path.exists(), "Last reader should clean up the warehouse");

        let next = unpack_warehouse(&cache);
        assert!(std::path::Path::new(next.path())
            .join("default.db/simple_log_table/schema/schema-0")
            .is_file());
    }
}
