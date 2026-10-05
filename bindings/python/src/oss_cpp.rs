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

use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const LIBRARY_PATH: &str = "fs.oss.cpp.library.path";

pub(crate) fn complete_options(options: HashMap<String, String>) -> HashMap<String, String> {
    complete_options_with(options, || discover_library("pypaimon_oss_cpp"))
}

fn complete_options_with(
    mut options: HashMap<String, String>,
    discover: impl FnOnce() -> Option<PathBuf>,
) -> HashMap<String, String> {
    if options.get("fs.oss.impl").is_some_and(|v| v == "cpp") && !options.contains_key(LIBRARY_PATH)
    {
        if let Some(path) = discover() {
            if let Some(path) = path.to_str() {
                options.insert(LIBRARY_PATH.into(), path.into());
            }
        }
    }
    options
}

fn discover_library(package: &str) -> Option<PathBuf> {
    Python::attach(|py| {
        // Inspect a top-level package without importing its initialization code.
        let spec = py
            .import("importlib.util")
            .ok()?
            .call_method1("find_spec", (package,))
            .ok()?;
        let origin = spec.getattr("origin").ok()?.extract::<String>().ok()?;
        library_in(Path::new(&origin).parent()?)
    })
}

fn library_in(directory: &Path) -> Option<PathBuf> {
    let name = if cfg!(target_os = "macos") {
        "liboss_cpp_bridge.dylib"
    } else if cfg!(target_os = "linux") {
        "liboss_cpp_bridge.so"
    } else {
        return None;
    };
    let path = directory.join(name);
    path.is_file().then_some(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_only_missing_cpp_path() {
        let options = HashMap::from([("fs.oss.impl".into(), "cpp".into())]);
        let result = complete_options_with(options.clone(), || Some("/package/bridge.so".into()));
        assert_eq!(result[LIBRARY_PATH], "/package/bridge.so");
        assert_eq!(result["fs.oss.impl"], "cpp");
        assert_eq!(complete_options_with(options.clone(), || None), options);
    }

    #[test]
    fn explicit_paths_are_never_replaced() {
        for path in ["/missing/explicit.so", ""] {
            let options = HashMap::from([
                ("fs.oss.impl".into(), "cpp".into()),
                (LIBRARY_PATH.into(), path.into()),
            ]);
            assert_eq!(
                complete_options_with(options.clone(), || panic!("must not discover")),
                options
            );
        }
    }

    #[test]
    fn other_backends_do_not_discover() {
        for backend in [None, Some("jindo"), Some("legacy"), Some("CPP")] {
            let options = backend
                .map(|v| HashMap::from([("fs.oss.impl".into(), v.into())]))
                .unwrap_or_default();
            assert_eq!(
                complete_options_with(options.clone(), || panic!("must not discover")),
                options
            );
        }
    }

    #[test]
    fn finds_only_the_platform_library() {
        let directory =
            std::env::temp_dir().join(format!("paimon-oss-cpp-discovery-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        assert!(library_in(&directory).is_none());
        let name = if cfg!(target_os = "macos") {
            "liboss_cpp_bridge.dylib"
        } else {
            "liboss_cpp_bridge.so"
        };
        let path = directory.join(name);
        std::fs::write(&path, []).unwrap();
        if cfg!(any(target_os = "macos", target_os = "linux")) {
            assert_eq!(library_in(&directory), Some(path.clone()));
            Python::attach(|py| {
                let name = "_paimon_oss_cpp_discovery_test";
                let module = PyModule::new(py, name).unwrap();
                let spec = py
                    .import("importlib.machinery")
                    .unwrap()
                    .getattr("ModuleSpec")
                    .unwrap()
                    .call1((name, py.None()))
                    .unwrap();
                spec.setattr("origin", directory.join("__init__.py").to_str().unwrap())
                    .unwrap();
                module.setattr("__spec__", spec).unwrap();
                let modules = py.import("sys").unwrap().getattr("modules").unwrap();
                modules.set_item(name, module).unwrap();
                let result = discover_library(name);
                modules.del_item(name).unwrap();
                assert_eq!(result, Some(path.clone()));
                assert!(discover_library("_paimon_nonexistent_oss_cpp_package").is_none());
            });
        }
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
}
