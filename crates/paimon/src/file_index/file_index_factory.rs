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

use once_cell::sync::Lazy;

use crate::{options::Options, spec::DataType};

use super::FileIndexer;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait FileIndexerFactory: Send + Sync {
    fn identifier(&self) -> String;

    fn create(&self, data_type: DataType, options: Options) -> crate::Result<Box<dyn FileIndexer>>;
}

pub static FACTORIES: Lazy<Mutex<HashMap<String, Arc<dyn FileIndexerFactory>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub fn register_factory(factory: Arc<dyn FileIndexerFactory>) -> crate::Result<()> {
    let mut factories = FACTORIES.lock().unwrap();
    factories
        .insert(factory.identifier().to_string(), factory.clone())
        .map_or(Ok(()), |_| {
            Err(crate::Error::FactoryAlreadyExists {
                identifier: factory.identifier().to_string(),
            })
        })
}

pub fn load_factory(typ: &str) -> crate::Result<Arc<dyn FileIndexerFactory>> {
    FACTORIES
        .lock()
        .unwrap()
        .get(typ)
        .map(Arc::clone)
        .ok_or_else(|| crate::Error::FactoryNotFound {
            identifier: typ.to_string(),
        })
}
