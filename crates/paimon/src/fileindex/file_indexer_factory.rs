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

use crate::{options::Options, spec::DataType};

use super::FileIndexer;

pub trait FileIndexerFactory: Send + Sync {
    fn identifier(&self) -> String;
    fn create(&self, data_type: DataType, options: Options) -> impl FileIndexer;
}

// pub struct FileIndexerFactoryRegistry<F: FileIndexerFactory> {
//     factories: Mutex<HashMap<String, Arc<F>>>,
// }

// lazy_static! {
//     static ref EXAMPLE_FACTORY_REGISTRY: FileIndexerFactoryRegistry<dyn FileIndexerFactory> =
//         FileIndexerFactoryRegistry {
//             factories: Mutex::new(HashMap::new()),
//         };
// }

// impl<F: FileIndexerFactory + 'static> FileIndexerFactoryRegistry<F> {
//     pub fn register(&self, factory: Arc<F>) {
//         let identifier = factory.identifier().to_string();
//         let mut factories = self.factories.lock().unwrap();
//         if factories.insert(identifier.clone(), factory).is_some() {
//             warn!(
//                 "Found multiple FileIndexer for type: {}, choose one of them",
//                 identifier
//             );
//         }
//     }

//     pub fn load(&self, type_name: &str) -> Arc<F> {
//         let factories = self.factories.lock().unwrap();
//         if let Some(factory) = factories.get(type_name) {
//             Arc::clone(factory)
//         } else {
//             panic!("Can't find file index for type: {}", type_name);
//         }
//     }
// }
