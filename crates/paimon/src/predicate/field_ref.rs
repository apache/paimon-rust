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

use crate::spec::DataType;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct FieldRef {
    index: usize,
    name: String,
    data_type: DataType,
}

impl FieldRef {
    pub fn new(index: usize, name: &str, data_type: DataType) -> Self {
        FieldRef {
            index,
            name: name.to_string(),
            data_type,
        }
    }

    pub fn index(&self) -> usize {
        self.index
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl PartialEq for FieldRef {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.name == other.name && self.data_type == other.data_type
    }
}

impl Eq for FieldRef {}

impl Debug for FieldRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FieldRef {{ index: {}, name: '{}', data_type: {:?} }}",
            self.index, self.name, self.data_type
        )
    }
}

impl Hash for FieldRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.index.hash(state);
        self.name.hash(state);
        self.data_type.hash(state);
    }
}

impl Display for FieldRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FieldRef {{ index: {}, name: '{}', data_type: {:?} }}",
            self.index, self.name, self.data_type
        )
    }
}
