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

//! Connect reader working-memory reservations to the executing DataFusion task.

use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use paimon::resource::{MemoryPool, ResourceContext};

#[derive(Debug)]
struct DataFusionMemoryPool {
    reservation: MemoryReservation,
}

impl MemoryPool for DataFusionMemoryPool {
    fn try_reserve(&self, bytes: usize) -> paimon::Result<()> {
        self.reservation
            .try_grow(bytes)
            .map_err(|error| paimon::Error::ResourceExhausted {
                message: error.to_string(),
            })
    }

    fn release(&self, bytes: usize) {
        self.reservation.shrink(bytes);
    }
}

pub(crate) fn reader_resources(
    context: &TaskContext,
    partition: usize,
) -> paimon::Result<ResourceContext> {
    // Register per execution, not on the reusable plan. Reader working state
    // cannot spill; downstream operators account for batches they retain.
    let reservation = MemoryConsumer::new(format!("PaimonTableScan[{partition}]"))
        .with_can_spill(false)
        .register(context.memory_pool());
    ResourceContext::builder()
        .memory_pool(Arc::new(DataFusionMemoryPool { reservation }))
        .build()
}
