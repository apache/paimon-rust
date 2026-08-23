// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use paimon_query_service_server::{init_logging, load_config, serve, BoxError};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let _logging_guard = init_logging()?;
    let path = std::env::var("QUERY_SERVICE_CONFIG")
        .or_else(|_| std::env::var("BLOB_QUERY_CONFIG"))
        .unwrap_or_else(|_| "query-service.json".to_string());
    let config = load_config(&path)?;
    serve(config).await
}
