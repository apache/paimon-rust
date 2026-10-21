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

//! Opaque, snapshot-scoped search work for local or remote schedulers.

use super::Table;
#[cfg(any(feature = "internal-api", test))]
use crate::spec::CoreOptions;
use crate::spec::{Snapshot, TableSchema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

const VERSION: u8 = 1;
pub(super) const SEARCH_TASK_WINDOW: usize = 16;

pub(super) fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[cfg(any(feature = "internal-api", test))]
pub(super) fn encode<T: Serialize>(value: &T) -> crate::Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| invalid(format!("Invalid search task: {error}")))
}

#[cfg(any(feature = "internal-api", test))]
pub(super) fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> crate::Result<T> {
    serde_json::from_slice(value).map_err(|error| invalid(format!("Invalid search task: {error}")))
}

/// Work created by Core. Schedulers transport it without interpreting its contents.
#[derive(Clone, Serialize, Deserialize)]
pub struct SearchTask {
    version: u8,
    id: String,
    location: String,
    branch: String,
    schema: TableSchema,
    snapshot: Option<Snapshot>,
    kind: String,
    payload: Value,
}

impl SearchTask {
    pub(super) fn new(table: &Table, kind: &str, payload: Value) -> crate::Result<Self> {
        Ok(Self {
            version: VERSION,
            id: uuid::Uuid::new_v4().to_string(),
            location: table.location().trim_end_matches('/').into(),
            branch: table.branch().into(),
            schema: table.schema().clone(),
            // None is an explicitly captured empty table, never "latest".
            snapshot: table.travel_snapshot().cloned(),
            kind: kind.into(),
            payload,
        })
    }
}

#[cfg(any(feature = "internal-api", test))]
impl SearchTask {
    pub fn serialize(&self) -> crate::Result<Vec<u8>> {
        encode(self)
    }

    /// Share repeated immutable task inputs once per scheduler invocation.
    /// Query matrices, schema/snapshot data and repeated bitmaps stay opaque.
    pub fn serialize_batch(tasks: &[Self]) -> crate::Result<(Vec<u8>, Vec<Vec<u8>>)> {
        let values = tasks
            .iter()
            .map(|task| serde_json::to_value(task).map_err(|error| invalid(error.to_string())))
            .collect::<crate::Result<Vec<_>>>()?;
        let mut counts = HashMap::<Vec<u8>, usize>::new();
        fn count(value: &Value, counts: &mut HashMap<Vec<u8>, usize>) -> crate::Result<()> {
            if value.is_array() || value.is_object() {
                let bytes = encode(value)?;
                if bytes.len() >= 256 {
                    *counts.entry(bytes).or_default() += 1;
                }
                match value {
                    Value::Array(values) => {
                        for value in values {
                            count(value, counts)?;
                        }
                    }
                    Value::Object(values) => {
                        for value in values.values() {
                            count(value, counts)?;
                        }
                    }
                    _ => (),
                }
            }
            Ok(())
        }
        for value in &values {
            count(value, &mut counts)?;
        }
        let context_id = uuid::Uuid::new_v4().to_string();
        let mut shared = Vec::new();
        let mut positions = HashMap::<Vec<u8>, usize>::new();
        fn share(
            value: &mut Value,
            path: &str,
            counts: &HashMap<Vec<u8>, usize>,
            positions: &mut HashMap<Vec<u8>, usize>,
            shared: &mut Vec<Value>,
            references: &mut Vec<(String, usize)>,
        ) -> crate::Result<()> {
            if value.is_array() || value.is_object() {
                let bytes = encode(value)?;
                if counts.get(&bytes).is_some_and(|count| *count > 1) {
                    let index = match positions.get(&bytes) {
                        Some(index) => *index,
                        None => {
                            let index = shared.len();
                            shared.push(value.clone());
                            positions.insert(bytes, index);
                            index
                        }
                    };
                    references.push((path.into(), index));
                    *value = Value::Null;
                    return Ok(());
                }
                match value {
                    Value::Array(values) => {
                        for (index, value) in values.iter_mut().enumerate() {
                            share(
                                value,
                                &format!("{path}/{index}"),
                                counts,
                                positions,
                                shared,
                                references,
                            )?;
                        }
                    }
                    Value::Object(values) => {
                        for (key, value) in values.iter_mut() {
                            let escaped = key.replace('~', "~0").replace('/', "~1");
                            share(
                                value,
                                &format!("{path}/{escaped}"),
                                counts,
                                positions,
                                shared,
                                references,
                            )?;
                        }
                    }
                    _ => (),
                }
            }
            Ok(())
        }
        let frames = values
            .into_iter()
            .map(|mut task| {
                let mut references = Vec::new();
                share(
                    &mut task,
                    "",
                    &counts,
                    &mut positions,
                    &mut shared,
                    &mut references,
                )?;
                encode(&SharedTaskFrame {
                    version: VERSION,
                    context_id: context_id.clone(),
                    task,
                    references,
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok((
            encode(&SharedTaskContext {
                version: VERSION,
                id: context_id,
                values: shared,
            })?,
            frames,
        ))
    }

    pub fn deserialize_with_context(bytes: &[u8], context: &[u8]) -> crate::Result<Self> {
        let context: SharedTaskContext = decode(context)?;
        let mut frame: SharedTaskFrame = decode(bytes)?;
        if context.version != VERSION || frame.version != VERSION || context.id != frame.context_id
        {
            return Err(invalid("Search task shared context does not match"));
        }
        for (path, index) in frame.references {
            let value = context
                .values
                .get(index)
                .ok_or_else(|| invalid("Invalid search task shared reference"))?;
            *frame
                .task
                .pointer_mut(&path)
                .ok_or_else(|| invalid("Invalid search task shared path"))? = value.clone();
        }
        let task: Self =
            serde_json::from_value(frame.task).map_err(|error| invalid(error.to_string()))?;
        if task.version != VERSION {
            return Err(invalid("Unsupported search task version"));
        }
        Ok(task)
    }

    pub async fn execute(&self, table: &Table) -> crate::Result<SearchTaskResult> {
        // The worker's runtime restrictions take precedence over transported options.
        CoreOptions::new(table.schema().options()).ensure_read_authorized()?;
        CoreOptions::new(self.schema.options()).ensure_read_authorized()?;
        if self.location != table.location().trim_end_matches('/') || self.branch != table.branch()
        {
            return Err(invalid(
                "Search task belongs to a different table or branch",
            ));
        }
        let pinned = table
            .copy_with_resolved_schema(self.schema.clone(), &self.branch)?
            .copy_with_pinned_snapshot(self.snapshot.as_ref());
        let payload = match self.kind.as_str() {
            "vector-index" | "vector-rows" => {
                super::de_vector_read::execute_search_task(&pinned, &self.kind, &self.payload)
                    .await?
            }
            #[cfg(feature = "fulltext")]
            kind if kind.starts_with("fulltext-") => {
                super::full_text_search_builder::execute_search_task(&pinned, kind, &self.payload)
                    .await?
            }
            _ => return Err(invalid("Unsupported search task kind")),
        };
        Ok(SearchTaskResult {
            version: VERSION,
            id: self.id.clone(),
            payload,
        })
    }
}

/// An opaque worker outcome, tied to the exact task that produced it.
#[derive(Clone, Serialize, Deserialize)]
pub struct SearchTaskResult {
    version: u8,
    id: String,
    payload: Value,
}

#[cfg(any(feature = "internal-api", test))]
impl SearchTaskResult {
    pub fn serialize(&self) -> crate::Result<Vec<u8>> {
        encode(self)
    }
    pub fn deserialize(bytes: &[u8]) -> crate::Result<Self> {
        let result: Self = decode(bytes)?;
        if result.version != VERSION {
            return Err(invalid("Unsupported search result version"));
        }
        Ok(result)
    }
}

/// A scheduler owns task placement and cancellation; Core owns search semantics.
/// On failure it must cancel or drain outstanding work before returning.
#[async_trait::async_trait]
pub trait SearchTaskExecutor: Send + Sync {
    async fn execute(&self, tasks: Vec<SearchTask>) -> crate::Result<Vec<SearchTaskResult>>;
}

pub(super) async fn execute_tasks(
    executor: &dyn SearchTaskExecutor,
    tasks: Vec<SearchTask>,
) -> crate::Result<Vec<Value>> {
    if tasks.is_empty() {
        return Ok(Vec::new());
    }
    let ids = tasks.iter().map(|task| task.id.clone()).collect::<Vec<_>>();
    let results = executor.execute(tasks).await?;
    if results.len() != ids.len() {
        return Err(invalid("Search executor returned the wrong result count"));
    }
    let mut by_id = HashMap::with_capacity(results.len());
    for result in results {
        if result.version != VERSION || by_id.insert(result.id, result.payload).is_some() {
            return Err(invalid(
                "Search executor returned a duplicate or invalid result",
            ));
        }
    }
    ids.into_iter()
        .map(|id| {
            by_id
                .remove(&id)
                .ok_or_else(|| invalid("Search executor returned a result for another task"))
        })
        .collect()
}

#[cfg(any(feature = "internal-api", test))]
#[derive(Serialize, Deserialize)]
struct SharedTaskContext {
    version: u8,
    id: String,
    values: Vec<Value>,
}
#[cfg(any(feature = "internal-api", test))]
#[derive(Serialize, Deserialize)]
struct SharedTaskFrame {
    version: u8,
    context_id: String,
    task: Value,
    references: Vec<(String, usize)>,
}

#[cfg(any(feature = "internal-api", test))]
impl Table {
    /// Internal worker bridge; ordinary callers execute a constructed scan plan.
    pub(crate) async fn execute_search_task(
        &self,
        task: &[u8],
        context: &[u8],
    ) -> crate::Result<Vec<u8>> {
        SearchTask::deserialize_with_context(task, context)?
            .execute(self)
            .await?
            .serialize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::vector_search_test_utils::vector_test_table;

    #[test]
    fn scheduled_transport_shares_large_inputs_and_rejects_foreign_context() {
        let table = vector_test_table();
        let matrix = (0..32)
            .map(|row| {
                (0..128)
                    .map(|column| (row * 128 + column) as u32)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let tasks = (0..20)
            .map(|entry| {
                SearchTask::new(
                    &table,
                    "vector-index",
                    serde_json::json!({"queries": matrix, "entry": entry}),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let original_size = tasks
            .iter()
            .map(|task| task.serialize().unwrap().len())
            .sum::<usize>();
        let (context, frames) = SearchTask::serialize_batch(&tasks).unwrap();
        let wire_size = context.len() + frames.iter().map(Vec::len).sum::<usize>();
        assert!(
            wire_size < original_size / 4,
            "shared {wire_size} versus copied {original_size}"
        );
        for (task, frame) in tasks.iter().zip(&frames) {
            assert_eq!(
                task.serialize().unwrap(),
                SearchTask::deserialize_with_context(frame, &context)
                    .unwrap()
                    .serialize()
                    .unwrap()
            );
        }
        let (foreign_context, _) = SearchTask::serialize_batch(&tasks).unwrap();
        assert!(SearchTask::deserialize_with_context(&frames[0], &foreign_context).is_err());
    }

    struct ReplyExecutor {
        invalid: bool,
    }
    #[async_trait::async_trait]
    impl SearchTaskExecutor for ReplyExecutor {
        async fn execute(&self, tasks: Vec<SearchTask>) -> crate::Result<Vec<SearchTaskResult>> {
            let mut results = tasks
                .into_iter()
                .map(|task| SearchTaskResult {
                    version: VERSION,
                    id: task.id,
                    payload: task.payload,
                })
                .collect::<Vec<_>>();
            if self.invalid {
                results[1].id = results[0].id.clone();
            }
            results.reverse();
            Ok(results)
        }
    }

    #[tokio::test]
    async fn scheduled_results_reorder_by_identity_and_reject_duplicates() {
        let table = vector_test_table();
        let tasks = (0..2)
            .map(|value| SearchTask::new(&table, "test", Value::from(value)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            execute_tasks(&ReplyExecutor { invalid: false }, tasks.clone())
                .await
                .unwrap(),
            vec![Value::from(0), Value::from(1)]
        );
        assert!(execute_tasks(&ReplyExecutor { invalid: true }, tasks)
            .await
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
    }

    #[tokio::test]
    async fn scheduled_worker_preserves_runtime_authorization_restrictions() {
        let table = vector_test_table();
        let task = SearchTask::new(&table, "vector-index", Value::Null).unwrap();
        let schema = table.schema().copy_with_options(HashMap::from([(
            "query-auth.enabled".into(),
            "true".into(),
        )]));
        let restricted = table
            .copy_with_resolved_schema(schema, table.branch())
            .unwrap();
        let error = task.execute(&restricted).await.err().unwrap();
        assert!(error.to_string().contains("query-auth.enabled"), "{error}");
    }
}
