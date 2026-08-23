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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{BinaryArray, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use base64::Engine;
use futures::future::join_all;
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::{
    BlobDescriptor, BlobType, DataType, IntType, Schema, SchemaChange, VarBinaryType,
};
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_query_service::{
    BatchGetRequest, BlobLookupOptions, BlobLookupService, DescriptorFormat, LookupError,
    LookupStatus, LookupStrategy, QueryBudget, TableLookupPolicy, TableRef,
};
use serde_json::json;
use tempfile::TempDir;

struct Fixture {
    _warehouse: TempDir,
    catalog: Arc<FileSystemCatalog>,
    table: TableRef,
}

impl Fixture {
    async fn new(descriptor_field: bool) -> Self {
        let warehouse = TempDir::new().unwrap();
        let mut options = Options::new();
        options.set(
            CatalogOptions::WAREHOUSE,
            warehouse.path().to_str().unwrap(),
        );
        let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
        catalog
            .create_database("db", false, Default::default())
            .await
            .unwrap();

        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("picture", DataType::Blob(BlobType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("global-index.enabled", "true");
        if descriptor_field {
            schema = schema.option("blob-descriptor-field", "picture");
        }
        catalog
            .create_table(
                &Identifier::new("db", "assets"),
                schema.build().unwrap(),
                false,
            )
            .await
            .unwrap();

        Self {
            _warehouse: warehouse,
            catalog,
            table: TableRef::new("db", "assets"),
        }
    }

    fn service(&self) -> BlobLookupService {
        self.service_with_budget(QueryBudget {
            max_batch_keys: 10,
            max_planned_files: 10,
            max_planned_bytes: 16 * 1024 * 1024,
        })
    }

    fn service_with_budget(&self, budget: QueryBudget) -> BlobLookupService {
        self.service_with_options(budget, BlobLookupOptions::default())
    }

    fn service_with_options(
        &self,
        budget: QueryBudget,
        options: BlobLookupOptions,
    ) -> BlobLookupService {
        BlobLookupService::new_with_options(
            self.catalog.clone(),
            [TableLookupPolicy {
                table: self.table.clone(),
                key_fields: vec!["id".to_string()],
                blob_fields: BTreeSet::from(["picture".to_string()]),
                strategy: LookupStrategy::GlobalBtree,
                budget,
            }],
            options,
        )
        .unwrap()
    }

    async fn append(&self, ids: Vec<i32>, pictures: Vec<Option<Vec<u8>>>) -> i64 {
        let picture_refs = pictures
            .iter()
            .map(|value| value.as_deref())
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, true),
                ArrowField::new("picture", ArrowDataType::Binary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(BinaryArray::from(picture_refs)),
            ],
        )
        .unwrap();

        let table = self
            .catalog
            .get_table(&Identifier::new("db", "assets"))
            .await
            .unwrap();
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        builder.new_commit().commit(messages).await.unwrap();
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap()
    }

    fn request(&self, ids: &[i32]) -> BatchGetRequest {
        BatchGetRequest {
            table: self.table.clone(),
            keys: ids
                .iter()
                .map(|id| BTreeMap::from([("id".to_string(), json!(id))]))
                .collect(),
            blob_fields: vec!["picture".to_string()],
            snapshot_id: None,
            descriptor_format: DescriptorFormat::PaimonBase64,
        }
    }
}

#[tokio::test]
async fn batch_get_returns_descriptors_and_preserves_request_order() {
    let fixture = Fixture::new(false).await;
    fixture
        .append(
            vec![1, 2, 3],
            vec![Some(b"first".to_vec()), Some(b"second".to_vec()), None],
        )
        .await;

    let response = fixture
        .service()
        .batch_get(fixture.request(&[2, 99, 1, 3]))
        .await
        .unwrap();

    assert_eq!(response.results.len(), 4);
    assert!(response.scan.planned_files >= 1);
    assert!(response.scan.planned_bytes > 0);
    assert_eq!(response.results[0].status, LookupStatus::Found);
    assert_eq!(response.results[1].status, LookupStatus::NotFound);
    assert_eq!(response.results[2].status, LookupStatus::Found);
    assert_eq!(response.results[3].status, LookupStatus::Found);
    assert_eq!(response.results[0].key["id"], json!(2));

    let descriptor = response.results[0].blobs["picture"].as_ref().unwrap();
    assert_eq!(descriptor.length, 6);
    assert!(descriptor.uri.ends_with(".blob"));
    let encoded = descriptor.encoded.as_ref().unwrap();
    let raw = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .unwrap();
    let decoded = BlobDescriptor::deserialize(&raw).unwrap();
    assert_eq!(decoded.length(), 6);

    assert_eq!(response.results[3].blobs["picture"], None);
}

#[tokio::test]
async fn explicit_snapshot_isolated_from_later_duplicate_key() {
    let fixture = Fixture::new(false).await;
    let first_snapshot = fixture.append(vec![1], vec![Some(b"old".to_vec())]).await;
    fixture.append(vec![1], vec![Some(b"new".to_vec())]).await;

    let service = fixture.service();
    let latest = service.batch_get(fixture.request(&[1])).await.unwrap();
    assert!(!latest.cache_hit);
    assert_eq!(latest.results[0].status, LookupStatus::NonUnique);

    let mut historical_request = fixture.request(&[1]);
    historical_request.snapshot_id = Some(first_snapshot);
    let historical = service.batch_get(historical_request.clone()).await.unwrap();
    assert!(!historical.cache_hit);
    assert_eq!(historical.snapshot_id, Some(first_snapshot));
    assert_eq!(historical.results[0].status, LookupStatus::Found);
    assert_eq!(
        historical.results[0].blobs["picture"]
            .as_ref()
            .unwrap()
            .length,
        3
    );

    let cached_historical = service.batch_get(historical_request).await.unwrap();
    assert!(cached_historical.cache_hit);
    assert_eq!(cached_historical.snapshot_id, Some(first_snapshot));
    assert_eq!(service.descriptor_cache_stats().misses, 2);
    assert_eq!(service.descriptor_cache_stats().hits, 1);
}

#[tokio::test]
async fn cached_explicit_snapshot_is_rejected_after_snapshot_deletion() {
    let fixture = Fixture::new(false).await;
    let snapshot_id = fixture.append(vec![1], vec![Some(b"old".to_vec())]).await;
    let service = fixture.service();
    let mut request = fixture.request(&[1]);
    request.snapshot_id = Some(snapshot_id);

    service.batch_get(request.clone()).await.unwrap();
    let table = fixture
        .catalog
        .get_table(&Identifier::new("db", "assets"))
        .await
        .unwrap();
    table
        .snapshot_manager()
        .delete_snapshot(snapshot_id)
        .await
        .unwrap();

    let error = service.batch_get(request).await.unwrap_err();
    assert!(matches!(
        error,
        LookupError::Paimon(paimon::Error::SnapshotNotExist {
            snapshot_id: missing
        }) if missing == snapshot_id
    ));
}

#[tokio::test]
async fn historical_lookup_validates_only_requested_blob_fields() {
    let fixture = Fixture::new(false).await;
    let snapshot_id = fixture.append(vec![1], vec![Some(b"old".to_vec())]).await;
    fixture
        .catalog
        .alter_table(
            &Identifier::new("db", "assets"),
            vec![SchemaChange::add_column(
                "thumbnail".to_string(),
                DataType::Blob(BlobType::new()),
            )],
            false,
        )
        .await
        .unwrap();
    let service = BlobLookupService::new(
        fixture.catalog.clone(),
        [TableLookupPolicy {
            table: fixture.table.clone(),
            key_fields: vec!["id".to_string()],
            blob_fields: BTreeSet::from(["picture".to_string(), "thumbnail".to_string()]),
            strategy: LookupStrategy::GlobalBtree,
            budget: QueryBudget {
                max_batch_keys: 10,
                max_planned_files: 10,
                max_planned_bytes: 16 * 1024 * 1024,
            },
        }],
    )
    .unwrap();
    let mut request = fixture.request(&[1]);
    request.snapshot_id = Some(snapshot_id);

    let response = service.batch_get(request.clone()).await.unwrap();
    assert_eq!(response.snapshot_id, Some(snapshot_id));
    assert_eq!(response.results[0].status, LookupStatus::Found);

    request.blob_fields = vec!["thumbnail".to_string()];
    let error = service.batch_get(request).await.unwrap_err();
    assert!(matches!(
        error,
        LookupError::InvalidRequest(message)
            if message.contains("selected snapshot") && message.contains("thumbnail")
    ));
}

#[tokio::test]
async fn descriptor_cache_does_not_cross_drop_and_recreate() {
    let fixture = Fixture::new(false).await;
    fixture.append(vec![1], vec![Some(b"old".to_vec())]).await;
    let service = fixture.service_with_options(
        QueryBudget {
            max_batch_keys: 10,
            max_planned_files: 10,
            max_planned_bytes: 16 * 1024 * 1024,
        },
        BlobLookupOptions {
            table_cache_ttl: Duration::ZERO,
            ..BlobLookupOptions::default()
        },
    );
    let first = service.batch_get(fixture.request(&[1])).await.unwrap();
    assert_eq!(
        first.results[0].blobs["picture"].as_ref().unwrap().length,
        3
    );

    let identifier = Identifier::new("db", "assets");
    fixture
        .catalog
        .drop_table(&identifier, false)
        .await
        .unwrap();
    fixture
        .catalog
        .create_table(
            &identifier,
            Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("picture", DataType::Blob(BlobType::new()))
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true")
                .option("global-index.enabled", "true")
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    fixture
        .append(vec![1], vec![Some(b"recreated".to_vec())])
        .await;

    let recreated = service.batch_get(fixture.request(&[1])).await.unwrap();
    assert!(!recreated.cache_hit);
    assert_eq!(
        recreated.results[0].blobs["picture"]
            .as_ref()
            .unwrap()
            .length,
        9
    );
}

#[tokio::test]
async fn inline_descriptor_field_round_trips_without_blob_payload_read() {
    let fixture = Fixture::new(true).await;
    let expected = BlobDescriptor::new("s3://example/object".to_string(), 7, -1);
    fixture
        .append(vec![7], vec![Some(expected.serialize())])
        .await;

    let response = fixture
        .service()
        .batch_get(fixture.request(&[7]))
        .await
        .unwrap();
    let actual = response.results[0].blobs["picture"].as_ref().unwrap();
    assert_eq!(actual.uri, "s3://example/object");
    assert_eq!(actual.offset, 7);
    assert_eq!(actual.length, -1);
}

#[tokio::test]
async fn rejects_unindexed_fallback_that_exceeds_the_data_file_budget() {
    let fixture = Fixture::new(false).await;
    fixture
        .append(vec![1], vec![Some(b"payload".to_vec())])
        .await;
    let service = fixture.service_with_budget(QueryBudget {
        max_batch_keys: 10,
        max_planned_files: 10,
        max_planned_bytes: 1,
    });

    let error = service.batch_get(fixture.request(&[1])).await.unwrap_err();
    assert!(matches!(error, LookupError::QueryBudgetExceeded { .. }));
}

#[tokio::test]
async fn caches_table_metadata_and_can_refresh_it_for_readiness() {
    let fixture = Fixture::new(false).await;
    fixture
        .append(vec![1], vec![Some(b"payload".to_vec())])
        .await;
    let service = fixture.service();

    let first = service.batch_get(fixture.request(&[1])).await.unwrap();
    let second = service.batch_get(fixture.request(&[1])).await.unwrap();
    assert!(!first.cache_hit);
    assert!(second.cache_hit);
    assert_eq!(service.table_cache_stats().hits, 1);
    assert_eq!(service.table_cache_stats().misses, 1);
    assert_eq!(service.table_cache_stats().entries, 1);
    assert_eq!(service.descriptor_cache_stats().hits, 1);
    assert_eq!(service.descriptor_cache_stats().misses, 1);

    service.check_ready().await.unwrap();
    assert_eq!(service.table_cache_stats().entries, 1);
}

#[tokio::test]
async fn zero_ttl_disables_table_metadata_cache() {
    let fixture = Fixture::new(false).await;
    fixture
        .append(vec![1], vec![Some(b"payload".to_vec())])
        .await;
    let service = fixture.service_with_options(
        QueryBudget {
            max_batch_keys: 10,
            max_planned_files: 10,
            max_planned_bytes: 16 * 1024 * 1024,
        },
        BlobLookupOptions {
            table_cache_ttl: Duration::ZERO,
            ..BlobLookupOptions::default()
        },
    );

    service.batch_get(fixture.request(&[1])).await.unwrap();
    service.batch_get(fixture.request(&[1])).await.unwrap();
    assert_eq!(service.table_cache_stats().hits, 0);
    assert_eq!(service.table_cache_stats().misses, 2);
    assert_eq!(service.table_cache_stats().entries, 0);
}

#[tokio::test]
async fn zero_limits_disable_descriptor_cache() {
    let fixture = Fixture::new(false).await;
    fixture
        .append(vec![1], vec![Some(b"payload".to_vec())])
        .await;
    let service = fixture.service_with_options(
        QueryBudget {
            max_batch_keys: 10,
            max_planned_files: 10,
            max_planned_bytes: 16 * 1024 * 1024,
        },
        BlobLookupOptions {
            descriptor_cache_ttl: Duration::ZERO,
            ..BlobLookupOptions::default()
        },
    );

    let first = service.batch_get(fixture.request(&[1])).await.unwrap();
    let second = service.batch_get(fixture.request(&[1])).await.unwrap();
    assert!(!first.cache_hit);
    assert!(!second.cache_hit);
    assert_eq!(service.descriptor_cache_stats(), Default::default());
}

#[tokio::test]
async fn disabled_long_term_cache_still_coalesces_concurrent_scans() {
    let fixture = Fixture::new(false).await;
    fixture
        .append(vec![1], vec![Some(b"payload".to_vec())])
        .await;
    let service = fixture.service_with_options(
        QueryBudget {
            max_batch_keys: 10,
            max_planned_files: 10,
            max_planned_bytes: 16 * 1024 * 1024,
        },
        BlobLookupOptions {
            descriptor_cache_ttl: Duration::ZERO,
            ..BlobLookupOptions::default()
        },
    );

    let queries = (0..8).map(|_| {
        let service = service.clone();
        let request = fixture.request(&[1]);
        async move { service.batch_get(request).await.unwrap() }
    });
    let responses = join_all(queries).await;

    assert_eq!(responses.len(), 8);
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.cache_hit)
            .count(),
        7
    );
    assert_eq!(service.descriptor_cache_stats(), Default::default());
}

#[tokio::test]
async fn oversized_descriptor_response_is_not_cached() {
    let fixture = Fixture::new(false).await;
    fixture
        .append(vec![1], vec![Some(b"payload".to_vec())])
        .await;
    let service = fixture.service_with_options(
        QueryBudget {
            max_batch_keys: 10,
            max_planned_files: 10,
            max_planned_bytes: 16 * 1024 * 1024,
        },
        BlobLookupOptions {
            descriptor_cache_max_entry_bytes: 1,
            ..BlobLookupOptions::default()
        },
    );

    let first = service.batch_get(fixture.request(&[1])).await.unwrap();
    let second = service.batch_get(fixture.request(&[1])).await.unwrap();
    assert!(!first.cache_hit);
    assert!(!second.cache_hit);
    assert_eq!(service.descriptor_cache_stats().hits, 0);
    assert_eq!(service.descriptor_cache_stats().misses, 2);
}

#[tokio::test]
async fn concurrent_oversized_cache_misses_share_one_scan() {
    let fixture = Fixture::new(false).await;
    fixture
        .append(vec![1], vec![Some(b"payload".to_vec())])
        .await;
    let service = fixture.service_with_options(
        QueryBudget {
            max_batch_keys: 10,
            max_planned_files: 10,
            max_planned_bytes: 16 * 1024 * 1024,
        },
        BlobLookupOptions {
            descriptor_cache_max_entry_bytes: 1,
            ..BlobLookupOptions::default()
        },
    );

    let queries = (0..8).map(|_| {
        let service = service.clone();
        let request = fixture.request(&[1]);
        async move { service.batch_get(request).await.unwrap() }
    });
    let responses = join_all(queries).await;

    assert_eq!(responses.len(), 8);
    assert_eq!(service.descriptor_cache_stats().misses, 1);
    assert_eq!(service.descriptor_cache_stats().hits, 7);
    assert_eq!(service.descriptor_cache_stats().entries, 0);
}

#[tokio::test]
async fn collapses_concurrent_cold_table_metadata_loads() {
    let fixture = Fixture::new(false).await;
    fixture
        .append(vec![1], vec![Some(b"payload".to_vec())])
        .await;
    let service = fixture.service();

    let queries = (0..8).map(|_| {
        let service = service.clone();
        let request = fixture.request(&[1]);
        async move { service.batch_get(request).await.unwrap() }
    });
    let responses = join_all(queries).await;
    assert_eq!(responses.len(), 8);
    assert_eq!(
        responses
            .iter()
            .filter(|response| response.cache_hit)
            .count(),
        7
    );
    assert_eq!(service.table_cache_stats().misses, 1);
    assert_eq!(service.table_cache_stats().hits, 7);
    assert_eq!(service.descriptor_cache_stats().misses, 1);
    assert_eq!(service.descriptor_cache_stats().hits, 7);
}

#[tokio::test]
async fn rejects_empty_key_batches_before_scanning() {
    let fixture = Fixture::new(false).await;
    let error = fixture
        .service()
        .batch_get(fixture.request(&[]))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        LookupError::InvalidRequest(message) if message.contains("keys must not be empty")
    ));
}

#[tokio::test]
async fn readiness_rejects_global_btree_binary_key_fields() {
    let warehouse = TempDir::new().unwrap();
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        warehouse.path().to_str().unwrap(),
    );
    let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    catalog
        .create_database("db", false, Default::default())
        .await
        .unwrap();
    catalog
        .create_table(
            &Identifier::new("db", "binary_assets"),
            Schema::builder()
                .column(
                    "asset_key",
                    DataType::VarBinary(VarBinaryType::new(32).unwrap()),
                )
                .column("picture", DataType::Blob(BlobType::new()))
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true")
                .option("global-index.enabled", "true")
                .build()
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    let service = BlobLookupService::new(
        catalog,
        [TableLookupPolicy {
            table: TableRef::new("db", "binary_assets"),
            key_fields: vec!["asset_key".to_string()],
            blob_fields: BTreeSet::from(["picture".to_string()]),
            strategy: LookupStrategy::GlobalBtree,
            budget: QueryBudget::default(),
        }],
    )
    .unwrap();

    let error = service.check_ready().await.unwrap_err();
    assert!(matches!(
        error,
        LookupError::InvalidPolicy(message) if message.contains("cannot be indexed")
    ));
}
