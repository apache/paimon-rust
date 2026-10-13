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

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use futures::TryStreamExt;
use paimon::catalog::{Catalog, Database, Identifier, LoadedTable};
use paimon::spec::{Schema as PaimonSchema, SchemaChange, TableType};
use paimon::table::Table;
use paimon::{CatalogOptions, FileSystemCatalog, Options, Result as PaimonResult};
use paimon_datafusion::{EngineTableRequest, SQLContext, TableEngineResolver};
use tempfile::TempDir;

const CATALOG: &str = "cat";
const DB: &str = "shared_db";

fn string_column_values(batches: &[RecordBatch], column: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..values.len())
                .map(|row| values.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Debug)]
struct TypedTestCatalog {
    inner: Arc<FileSystemCatalog>,
    declared_types: HashMap<String, TableType>,
}

#[async_trait]
impl Catalog for TypedTestCatalog {
    async fn list_databases(&self) -> PaimonResult<Vec<String>> {
        self.inner.list_databases().await
    }

    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: HashMap<String, String>,
    ) -> PaimonResult<()> {
        self.inner
            .create_database(name, ignore_if_exists, properties)
            .await
    }

    async fn get_database(&self, name: &str) -> PaimonResult<Database> {
        self.inner.get_database(name).await
    }

    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> PaimonResult<()> {
        self.inner
            .drop_database(name, ignore_if_not_exists, cascade)
            .await
    }

    async fn get_table(&self, identifier: &Identifier) -> PaimonResult<Table> {
        if let Some(declared) = self.declared_types.get(identifier.object()) {
            return Err(paimon::Error::Unsupported {
                message: format!(
                    "table '{}' is declared '{declared}' and cannot be read as a Paimon table",
                    identifier.full_name()
                ),
            });
        }
        self.inner.get_table(identifier).await
    }

    async fn load_table(&self, identifier: &Identifier) -> PaimonResult<LoadedTable> {
        if let Some(declared) = self.declared_types.get(identifier.object()) {
            if declared.requires_table_engine() {
                let options = HashMap::new();
                let fields = vec![paimon::spec::DataField::new(
                    0,
                    "external_id".to_string(),
                    paimon::spec::DataType::Int(paimon::spec::IntType::new()),
                )];
                return LoadedTable::external_with_fields(
                    *declared,
                    fields,
                    &paimon::spec::CoreOptions::new(&options),
                    &identifier.full_name(),
                );
            }
        }
        Ok(LoadedTable::Paimon(Box::new(
            self.get_table(identifier).await?,
        )))
    }

    async fn list_tables(&self, database_name: &str) -> PaimonResult<Vec<String>> {
        let mut names = self.inner.list_tables(database_name).await?;
        names.extend(self.declared_types.keys().cloned());
        Ok(names)
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        creation: PaimonSchema,
        ignore_if_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .create_table(identifier, creation, ignore_if_exists)
            .await
    }

    async fn drop_table(
        &self,
        identifier: &Identifier,
        ignore_if_not_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .drop_table(identifier, ignore_if_not_exists)
            .await
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .rename_table(from, to, ignore_if_not_exists)
            .await
    }

    async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .alter_table(identifier, changes, ignore_if_not_exists)
            .await
    }
}

#[derive(Debug)]
struct LegacyTestCatalog {
    inner: Arc<FileSystemCatalog>,
}

#[async_trait]
impl Catalog for LegacyTestCatalog {
    async fn list_databases(&self) -> PaimonResult<Vec<String>> {
        self.inner.list_databases().await
    }

    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: HashMap<String, String>,
    ) -> PaimonResult<()> {
        self.inner
            .create_database(name, ignore_if_exists, properties)
            .await
    }

    async fn get_database(&self, name: &str) -> PaimonResult<Database> {
        self.inner.get_database(name).await
    }

    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> PaimonResult<()> {
        self.inner
            .drop_database(name, ignore_if_not_exists, cascade)
            .await
    }

    async fn get_table(&self, identifier: &Identifier) -> PaimonResult<Table> {
        let (location, schema) = self.inner.fetch_table_schema(identifier).await?;
        Ok(Table::new(
            self.inner.file_io().clone(),
            identifier.clone(),
            location,
            schema,
            None,
        ))
    }

    async fn list_tables(&self, database_name: &str) -> PaimonResult<Vec<String>> {
        self.inner.list_tables(database_name).await
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        creation: PaimonSchema,
        ignore_if_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .create_table(identifier, creation, ignore_if_exists)
            .await
    }

    async fn drop_table(
        &self,
        identifier: &Identifier,
        ignore_if_not_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .drop_table(identifier, ignore_if_not_exists)
            .await
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .rename_table(from, to, ignore_if_not_exists)
            .await
    }

    async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .alter_table(identifier, changes, ignore_if_not_exists)
            .await
    }
}

#[derive(Debug)]
struct FakeEngineResolver;

#[async_trait]
impl TableEngineResolver for FakeEngineResolver {
    async fn resolve_table(
        &self,
        request: &EngineTableRequest,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if request.table != "it" {
            return Ok(None);
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 3])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .map_err(DataFusionError::from)?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Some(Arc::new(table)))
    }
}

#[derive(Debug, Default)]
struct MetadataEngineResolver {
    calls: AtomicUsize,
}

#[derive(Debug, Default)]
struct SnapshotEngineResolver {
    calls: AtomicUsize,
    current_calls: AtomicUsize,
    current_unavailable: bool,
    timestamps: Mutex<Vec<i64>>,
}

const LARGE_SNAPSHOT_ID: i64 = 6_251_097_304_546_047_273;
/// Local commit times of the fake engine's snapshots 10 and 20.
const SNAPSHOT_10_TIME: &str = "2026-10-01 00:00:00";
const SNAPSHOT_20_TIME: &str = "2026-10-02 00:00:00";
const BACKEND_FAILURE_TIME: &str = "2000-01-01 00:00:00";

fn local_millis(timestamp: &str) -> i64 {
    use chrono::TimeZone;
    let naive = chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d %H:%M:%S").unwrap();
    chrono::Local
        .from_local_datetime(&naive)
        .single()
        .unwrap()
        .timestamp_millis()
}

#[async_trait]
impl TableEngineResolver for SnapshotEngineResolver {
    async fn resolve_table(
        &self,
        request: &EngineTableRequest,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.current_calls.fetch_add(1, Ordering::Relaxed);
        if self.current_unavailable {
            return Err(DataFusionError::Execution(
                "current table is unavailable".into(),
            ));
        }
        FakeEngineResolver.resolve_table(request).await
    }

    async fn resolve_metadata_table(
        &self,
        request: &EngineTableRequest,
        metadata_table: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        MetadataEngineResolver::default()
            .resolve_metadata_table(request, metadata_table)
            .await
    }

    async fn resolve_table_snapshot(
        &self,
        request: &EngineTableRequest,
        snapshot_id: i64,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        assert_eq!(request.database, DB);
        assert_eq!(request.declared, TableType::IcebergTable);
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        if request.table != "it" {
            return Ok(None);
        }
        let (ids, values) = match snapshot_id {
            10 => (vec![1, 2], vec!["old-a", "old-b"]),
            20 => (vec![1, 4], vec!["new-a", "new-d"]),
            LARGE_SNAPSHOT_ID => (vec![9], vec!["large-id"]),
            500 => return Err(DataFusionError::Execution("snapshot backend failed".into())),
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "snapshot {snapshot_id} does not exist or has expired"
                )));
            }
        };
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("historical", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(values)),
            ],
        )?;
        Ok(Some(Arc::new(MemTable::try_new(
            schema,
            vec![vec![batch]],
        )?)))
    }

    async fn resolve_table_at_timestamp(
        &self,
        request: &EngineTableRequest,
        timestamp_millis: i64,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.timestamps.lock().unwrap().push(timestamp_millis);
        // Pick the latest snapshot committed at or before the timestamp.
        let snapshot_id = if timestamp_millis == local_millis(BACKEND_FAILURE_TIME) {
            500
        } else if timestamp_millis >= local_millis(SNAPSHOT_20_TIME) {
            20
        } else if timestamp_millis >= local_millis(SNAPSHOT_10_TIME) {
            10
        } else {
            return Err(DataFusionError::Plan(format!(
                "no snapshot exists at or before {timestamp_millis}"
            )));
        };
        self.resolve_table_snapshot(request, snapshot_id).await
    }
}

#[async_trait]
impl TableEngineResolver for MetadataEngineResolver {
    async fn resolve_table(
        &self,
        request: &EngineTableRequest,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        FakeEngineResolver.resolve_table(request).await
    }

    async fn resolve_metadata_table(
        &self,
        request: &EngineTableRequest,
        metadata_table: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        assert_eq!(request.database, DB);
        assert_eq!(request.declared, TableType::IcebergTable);
        if request.table != "it" {
            return Ok(None);
        }
        match metadata_table {
            "snapshots" | "manifests" | "history" => {}
            "broken" => {
                return Err(DataFusionError::Execution(
                    "metadata backend exploded".to_string(),
                ));
            }
            _ => return Ok(None),
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("snapshot_id", DataType::Int32, false),
            Field::new("operation", DataType::Utf8, false),
            Field::new("metadata_table", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec!["append", "overwrite"])),
                Arc::new(StringArray::from(vec![metadata_table; 2])),
            ],
        )?;
        Ok(Some(Arc::new(MemTable::try_new(
            schema,
            vec![vec![batch]],
        )?)))
    }
}

struct TestEnv {
    _paimon_dir: TempDir,
    ctx: SQLContext,
    catalog: Arc<FileSystemCatalog>,
}

async fn setup() -> TestEnv {
    setup_with_engine(Some(Arc::new(FakeEngineResolver))).await
}

async fn setup_with_engine(resolver: Option<Arc<dyn TableEngineResolver>>) -> TestEnv {
    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let typed_catalog = Arc::new(TypedTestCatalog {
        inner: Arc::clone(&fs_catalog),
        declared_types: HashMap::from([
            ("it".to_string(), TableType::IcebergTable),
            ("ghost".to_string(), TableType::IcebergTable),
            ("ft".to_string(), TableType::IcebergTable),
        ]),
    });
    let mut ctx = SQLContext::new();
    ctx.register_catalog(CATALOG, typed_catalog).await.unwrap();
    ctx.sql(&format!("CREATE SCHEMA {CATALOG}.{DB}"))
        .await
        .unwrap();
    ctx.sql(&format!(
        "CREATE TABLE {CATALOG}.{DB}.pt (id INT NOT NULL, name STRING)"
    ))
    .await
    .unwrap();
    for stmt in [
        format!("INSERT INTO {CATALOG}.{DB}.pt VALUES (1, 'a')"),
        format!("INSERT INTO {CATALOG}.{DB}.pt VALUES (2, 'b')"),
    ] {
        ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
    }

    if let Some(resolver) = resolver {
        ctx.register_catalog_table_engine(CATALOG, TableType::IcebergTable, resolver)
            .unwrap();
    }

    TestEnv {
        _paimon_dir: paimon_dir,
        ctx,
        catalog: fs_catalog,
    }
}

fn column_i32(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

async fn enable_version_syntax(env: &TestEnv) {
    env.ctx
        .ctx()
        .sql("SET datafusion.sql_parser.dialect = 'databricks'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

async fn snapshot_env() -> (TestEnv, Arc<SnapshotEngineResolver>) {
    let resolver = Arc::new(SnapshotEngineResolver::default());
    let env = setup_with_engine(Some(resolver.clone())).await;
    enable_version_syntax(&env).await;
    (env, resolver)
}

async fn routed_sql(
    env: &TestEnv,
    native: bool,
    sql: &str,
) -> DFResult<datafusion::dataframe::DataFrame> {
    if native {
        env.ctx.ctx().sql(sql).await
    } else {
        env.ctx.sql(sql).await
    }
}

#[tokio::test]
async fn engine_snapshot_uses_historical_data_and_schema() {
    let (env, _) = snapshot_env().await;
    for native in [false, true] {
        let batches = routed_sql(
            &env,
            native,
            &format!(
                "SELECT h.historical FROM {CATALOG}.{DB}.it VERSION AS OF 10 AS h WHERE h.id = 2"
            ),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        assert_eq!(string_column_values(&batches, "historical"), ["old-b"]);

        let err = routed_sql(
            &env,
            native,
            &format!("SELECT payload FROM {CATALOG}.{DB}.it VERSION AS OF 10"),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("payload"), "{err}");

        let batches = routed_sql(
            &env,
            native,
            &format!("SELECT payload FROM {CATALOG}.{DB}.it ORDER BY id"),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        assert_eq!(string_column_values(&batches, "payload"), ["x", "y"]);

        let mut versions = vec![LARGE_SNAPSHOT_ID.to_string()];
        if !native {
            versions.push(format!("'{LARGE_SNAPSHOT_ID}'"));
        }
        for version in versions {
            let batches = routed_sql(
                &env,
                native,
                &format!("SELECT historical FROM {CATALOG}.{DB}.it VERSION AS OF {version}"),
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
            assert_eq!(string_column_values(&batches, "historical"), ["large-id"]);
        }
    }
}

#[tokio::test]
async fn engine_timestamp_passes_exact_millis_and_uses_the_resolved_snapshot() {
    let (env, resolver) = snapshot_env().await;
    for native in [false, true] {
        for (timestamp, expected) in [
            (SNAPSHOT_10_TIME, ["old-a", "old-b"]),
            ("2026-10-01 23:59:59", ["old-a", "old-b"]),
            (SNAPSHOT_20_TIME, ["new-a", "new-d"]),
            ("2026-10-03 00:00:00", ["new-a", "new-d"]),
        ] {
            resolver.timestamps.lock().unwrap().clear();
            let batches = routed_sql(
                &env,
                native,
                &format!(
                    "SELECT h.historical FROM {CATALOG}.{DB}.it TIMESTAMP AS OF '{timestamp}' AS h \
                     ORDER BY h.id"
                ),
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
            assert_eq!(
                string_column_values(&batches, "historical"),
                expected,
                "{timestamp}"
            );
            assert_eq!(
                *resolver.timestamps.lock().unwrap(),
                [local_millis(timestamp)]
            );
        }

        let err = routed_sql(
            &env,
            native,
            &format!("SELECT payload FROM {CATALOG}.{DB}.it TIMESTAMP AS OF '{SNAPSHOT_20_TIME}'"),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("payload"), "{err}");
    }
}

#[tokio::test]
async fn sql_context_snapshot_remains_readable_when_current_resolution_fails() {
    let (env, resolver) = current_unavailable_env().await;
    for selector in [
        "VERSION AS OF 10".to_string(),
        format!("TIMESTAMP AS OF '{SNAPSHOT_10_TIME}'"),
    ] {
        let batches = env
            .ctx
            .sql(&format!(
                "SELECT h.historical FROM {CATALOG}.{DB}.it {selector} AS h WHERE h.id = 2"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(string_column_values(&batches, "historical"), ["old-b"]);
    }

    let err = env
        .ctx
        .sql(&format!(
            "SELECT * FROM {CATALOG}.{DB}.it VERSION AS OF 404 WHERE FALSE"
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("snapshot 404"), "{err}");
    assert_eq!(resolver.current_calls.load(Ordering::Relaxed), 0);

    for native in [false, true] {
        for query in [
            format!("SELECT * FROM {CATALOG}.{DB}.it"),
            format!("SELECT * FROM {CATALOG}.{DB}.it WHERE FALSE"),
            format!("SELECT COUNT(*) FROM {CATALOG}.{DB}.it LIMIT 0"),
            format!(
                "SELECT h.historical FROM {CATALOG}.{DB}.it VERSION AS OF 10 AS h \
                 JOIN {CATALOG}.{DB}.it AS c ON h.id = c.id"
            ),
        ] {
            let err = routed_sql(&env, native, &query).await.unwrap_err();
            assert!(
                err.to_string().contains("current table is unavailable"),
                "{err}"
            );
        }
    }
}

#[tokio::test]
async fn raw_session_snapshot_propagates_current_resolution_errors() {
    let (env, resolver) = current_unavailable_env().await;
    for selector in [
        "VERSION AS OF 10".to_string(),
        format!("TIMESTAMP AS OF '{SNAPSHOT_10_TIME}'"),
    ] {
        for suffix in ["", " WHERE FALSE", " LIMIT 0"] {
            let err = env
                .ctx
                .ctx()
                .sql(&format!(
                    "SELECT * FROM {CATALOG}.{DB}.it {selector}{suffix}"
                ))
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("current table is unavailable"),
                "{err}"
            );
        }
    }
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 0);
    assert!(resolver.timestamps.lock().unwrap().is_empty());
}

async fn current_unavailable_env() -> (TestEnv, Arc<SnapshotEngineResolver>) {
    let resolver = Arc::new(SnapshotEngineResolver {
        current_unavailable: true,
        ..Default::default()
    });
    let env = setup_with_engine(Some(resolver.clone())).await;
    enable_version_syntax(&env).await;
    (env, resolver)
}

#[tokio::test]
async fn engine_errors_propagate_to_schema_table() {
    let (env, _) = current_unavailable_env().await;
    let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
    let err = schema.table("it").await.map(|_| ()).unwrap_err();
    assert!(
        err.to_string().contains("current table is unavailable"),
        "{err}"
    );
}

#[tokio::test]
async fn engine_errors_propagate_to_context_table_provider() {
    let (env, _) = current_unavailable_env().await;
    let err = env
        .ctx
        .ctx()
        .table_provider(format!("{CATALOG}.{DB}.it"))
        .await
        .map(|_| ())
        .unwrap_err();
    assert!(
        err.to_string().contains("current table is unavailable"),
        "{err}"
    );
}

#[tokio::test]
async fn engine_errors_propagate_to_context_table() {
    let (env, _) = current_unavailable_env().await;
    let err = env
        .ctx
        .ctx()
        .table(format!("{CATALOG}.{DB}.it"))
        .await
        .map(|_| ())
        .unwrap_err();
    assert!(
        err.to_string().contains("current table is unavailable"),
        "{err}"
    );
}

#[tokio::test]
async fn engine_snapshots_can_be_joined_with_each_other_and_paimon() {
    let (env, resolver) = snapshot_env().await;
    for native in [false, true] {
        let batches = routed_sql(
            &env,
            native,
            &format!(
                "SELECT a.historical AS old_value, b.historical AS new_value \
             FROM {CATALOG}.{DB}.it VERSION AS OF 10 AS a \
             JOIN {CATALOG}.{DB}.it VERSION AS OF 20 AS b ON a.id = b.id"
            ),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        assert_batches_eq!(
            [
                "+-----------+-----------+",
                "| old_value | new_value |",
                "+-----------+-----------+",
                "| old-a     | new-a     |",
                "+-----------+-----------+",
            ],
            &batches
        );

        let batches = routed_sql(
            &env,
            native,
            &format!(
                "SELECT h.historical FROM {CATALOG}.{DB}.it VERSION AS OF 10 AS h \
             JOIN {CATALOG}.{DB}.pt VERSION AS OF 1 AS p ON h.id = p.id"
            ),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        assert_eq!(string_column_values(&batches, "historical"), ["old-a"]);

        let batches = routed_sql(
            &env,
            native,
            &format!(
                "SELECT a.historical AS old_value, b.historical AS new_value \
             FROM {CATALOG}.{DB}.it TIMESTAMP AS OF '{SNAPSHOT_10_TIME}' AS a \
             JOIN {CATALOG}.{DB}.it VERSION AS OF 20 AS b ON a.id = b.id"
            ),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        assert_eq!(string_column_values(&batches, "old_value"), ["old-a"]);
        assert_eq!(string_column_values(&batches, "new_value"), ["new-a"]);

        // Paimon tables keep Paimon's own timestamp resolution.
        resolver.timestamps.lock().unwrap().clear();
        let batches = routed_sql(
            &env,
            native,
            &format!(
                "SELECT p.id, h.historical \
             FROM {CATALOG}.{DB}.it TIMESTAMP AS OF '{SNAPSHOT_10_TIME}' AS h \
             JOIN {CATALOG}.{DB}.pt TIMESTAMP AS OF '2999-01-01 00:00:00' AS p ON h.id = p.id \
             ORDER BY h.id"
            ),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        assert_eq!(
            string_column_values(&batches, "historical"),
            ["old-a", "old-b"]
        );
        assert_eq!(column_i32(&batches), [1, 2]);
        assert_eq!(
            *resolver.timestamps.lock().unwrap(),
            [local_millis(SNAPSHOT_10_TIME)]
        );
    }
}

#[tokio::test]
async fn engine_snapshot_supports_count_and_empty_results() {
    let (env, _) = snapshot_env().await;
    for native in [false, true] {
        let batches = routed_sql(
            &env,
            native,
            &format!("SELECT COUNT(*) AS n FROM {CATALOG}.{DB}.it VERSION AS OF 10"),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
        assert_batches_eq!(["+---+", "| n |", "+---+", "| 2 |", "+---+"], &batches);

        let df = routed_sql(
            &env,
            native,
            &format!("SELECT historical FROM {CATALOG}.{DB}.it VERSION AS OF 10 WHERE FALSE"),
        )
        .await
        .unwrap();
        assert_eq!(df.schema().field(0).name(), "historical");
        assert_eq!(
            df.collect()
                .await
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            0
        );
    }
}

#[tokio::test]
async fn engine_snapshot_errors_never_fall_back_to_current_data() {
    let (env, _) = snapshot_env().await;
    for native in [false, true] {
        for (table, selector, message) in [
            (
                "it",
                "VERSION AS OF 404".to_string(),
                "snapshot 404 does not exist or has expired",
            ),
            (
                "it",
                "VERSION AS OF 500".to_string(),
                "snapshot backend failed",
            ),
            ("ghost", "VERSION AS OF 10".to_string(), "not found"),
            (
                "it",
                "TIMESTAMP AS OF '2026-09-30 23:59:59'".to_string(),
                "no snapshot exists at or before",
            ),
            (
                "it",
                format!("TIMESTAMP AS OF '{BACKEND_FAILURE_TIME}'"),
                "snapshot backend failed",
            ),
            (
                "ghost",
                format!("TIMESTAMP AS OF '{SNAPSHOT_10_TIME}'"),
                "not found",
            ),
        ] {
            let err = routed_sql(
                &env,
                native,
                &format!("SELECT * FROM {CATALOG}.{DB}.{table} {selector} WHERE FALSE"),
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains(message), "{selector}: {err}");
        }
    }
}

#[tokio::test]
async fn engine_snapshot_providers_remain_read_only() {
    use datafusion::catalog::default_table_source::source_as_provider;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::{dml::InsertOp, LogicalPlan};
    use datafusion::physical_plan::empty::EmptyExec;

    let (env, _) = snapshot_env().await;
    for native in [false, true] {
        for selector in [
            "VERSION AS OF 10".to_string(),
            format!("TIMESTAMP AS OF '{SNAPSHOT_10_TIME}'"),
        ] {
            let df = routed_sql(
                &env,
                native,
                &format!("SELECT * FROM {CATALOG}.{DB}.it {selector}"),
            )
            .await
            .unwrap();
            let mut provider = None;
            df.logical_plan()
                .apply(|node| {
                    if let LogicalPlan::TableScan(scan) = node {
                        provider = Some(source_as_provider(&scan.source)?);
                        return Ok(TreeNodeRecursion::Stop);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
            let provider = provider.unwrap();
            let err = provider
                .insert_into(
                    &env.ctx.ctx().state(),
                    Arc::new(EmptyExec::new(provider.schema())),
                    InsertOp::Append,
                )
                .await
                .unwrap_err();
            assert!(err.to_string().contains("write is not supported"), "{err}");
        }
    }
}

#[tokio::test]
async fn engine_snapshot_resolution_checks_session_options() {
    let (env, resolver) = snapshot_env().await;
    for (key, value, message) in [
        ("query-auth.enabled", "true", "query-auth"),
        ("scan.version", "20", "time travel"),
        ("incremental-between", "1,5", "incremental-between"),
    ] {
        env.ctx
            .sql(&format!("SET 'paimon.{key}' = '{value}'"))
            .await
            .unwrap();
        for native in [false, true] {
            for selector in [
                "VERSION AS OF 10".to_string(),
                format!("TIMESTAMP AS OF '{SNAPSHOT_10_TIME}'"),
            ] {
                let err = routed_sql(
                    &env,
                    native,
                    &format!("SELECT * FROM {CATALOG}.{DB}.it {selector}"),
                )
                .await
                .unwrap_err();
                assert!(err.to_string().contains(message), "{selector}: {err}");
                assert_eq!(resolver.calls.load(Ordering::Relaxed), 0);
                assert!(resolver.timestamps.lock().unwrap().is_empty());
            }
        }
        env.ctx.sql(&format!("RESET 'paimon.{key}'")).await.unwrap();
    }
}

#[tokio::test]
async fn engine_snapshot_rejects_unsupported_selectors_and_metadata_tables() {
    let (env, resolver) = snapshot_env().await;
    for native in [false, true] {
        let (selector, message) = if native {
            (
                "FOR SYSTEM_TIME AS OF '2020-01-01 00:00:00'",
                "time travel is not supported",
            )
        } else {
            ("VERSION AS OF 'tag-name'", "integer snapshot id")
        };
        let timestamp = format!("TIMESTAMP AS OF '{SNAPSHOT_10_TIME}'");
        let timestamp = timestamp.as_str();
        for (table, selector, message) in [
            ("it", selector, message),
            (
                "it",
                "VERSION AS OF 9223372036854775808",
                "integer snapshot id",
            ),
            (
                "it",
                "TIMESTAMP AS OF '2026-10-01'",
                "Cannot parse time travel timestamp",
            ),
            (
                "it$history",
                "VERSION AS OF 10",
                "time travel is not supported",
            ),
            ("it$history", timestamp, "time travel is not supported"),
            (
                "it$branch_b1",
                "VERSION AS OF 10",
                "branches are not supported",
            ),
            ("it$branch_b1", timestamp, "branches are not supported"),
        ] {
            let err = routed_sql(
                &env,
                native,
                &format!("SELECT * FROM {CATALOG}.{DB}.\"{table}\" {selector}"),
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains(message), "{selector}: {err}");
            assert_eq!(resolver.calls.load(Ordering::Relaxed), 0);
            assert!(resolver.timestamps.lock().unwrap().is_empty());
        }
    }
}

#[tokio::test]
async fn engine_snapshot_requires_explicit_resolver_support() {
    for registered in [false, true] {
        let resolver =
            registered.then(|| Arc::new(FakeEngineResolver) as Arc<dyn TableEngineResolver>);
        let env = setup_with_engine(resolver).await;
        enable_version_syntax(&env).await;
        for native in [false, true] {
            for selector in [
                "VERSION AS OF 10".to_string(),
                format!("TIMESTAMP AS OF '{SNAPSHOT_10_TIME}'"),
            ] {
                let err = routed_sql(
                    &env,
                    native,
                    &format!("SELECT * FROM {CATALOG}.{DB}.it {selector} WHERE FALSE"),
                )
                .await
                .unwrap_err();
                let expected = if registered {
                    "time travel is not supported"
                } else {
                    "no table engine is registered"
                };
                assert!(err.to_string().contains(expected), "{selector}: {err}");
            }
        }
    }
}

#[test]
fn engine_snapshot_resolution_on_runtime_workers() {
    let runtime = paimon_datafusion::runtime::runtime();
    runtime.block_on(async {
        let (env, _) = snapshot_env().await;
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            tokio::spawn(async move {
                let batches = routed_sql(
                    &env,
                    true,
                    &format!(
                        "SELECT historical FROM {CATALOG}.{DB}.it VERSION AS OF 10 ORDER BY id"
                    ),
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
                assert_eq!(
                    string_column_values(&batches, "historical"),
                    ["old-a", "old-b"]
                );
            }),
        )
        .await
        .expect("snapshot resolution stalled on a runtime worker")
        .unwrap();
    });
}

#[tokio::test]
async fn paimon_path_still_serves_paimon_tables() {
    let env = setup().await;
    let batches = env
        .ctx
        .sql(&format!("SELECT id FROM {CATALOG}.{DB}.pt ORDER BY id"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_i32(&batches), vec![1, 2]);
}

#[tokio::test]
async fn declared_engine_table_routes_to_engine() {
    let env = setup().await;
    let df = env
        .ctx
        .sql(&format!("SELECT id, payload FROM {CATALOG}.{DB}.it"))
        .await
        .unwrap();
    let names: Vec<String> = df
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["id".to_string(), "payload".to_string()]);
    let batches = df.collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn cross_engine_join_plans_and_runs() {
    let env = setup().await;
    let batches = env
        .ctx
        .sql(&format!(
            "SELECT p.id FROM {CATALOG}.{DB}.pt p JOIN {CATALOG}.{DB}.it i ON p.id = i.id"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_i32(&batches), vec![1]);
}

#[tokio::test]
async fn missing_table_still_errors() {
    let env = setup().await;
    let Err(err) = env
        .ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.does_not_exist"))
        .await
    else {
        panic!("query against a missing table must fail");
    };
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("does_not_exist") || msg.contains("not found"),
        "{msg}"
    );
}

#[tokio::test]
async fn time_travel_still_works_with_engines_registered() {
    let env = setup().await;
    let batches = env
        .ctx
        .sql(&format!("SELECT id FROM {CATALOG}.{DB}.pt VERSION AS OF 1"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_i32(&batches), vec![1]);
}

#[tokio::test]
async fn temp_tables_still_work_with_engines_registered() {
    let env = setup().await;
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
    env.ctx
        .register_temp_table(format!("{CATALOG}.{DB}.tmp_t"), Arc::new(mem))
        .expect("temp table registration must survive engine registration");
    assert!(env
        .ctx
        .temp_table_exist(format!("{CATALOG}.{DB}.tmp_t"))
        .unwrap());
}

#[tokio::test]
async fn schemas_and_ddl_behave_with_engines_registered() {
    let env = setup().await;
    let provider = env.ctx.ctx().catalog(CATALOG).unwrap();
    assert!(provider.schema(DB).is_some());
    assert!(provider.schema("no_such_db").is_none());
    env.ctx
        .sql(&format!("CREATE SCHEMA {CATALOG}.fresh_db"))
        .await
        .expect("CREATE SCHEMA must work with engines registered");
    env.ctx
        .sql(&format!(
            "CREATE TABLE {CATALOG}.fresh_db.t (id INT NOT NULL)"
        ))
        .await
        .expect("CREATE TABLE in the fresh schema must work");
}

#[tokio::test]
async fn table_names_come_from_the_catalog_listing() {
    let env = setup().await;
    let provider = env.ctx.ctx().catalog(CATALOG).unwrap();
    let schema = provider.schema(DB).unwrap();
    let mut names = schema.table_names();
    names.sort();
    names.dedup();
    assert!(names.contains(&"pt".to_string()), "{names:?}");
    assert!(names.contains(&"it".to_string()), "{names:?}");
}

#[derive(Debug)]
struct BrokenResolver;

#[async_trait]
impl TableEngineResolver for BrokenResolver {
    async fn resolve_table(
        &self,
        _request: &EngineTableRequest,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "engine backend exploded".to_string(),
        ))
    }
}

#[tokio::test]
async fn engine_errors_are_surfaced() {
    let env = setup().await;
    env.ctx
        .register_catalog_table_engine(CATALOG, TableType::IcebergTable, Arc::new(BrokenResolver))
        .unwrap();
    let Err(err) = env
        .ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it"))
        .await
    else {
        panic!("broken engine must fail loudly");
    };
    let msg = err.to_string();
    assert!(msg.contains("engine backend exploded"), "{msg}");
}

#[tokio::test]
async fn paimon_served_type_takes_paimon_path() {
    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let typed_catalog = Arc::new(TypedTestCatalog {
        inner: fs_catalog,
        declared_types: HashMap::from([("mt".to_string(), TableType::MaterializedTable)]),
    });
    let mut ctx = SQLContext::new();
    ctx.register_catalog(CATALOG, typed_catalog).await.unwrap();
    ctx.sql(&format!("CREATE SCHEMA {CATALOG}.{DB}"))
        .await
        .unwrap();
    let Err(err) = ctx.sql(&format!("SELECT * FROM {CATALOG}.{DB}.mt")).await else {
        panic!("a Paimon-served type must fall through to Paimon");
    };
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("mt") || msg.contains("not found"), "{msg}");
}

#[tokio::test]
async fn writes_to_routed_tables_fail_closed() {
    let env = setup().await;
    let Err(err) = env
        .ctx
        .sql(&format!("UPDATE {CATALOG}.{DB}.ft SET id = 1"))
        .await
    else {
        panic!("UPDATE on a routed table must fail");
    };
    let msg = err.to_string();
    assert!(msg.contains("cannot be read as a Paimon table"), "{msg}");
    assert!(msg.contains(TableType::IcebergTable.as_str()), "{msg}");
}

#[tokio::test]
async fn metadata_tables_without_engine_support_are_unavailable() {
    let env = setup().await;
    let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
    for name in ["it$snapshots", "it$history"] {
        assert!(schema.table(name).await.unwrap().is_none(), "{name}");
        assert!(!schema.table_exist(name), "{name}");
        let err = env
            .ctx
            .sql(&format!("SELECT * FROM {CATALOG}.{DB}.\"{name}\""))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not found"), "{err}");
    }
}

#[tokio::test]
async fn metadata_tables_without_a_registered_engine_report_the_declared_type() {
    let env = setup_with_engine(None).await;
    let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
    for name in ["it$snapshots", "it$history"] {
        assert!(!schema.table_exist(name), "{name}");
        let err = schema.table(name).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("no table engine is registered for 'iceberg-table'"),
            "{err}"
        );
    }
}

#[tokio::test]
async fn engine_metadata_tables_support_projection_filters_and_aggregation() {
    let env = setup_with_engine(Some(Arc::new(MetadataEngineResolver::default()))).await;
    for suffix in ["snapshots", "manifests", "History"] {
        let batches = env
            .ctx
            .sql(&format!(
                "SELECT operation, snapshot_id FROM {CATALOG}.{DB}.\"it${suffix}\" \
                 WHERE snapshot_id > 2"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_eq!(
            [
                "+-----------+-------------+",
                "| operation | snapshot_id |",
                "+-----------+-------------+",
                "| overwrite | 4           |",
                "+-----------+-------------+",
            ],
            &batches
        );
        let batches = env
            .ctx
            .sql(&format!(
                "SELECT metadata_table FROM {CATALOG}.{DB}.\"it${suffix}\" \
                 WHERE snapshot_id > 2"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            string_column_values(&batches, "metadata_table"),
            vec![suffix.to_ascii_lowercase()]
        );
        let batches = env
            .ctx
            .sql(&format!(
                "SELECT count(*) AS n FROM {CATALOG}.{DB}.\"it${suffix}\""
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_batches_eq!(["+---+", "| n |", "+---+", "| 2 |", "+---+"], &batches);
    }
}

#[tokio::test]
async fn metadata_table_existence_matches_the_resolver() {
    let env = setup_with_engine(Some(Arc::new(MetadataEngineResolver::default()))).await;
    let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
    for (name, exists) in [
        ("it$snapshots", true),
        ("it$history", true),
        ("it$SNAPSHOTS", true),
        ("it$History", true),
        ("it$unknown", false),
        ("ghost$history", false),
        ("missing$history", false),
        ("pt$history", false),
    ] {
        assert_eq!(
            schema.table(name).await.unwrap().is_some(),
            exists,
            "{name}"
        );
        assert_eq!(schema.table_exist(name), exists, "{name}");
    }
}

#[tokio::test]
async fn metadata_engine_errors_are_surfaced() {
    let env = setup_with_engine(Some(Arc::new(MetadataEngineResolver::default()))).await;
    let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
    assert!(schema.table_exist("it$broken"));
    let err = env
        .ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.\"it$broken\""))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("metadata backend exploded"),
        "{err}"
    );
}

#[tokio::test]
async fn paimon_system_tables_keep_the_paimon_provider() {
    let env = setup_with_engine(Some(Arc::new(MetadataEngineResolver::default()))).await;
    let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
    assert!(schema.table_exist("pt$snapshots"));
    let batches = env
        .ctx
        .sql(&format!(
            "SELECT snapshot_id FROM {CATALOG}.{DB}.\"pt$snapshots\" ORDER BY snapshot_id"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_batches_eq!(
        [
            "+-------------+",
            "| snapshot_id |",
            "+-------------+",
            "| 1           |",
            "| 2           |",
            "+-------------+",
        ],
        &batches
    );
}

#[tokio::test]
async fn routed_metadata_tables_are_read_only() {
    let env = setup_with_engine(Some(Arc::new(MetadataEngineResolver::default()))).await;
    let err = async {
        env.ctx
            .sql(&format!(
                "INSERT INTO {CATALOG}.{DB}.\"it$history\" VALUES (5, 'append', 'history')"
            ))
            .await?
            .collect()
            .await
    }
    .await
    .unwrap_err();
    assert!(err.to_string().contains("write is not supported"), "{err}");
}

#[tokio::test]
async fn routed_metadata_tables_are_views() {
    use datafusion::datasource::TableType as DFTableType;

    let env = setup_with_engine(Some(Arc::new(MetadataEngineResolver::default()))).await;
    let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
    let metadata = schema.table("it$history").await.unwrap().unwrap();
    assert_eq!(metadata.table_type(), DFTableType::View);
    let base = schema.table("it").await.unwrap().unwrap();
    assert_eq!(base.table_type(), DFTableType::Base);

    // Native DROP TABLE must reject metadata views.
    let result = async {
        env.ctx
            .ctx()
            .sql(&format!("DROP TABLE {CATALOG}.{DB}.\"it$history\""))
            .await?
            .collect()
            .await
    }
    .await;
    assert!(result.is_err(), "{result:?}");
    assert!(schema.table_exist("it$history"));
}

#[tokio::test]
async fn metadata_drop_view_preserves_physical_name_collisions() {
    for name in ["it$history", "it$History", "pt$snapshots"] {
        assert_metadata_ddl_preserves_physical_table(
            name,
            &format!("DROP VIEW {CATALOG}.{DB}.\"{name}\""),
        )
        .await;
    }
}

#[tokio::test]
async fn metadata_replace_view_preserves_physical_name_collisions() {
    for name in ["it$history", "it$History", "pt$snapshots"] {
        assert_metadata_ddl_preserves_physical_table(
            name,
            &format!("CREATE OR REPLACE VIEW {CATALOG}.{DB}.\"{name}\" AS SELECT 42 AS id"),
        )
        .await;
    }
}

async fn assert_metadata_ddl_preserves_physical_table(name: &str, ddl: &str) {
    let env = setup_with_engine(Some(Arc::new(MetadataEngineResolver::default()))).await;
    env.ctx
        .sql(&format!(
            "CREATE TABLE {CATALOG}.{DB}.\"{name}\" (id INT NOT NULL)"
        ))
        .await
        .unwrap();
    let identifier = Identifier::new(DB, name);
    let table = env.catalog.get_table(&identifier).await.unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![7, 9]))],
    )
    .unwrap();
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    // Use native DDL to bypass SQLContext guards.
    let result = async { env.ctx.ctx().sql(ddl).await?.collect().await }.await;
    assert!(
        table.file_io().exists_dir(table.location()).await.unwrap(),
        "{ddl} deleted the physical table directory"
    );
    let remaining = env.catalog.get_table(&identifier).await.unwrap();
    let read = remaining.new_read_builder();
    let plan = read.new_scan().plan().await.unwrap();
    let batches = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(column_i32(&batches), vec![7, 9], "{ddl}");
    let error = result.unwrap_err().to_string();
    assert!(error.contains(name), "{ddl}: {error}");
}

#[tokio::test]
async fn metadata_deregistration_reports_read_only() {
    let env = setup_with_engine(Some(Arc::new(MetadataEngineResolver::default()))).await;
    let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
    for name in ["it$history", "it$History", "it$unknown", "pt$snapshots"] {
        let error = schema.deregister_table(name).unwrap_err().to_string();
        assert!(error.contains("read-only"), "{name}: {error}");
        assert!(error.contains(&format!("{DB}.{name}")), "{name}: {error}");
    }
}

#[tokio::test]
async fn native_drop_table_still_removes_a_regular_table() {
    let env = setup().await;
    let identifier = Identifier::new(DB, "pt");
    let table = env.catalog.get_table(&identifier).await.unwrap();
    env.ctx
        .ctx()
        .sql(&format!("DROP TABLE {CATALOG}.{DB}.pt"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(!table.file_io().exists_dir(table.location()).await.unwrap());
    assert!(matches!(
        env.catalog.get_table(&identifier).await,
        Err(paimon::Error::TableNotExist { .. })
    ));
}

#[tokio::test]
async fn routed_metadata_tables_reject_branches_and_time_travel() {
    let env = setup_with_engine(Some(Arc::new(MetadataEngineResolver::default()))).await;
    let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
    let err = schema.table("it$branch_b1$history").await.unwrap_err();
    assert!(
        err.to_string().contains("branches are not supported"),
        "{err}"
    );
    assert!(!schema.table_exist("it$branch_b1$history"));

    env.ctx
        .ctx()
        .sql("SET datafusion.sql_parser.dialect = 'databricks'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    for clause in [
        "VERSION AS OF 1",
        "TIMESTAMP AS OF '2020-01-01 00:00:00'",
        "FOR SYSTEM_TIME AS OF '2020-01-01 00:00:00'",
    ] {
        let err = env
            .ctx
            .ctx()
            .sql(&format!(
                "SELECT * FROM {CATALOG}.{DB}.\"it$history\" {clause}"
            ))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("time travel is not supported"),
            "{err}"
        );
    }
}

#[tokio::test]
async fn metadata_resolution_enforces_session_options_before_calling_the_engine() {
    for (option, value, error) in [
        ("query-auth.enabled", "true", "query-auth"),
        ("scan.version", "1", "time travel"),
        ("incremental-between", "1,5", "incremental-between"),
    ] {
        let resolver = Arc::new(MetadataEngineResolver::default());
        let env = setup_with_engine(Some(resolver.clone())).await;
        env.ctx
            .sql(&format!("SET 'paimon.{option}' = '{value}'"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let schema = env.ctx.ctx().catalog(CATALOG).unwrap().schema(DB).unwrap();
        let err = schema.table("it$history").await.unwrap_err();
        assert!(err.to_string().contains(error), "{err}");
        if option == "scan.version" {
            assert!(err.to_string().contains("it$history"), "{err}");
        }
        assert!(schema.table_exist("it$history"));
        assert_eq!(resolver.calls.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn table_exist_mirrors_the_resolver() {
    let env = setup().await;
    let provider = env.ctx.ctx().catalog(CATALOG).unwrap();
    let schema = provider.schema(DB).unwrap();
    assert!(schema.table_exist("it"));
    assert!(!schema.table_exist("ghost"));
    assert!(!schema.table_exist("it$snapshots"));
}

#[tokio::test]
async fn paimon_managed_types_cannot_be_routed() {
    let env = setup().await;
    let Err(err) = env.ctx.register_catalog_table_engine(
        CATALOG,
        TableType::FormatTable,
        Arc::new(FakeEngineResolver),
    ) else {
        panic!("registering an engine for a Paimon-managed type must fail");
    };
    let msg = err.to_string();
    assert!(msg.contains("served by the Paimon reader"), "{msg}");
}

#[tokio::test]
async fn insert_into_routed_table_is_rejected() {
    let env = setup().await;
    let err = async {
        env.ctx
            .sql(&format!("INSERT INTO {CATALOG}.{DB}.it VALUES (9, 'z')"))
            .await?
            .collect()
            .await
    }
    .await
    .expect_err("insert into a routed table must fail");
    assert!(
        err.to_string()
            .contains("write is not supported for routed 'iceberg-table' tables"),
        "{err}"
    );
}

#[tokio::test]
async fn object_and_lance_tables_route_to_engines() {
    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let typed_catalog = Arc::new(TypedTestCatalog {
        inner: fs_catalog,
        declared_types: HashMap::from([
            ("it".to_string(), TableType::ObjectTable),
            ("lt".to_string(), TableType::LanceTable),
        ]),
    });
    let mut ctx = SQLContext::new();
    ctx.register_catalog(CATALOG, typed_catalog).await.unwrap();
    ctx.sql(&format!("CREATE SCHEMA {CATALOG}.{DB}"))
        .await
        .unwrap();
    for declared in [TableType::ObjectTable, TableType::LanceTable] {
        ctx.register_catalog_table_engine(CATALOG, declared, Arc::new(FakeEngineResolver))
            .unwrap();
    }

    let batches = ctx
        .sql(&format!("SELECT id FROM {CATALOG}.{DB}.it ORDER BY id"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_i32(&batches), vec![1, 3]);
}

#[tokio::test]
async fn an_unsupported_time_travel_clause_on_a_paimon_table_is_rejected() {
    use datafusion::prelude::SessionContext;
    use paimon_datafusion::PaimonCatalogProvider;

    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    fs_catalog
        .create_database(DB, false, HashMap::new())
        .await
        .unwrap();
    let plain = PaimonSchema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .build()
        .unwrap();
    fs_catalog
        .create_table(&Identifier::new(DB, "pt"), plain, false)
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog(
        CATALOG,
        Arc::new(PaimonCatalogProvider::new(
            Some(CATALOG.to_string()),
            fs_catalog,
            Default::default(),
            Default::default(),
            None,
        )),
    );
    paimon_datafusion::register_catalog_table_engine(
        &ctx,
        CATALOG,
        TableType::IcebergTable,
        Arc::new(FakeEngineResolver),
    )
    .unwrap();
    for dialect in ["databricks", "mssql", "bigquery", "snowflake"] {
        ctx.sql(&format!("SET datafusion.sql_parser.dialect = '{dialect}'"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let run = |sql: String| {
            let ctx = &ctx;
            async move {
                match ctx.sql(&sql).await {
                    Err(err) => Err(err.to_string()),
                    Ok(df) => df.collect().await.map(|_| ()).map_err(|e| e.to_string()),
                }
            }
        };
        let ts = "2020-01-01 00:00:00";
        let system_time = run(format!(
            "SELECT * FROM {CATALOG}.{DB}.pt FOR SYSTEM_TIME AS OF '{ts}'"
        ))
        .await;
        let timestamp = run(format!(
            "SELECT * FROM {CATALOG}.{DB}.pt TIMESTAMP AS OF '{ts}'"
        ))
        .await;
        assert_eq!(
            system_time, timestamp,
            "[{dialect}] the two spellings diverged"
        );

        let Err(msg) = run(format!("SELECT * FROM {CATALOG}.{DB}.pt AT('{ts}')")).await else {
            panic!("[{dialect}] a historical clause must not answer with current rows");
        };
        assert!(
            msg.contains("this time-travel syntax is not supported"),
            "[{dialect}] {msg}"
        );

        let err = ctx
            .sql(&format!("SELECT * FROM {CATALOG}.{DB}.pt VERSION AS OF 1"))
            .await
            .unwrap()
            .collect()
            .await
            .err()
            .unwrap_or_else(|| panic!("[{dialect}] snapshot 1 does not exist in this fixture"));
        assert!(err.to_string().contains("Snapshot 1"), "[{dialect}] {err}");

        let Err(msg) = run(format!(
            "SELECT * FROM {CATALOG}.{DB}.\"pt$schemas\" VERSION AS OF 1"
        ))
        .await
        else {
            panic!("[{dialect}] a system table answered a historical clause with current rows");
        };
        assert!(
            msg.contains("time travel is not supported for"),
            "[{dialect}] {msg}"
        );
    }
}

#[tokio::test]
async fn time_travel_on_routed_tables_is_rejected() {
    let env = setup().await;
    env.ctx
        .ctx()
        .sql("SET datafusion.sql_parser.dialect = 'databricks'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let Err(err) = env
        .ctx
        .ctx()
        .sql(&format!(
            "SELECT * FROM {CATALOG}.{DB}.it VERSION AS OF 999999"
        ))
        .await
    else {
        panic!("time travel on a routed table must not silently read current data");
    };
    let msg = err.to_string();
    assert!(msg.contains("time travel is not supported"), "{msg}");

    let Err(err) = env
        .ctx
        .ctx()
        .sql(&format!(
            "SELECT * FROM {CATALOG}.{DB}.it TIMESTAMP AS OF '2020-01-01 00:00:00'"
        ))
        .await
    else {
        panic!("timestamp travel on a routed table must be rejected too");
    };
    assert!(
        err.to_string().contains("time travel is not supported"),
        "{err}"
    );
}

#[tokio::test]
async fn session_time_travel_on_routed_tables_is_rejected() {
    let env = setup().await;
    env.ctx
        .sql("SET 'paimon.scan.version' = '1'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let Err(err) = env
        .ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it"))
        .await
    else {
        panic!("a session scan selector must not silently read current data");
    };
    assert!(
        err.to_string().contains("time travel is not supported"),
        "{err}"
    );

    env.ctx
        .sql("RESET 'paimon.scan.version'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    env.ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it"))
        .await
        .expect("routing works again once the selector is reset");

    env.ctx
        .sql("SET 'paimon.incremental-between' = '1,5'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let Err(err) = env
        .ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it"))
        .await
    else {
        panic!("an unsupported scan option must not be silently ignored");
    };
    assert!(err.to_string().contains("incremental-between"), "{err}");
}

#[tokio::test]
async fn every_version_clause_on_routed_tables_is_rejected() {
    let env = setup().await;
    env.ctx
        .ctx()
        .sql("SET datafusion.sql_parser.dialect = 'databricks'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    for clause in [
        "VERSION AS OF 999999",
        "TIMESTAMP AS OF '2020-01-01 00:00:00'",
        "FOR SYSTEM_TIME AS OF '2020-01-01 00:00:00'",
    ] {
        let Err(err) = env
            .ctx
            .ctx()
            .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it {clause}"))
            .await
        else {
            panic!("{clause} must not silently read current data");
        };
        assert!(
            err.to_string().contains("time travel is not supported"),
            "{clause}: {err}"
        );
    }
}

#[tokio::test]
async fn show_create_on_routed_tables_reports_the_declared_type() {
    let env = setup().await;
    let Err(err) = env
        .ctx
        .sql(&format!("SHOW CREATE TABLE {CATALOG}.{DB}.it"))
        .await
    else {
        panic!("Paimon DDL would misrepresent an engine-served table");
    };
    let msg = err.to_string();
    assert!(msg.contains("iceberg-table"), "{msg}");
    assert!(msg.contains("cannot be read as a Paimon table"), "{msg}");
}

#[tokio::test]
async fn session_query_auth_blocks_routed_reads() {
    let env = setup().await;
    env.ctx
        .sql("SET 'paimon.query-auth.enabled' = 'true'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let Err(err) = env
        .ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it"))
        .await
    else {
        panic!("query-auth must block a routed read, not just a Paimon one");
    };
    assert!(err.to_string().contains("query-auth"), "{err}");

    env.ctx
        .sql("RESET 'paimon.query-auth.enabled'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    env.ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it"))
        .await
        .expect("routing works again once the flag is reset");
}

#[tokio::test]
async fn registering_on_a_raw_session_installs_the_planner() {
    use datafusion::prelude::SessionContext;
    use paimon_datafusion::{register_catalog_table_engine, PaimonCatalogProvider};

    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    fs_catalog
        .create_database(DB, false, HashMap::new())
        .await
        .unwrap();
    let typed_catalog = Arc::new(TypedTestCatalog {
        inner: fs_catalog,
        declared_types: HashMap::from([("it".to_string(), TableType::IcebergTable)]),
    });

    // No SQLContext: the raw path a caller might take.
    let ctx = SessionContext::new();
    ctx.register_catalog(
        CATALOG,
        Arc::new(PaimonCatalogProvider::new(
            Some(CATALOG.to_string()),
            typed_catalog,
            Default::default(),
            Default::default(),
            None,
        )),
    );
    register_catalog_table_engine(
        &ctx,
        CATALOG,
        TableType::IcebergTable,
        Arc::new(FakeEngineResolver),
    )
    .unwrap();
    ctx.sql("SET datafusion.sql_parser.dialect = 'databricks'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let Err(err) = ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it VERSION AS OF 1"))
        .await
    else {
        panic!("registration must install the planner, so this cannot read current data");
    };
    assert!(err.to_string().contains("not supported"), "{err}");

    register_catalog_table_engine(
        &ctx,
        CATALOG,
        TableType::IcebergTable,
        Arc::new(SnapshotEngineResolver::default()),
    )
    .unwrap();
    let batches = ctx
        .sql(&format!(
            "SELECT historical FROM {CATALOG}.{DB}.it VERSION AS OF 10 ORDER BY id"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        string_column_values(&batches, "historical"),
        ["old-a", "old-b"]
    );
}

#[tokio::test]
async fn an_external_type_without_an_engine_says_so() {
    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let typed_catalog = Arc::new(TypedTestCatalog {
        inner: fs_catalog,
        declared_types: HashMap::from([("it".to_string(), TableType::IcebergTable)]),
    });
    let mut ctx = SQLContext::new();
    ctx.register_catalog(CATALOG, typed_catalog).await.unwrap();
    ctx.sql(&format!("CREATE SCHEMA {CATALOG}.{DB}"))
        .await
        .unwrap();

    let err = ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it"))
        .await
        .expect("metadata-only planning should succeed")
        .collect()
        .await
        .expect_err("an external table without an engine must not be readable");
    let msg = err.to_string();
    assert!(msg.contains("no table engine is registered"), "{msg}");
    assert!(msg.contains("iceberg-table"), "{msg}");
}

#[tokio::test]
async fn unregistered_external_table_does_not_break_information_schema_columns() {
    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let typed_catalog = Arc::new(TypedTestCatalog {
        inner: fs_catalog,
        declared_types: HashMap::from([("external".to_string(), TableType::IcebergTable)]),
    });
    let mut ctx = SQLContext::new();
    ctx.register_catalog(CATALOG, typed_catalog).await.unwrap();
    ctx.sql(&format!("CREATE SCHEMA {CATALOG}.{DB}"))
        .await
        .unwrap();
    ctx.sql(&format!(
        "CREATE TABLE {CATALOG}.{DB}.pt (id INT NOT NULL, name STRING)"
    ))
    .await
    .unwrap();
    ctx.sql("SET 'paimon.scan.version' = '1'").await.unwrap();

    let provider = ctx.ctx().catalog(CATALOG).unwrap();
    let schema = provider.schema(DB).unwrap();
    assert!(
        schema.table("external").await.unwrap().is_some(),
        "metadata-only table loading should succeed without a registered engine"
    );
    assert!(
        schema.table_exist("external"),
        "table_exist must agree with the metadata-only table provider"
    );

    let show_tables = ctx
        .sql("SHOW TABLES")
        .await
        .expect("SHOW TABLES must not load an external table engine")
        .collect()
        .await
        .expect("SHOW TABLES must remain queryable");
    let shown_names = string_column_values(&show_tables, "table_name");
    assert!(
        shown_names.contains(&"external".to_string()),
        "{shown_names:?}"
    );

    let batches = ctx
        .sql(&format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_catalog = '{CATALOG}' \
               AND table_schema = '{DB}' \
               AND table_name = 'pt' \
             ORDER BY ordinal_position"
        ))
        .await
        .expect("an unrelated unregistered engine table must not break planning")
        .collect()
        .await
        .expect("information_schema.columns must remain queryable");
    let names = string_column_values(&batches, "column_name");
    assert_eq!(names, vec!["id", "name"]);

    let external_columns = ctx
        .sql(&format!(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_catalog = '{CATALOG}' \
               AND table_schema = '{DB}' \
               AND table_name = 'external'"
        ))
        .await
        .expect("the external table schema should be available from catalog metadata")
        .collect()
        .await
        .expect("the external table schema should not require an engine");
    let external_names = string_column_values(&external_columns, "column_name");
    assert_eq!(external_names, vec!["external_id"]);
}

async fn legacy_catalog_with_iceberg_table() -> (TempDir, Arc<LegacyTestCatalog>) {
    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    fs_catalog
        .create_database(DB, false, HashMap::new())
        .await
        .unwrap();
    let schema = PaimonSchema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .column(
            "pt",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .partition_keys(["pt"])
        .option("type", "iceberg-table")
        .build()
        .unwrap();
    fs_catalog
        .create_table(&Identifier::new(DB, "it"), schema, false)
        .await
        .unwrap();
    (
        paimon_dir,
        Arc::new(LegacyTestCatalog { inner: fs_catalog }),
    )
}

#[tokio::test]
async fn the_default_load_table_classifies_for_a_catalog_that_only_has_get_table() {
    let (_dir, catalog) = legacy_catalog_with_iceberg_table().await;

    let loaded = catalog
        .load_table(&Identifier::new(DB, "it"))
        .await
        .unwrap();
    assert!(
        matches!(loaded, LoadedTable::External(ref e) if e.declared() == TableType::IcebergTable),
        "{loaded:?}"
    );
}

#[tokio::test]
async fn a_legacy_catalog_cannot_serve_an_external_table_as_paimon() {
    let (_dir, catalog) = legacy_catalog_with_iceberg_table().await;
    let mut ctx = SQLContext::new();
    ctx.register_catalog(CATALOG, catalog).await.unwrap();

    let err = ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it"))
        .await
        .expect("catalog metadata should be sufficient for planning")
        .collect()
        .await
        .expect_err("a legacy catalog must not serve an iceberg table as Paimon");
    let msg = err.to_string();
    assert!(msg.contains("no table engine is registered"), "{msg}");
    assert!(msg.contains("iceberg-table"), "{msg}");
}

#[tokio::test]
async fn a_hand_built_external_table_is_rejected_by_the_provider() {
    let (_dir, catalog) = legacy_catalog_with_iceberg_table().await;

    let table = catalog.get_table(&Identifier::new(DB, "it")).await.unwrap();
    let Err(err) = paimon_datafusion::PaimonTableProvider::try_new(table) else {
        panic!("a table declared iceberg-table must not become a Paimon provider");
    };
    let msg = err.to_string();
    assert!(msg.contains("cannot be read as a Paimon table"), "{msg}");
    assert!(msg.contains("iceberg-table"), "{msg}");
}

async fn legacy_sql_context() -> (TempDir, SQLContext) {
    let (dir, catalog) = legacy_catalog_with_iceberg_table().await;
    let mut ctx = SQLContext::new();
    ctx.register_catalog(CATALOG, catalog).await.unwrap();
    (dir, ctx)
}

#[tokio::test]
async fn a_legacy_catalog_refuses_every_destructive_statement() {
    let (_dir, ctx) = legacy_sql_context().await;

    for sql in [
        format!("INSERT INTO {CATALOG}.{DB}.it VALUES (1, 1)"),
        format!("INSERT OVERWRITE {CATALOG}.{DB}.it PARTITION (pt = 1) VALUES (1)"),
        format!("UPDATE {CATALOG}.{DB}.it SET id = 2"),
        format!("DELETE FROM {CATALOG}.{DB}.it"),
        format!("TRUNCATE TABLE {CATALOG}.{DB}.it"),
        format!("CALL {CATALOG}.sys.create_tag(table => '{DB}.it', tag => 't1')"),
    ] {
        let outcome = match ctx.sql(&sql).await {
            Err(err) => Err(err),
            Ok(df) => df.collect().await.map(|_| ()),
        };
        let Err(err) = outcome else {
            panic!("must not run against an iceberg-table: {sql}");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("iceberg-table") || msg.contains("no table engine is registered"),
            "{sql} -> {msg}"
        );
    }
}

#[tokio::test]
async fn a_legacy_catalog_refuses_system_tables() {
    let (_dir, ctx) = legacy_sql_context().await;

    let outcome = match ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.\"it$snapshots\""))
        .await
    {
        Err(err) => Err(err),
        Ok(df) => df.collect().await.map(|_| ()),
    };
    let Err(err) = outcome else {
        panic!("a system table on an iceberg-table must not resolve");
    };
    let msg = err.to_string();
    assert!(msg.contains("iceberg-table"), "{msg}");
}

#[tokio::test]
async fn a_legacy_catalog_refuses_paimon_reads_and_writes_in_core() {
    let (_dir, catalog) = legacy_catalog_with_iceberg_table().await;
    let table = catalog.get_table(&Identifier::new(DB, "it")).await.unwrap();

    let read = table.new_read_builder().new_read();
    assert!(read.is_err(), "core read must be refused");

    let write = paimon::table::WriteBuilder::new(&table).new_write();
    assert!(write.is_err(), "core write must be refused");

    let commit = paimon::table::WriteBuilder::new(&table).new_commit();
    assert!(
        commit.commit(Vec::new()).await.is_err(),
        "core commit must be refused"
    );
    assert!(
        commit.truncate_table_with_identifier(1).await.is_err(),
        "core truncate must be refused"
    );
    assert!(
        commit.abort(&[]).await.is_err(),
        "core abort must be refused"
    );

    let incremental = table
        .new_read_builder()
        .new_incremental_scan(paimon::table::IncrementalScanMode::Delta, 0, 1)
        .plan()
        .await;
    assert!(
        incremental.is_err(),
        "core incremental scan must be refused"
    );
}

#[tokio::test]
async fn a_legacy_catalog_refuses_scan_planning_rather_than_reporting_empty() {
    let (_dir, catalog) = legacy_catalog_with_iceberg_table().await;
    let table = catalog.get_table(&Identifier::new(DB, "it")).await.unwrap();

    let plan = table.new_read_builder().new_scan().plan().await;
    assert!(
        plan.is_err(),
        "planning must be refused, not answered with an empty plan"
    );

    let stats = table.partition_stats().await;
    assert!(stats.is_err(), "partition stats must be refused");
}

#[tokio::test]
async fn a_legacy_catalog_refuses_the_infallible_commit_path() {
    let (_dir, catalog) = legacy_catalog_with_iceberg_table().await;
    let table = catalog.get_table(&Identifier::new(DB, "it")).await.unwrap();

    let commit = paimon::table::WriteBuilder::new(&table).new_commit();
    assert!(
        commit.truncate_table().await.is_err(),
        "truncate must not write Paimon metadata over foreign data"
    );
}

#[tokio::test]
async fn a_dynamic_copy_cannot_launder_the_declared_type() {
    let (_dir, catalog) = legacy_catalog_with_iceberg_table().await;
    let table = catalog.get_table(&Identifier::new(DB, "it")).await.unwrap();

    let copied =
        table.copy_with_options(HashMap::from([("type".to_string(), "table".to_string())]));
    assert!(
        copied.new_read_builder().new_read().is_err(),
        "an override of 'type' must not re-route foreign data through the Paimon reader"
    );
    assert!(
        paimon::table::WriteBuilder::new(&copied)
            .new_write()
            .is_err(),
        "an override of 'type' must not open a Paimon write on foreign data"
    );
}

#[tokio::test]
async fn a_legacy_catalog_refuses_show_create() {
    let (_dir, ctx) = legacy_sql_context().await;

    let outcome = match ctx
        .sql(&format!("SHOW CREATE TABLE {CATALOG}.{DB}.it"))
        .await
    {
        Err(err) => Err(err),
        Ok(df) => df.collect().await.map(|_| ()),
    };
    let Err(err) = outcome else {
        panic!("SHOW CREATE must not emit Paimon DDL for an iceberg-table");
    };
    let msg = err.to_string();
    assert!(msg.contains("iceberg-table"), "{msg}");
}

#[tokio::test]
async fn a_branch_copy_cannot_launder_the_declared_type() {
    let (_dir, catalog) = legacy_catalog_with_iceberg_table().await;
    let table = catalog.get_table(&Identifier::new(DB, "it")).await.unwrap();

    assert!(
        table.copy_with_branch("b1").await.is_err(),
        "a branch copy must not shed the declared type"
    );
    assert!(
        table
            .copy_with_time_travel(HashMap::from([(
                "scan.snapshot-id".to_string(),
                "1".to_string(),
            )]))
            .await
            .is_err(),
        "time travel must not read Paimon snapshot paths of foreign data"
    );
}

#[tokio::test]
async fn a_rejected_external_table_does_not_pollute_the_blob_registry() {
    let (_dir, catalog) = legacy_catalog_with_iceberg_table().await;
    let table = catalog.get_table(&Identifier::new(DB, "it")).await.unwrap();
    let location = table.location().to_string();

    let registry = paimon_datafusion::BlobReaderRegistry::default();
    let built = paimon_datafusion::PaimonTableProvider::try_new_with_blob_reader_registry(
        table,
        registry.clone(),
    );
    assert!(
        built.is_err(),
        "an iceberg-table must not become a provider"
    );
    assert!(
        registry.resolve(&format!("{location}/blob/x")).is_none(),
        "a rejected table must leave no registration behind"
    );
}

#[derive(Debug)]
struct ForeignRelationPlanner;

impl datafusion::logical_expr::planner::RelationPlanner for ForeignRelationPlanner {
    fn plan_relation(
        &self,
        relation: datafusion::sql::sqlparser::ast::TableFactor,
        _context: &mut dyn datafusion::logical_expr::planner::RelationPlannerContext,
    ) -> DFResult<datafusion::logical_expr::planner::RelationPlanning> {
        use datafusion::sql::sqlparser::ast::TableFactor;
        if matches!(
            relation,
            TableFactor::Table {
                version: Some(_),
                ..
            }
        ) {
            return Err(DataFusionError::Plan("foreign planner reached".into()));
        }
        Ok(datafusion::logical_expr::planner::RelationPlanning::Original(Box::new(relation)))
    }
}

#[tokio::test]
async fn a_foreign_provider_keeps_its_own_version_clause() {
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new();
    for name in ["foreign_table", "foreign$history", "foreign$schemas"] {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as _],
        )
        .unwrap();
        ctx.register_table(
            datafusion::common::TableReference::bare(name),
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    }
    ctx.register_relation_planner(Arc::new(ForeignRelationPlanner))
        .unwrap();
    ctx.register_relation_planner(Arc::new(paimon_datafusion::PaimonRelationPlanner::new()))
        .unwrap();
    ctx.sql("SET datafusion.sql_parser.dialect = 'databricks'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    for table in [
        "foreign_table",
        "\"foreign$history\"",
        "\"foreign$schemas\"",
    ] {
        for clause in [
            "FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP() - INTERVAL '1' DAY",
            "TIMESTAMP AS OF '2020-01-01T00:00:00Z'",
            "VERSION AS OF 1",
        ] {
            let err = ctx
                .sql(&format!("SELECT * FROM {table} {clause}"))
                .await
                .err()
                .unwrap_or_else(|| {
                    panic!("[{table} {clause}] expected the foreign planner to claim it")
                });
            assert!(
                err.to_string().contains("foreign planner reached"),
                "[{table} {clause}] Paimon took a clause that is not its own: {err}"
            );
        }
    }
}
