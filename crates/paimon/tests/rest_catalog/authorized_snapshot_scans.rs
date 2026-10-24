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

use super::*;

async fn append(g: &Guarded, batch: RecordBatch) {
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", g._tmp.path().display()),
    );
    let catalog = FileSystemCatalog::new(options).unwrap();
    let plain = catalog.get_table(&g.identifier).await.unwrap();
    write_batch(&plain, batch, "auth-stream-append").await;
}

async fn read_frame(
    builder: &paimon::table::ReadBuilder<'_>,
    plan: &paimon::table::Plan,
    kinds: bool,
) -> paimon::Result<Vec<RecordBatch>> {
    let read = builder.new_read()?;
    if kinds {
        read.to_arrow_with_row_kind(plan.splits())?
    } else {
        read.to_arrow(plan.splits())?
    }
    .try_collect()
    .await
}

#[tokio::test]
async fn authorized_stream_refreshes_rules_but_old_frames_keep_captured_rules() {
    let g = written("stream_refresh", people, vec![people_rows(1..=3)]).await;
    g.set_rules(
        &[int_leaf(0, "id", "GREATER_THAN", 1)],
        &[("name", upper(1, "name"))],
    );
    let mut builder = g.table.new_read_builder();
    builder.with_projection(&["id", "name"]).unwrap();
    let mut stream = builder.new_stream_scan().unwrap();
    let first = stream.plan().await.unwrap().unwrap();
    assert_eq!(
        ids(&read_frame(&builder, &first, false).await.unwrap()),
        [2, 3]
    );
    assert_eq!(stream.checkpoint(), Some(2));
    g.set_rules(
        &[int_leaf(0, "id", "GREATER_OR_EQUAL", 5)],
        &[("name", upper(1, "name"))],
    );
    append(&g, people_rows(4..=5)).await;
    let next = stream.plan().await.unwrap().unwrap();
    assert_eq!(
        id_names(&read_frame(&builder, &next, true).await.unwrap()),
        [(5, Some("ERIN".into()))]
    );
    assert_eq!(stream.checkpoint(), Some(3));
    assert_eq!(
        id_names(&read_frame(&builder, &first, true).await.unwrap()),
        [(2, Some("BOB".into())), (3, Some("CAROL".into()))]
    );
    assert!(stream.plan().await.unwrap().is_none());
    g.set_rules(&[], &[]);
    append(&g, people_rows(6..=6)).await;
    assert_eq!(
        ids(
            &read_frame(&builder, &stream.plan().await.unwrap().unwrap(), false)
                .await
                .unwrap()
        ),
        [6]
    );
}

#[tokio::test]
async fn authorization_failure_preserves_initial_follow_up_and_empty_poll_progress() {
    let g = written("stream_retry", people, vec![people_rows(1..=2)]).await;
    let mut builder = g.table.new_read_builder();
    builder.with_projection(&["id", "name"]).unwrap();
    let mut stream = builder.new_stream_scan().unwrap();
    g.ctx
        .server
        .set_column_auth("default", "stream_retry", vec!["id".into()]);
    assert_forbidden(stream.plan().await.unwrap_err());
    assert_eq!(stream.checkpoint(), None);
    g.ctx
        .server
        .set_column_auth("default", "stream_retry", vec!["id".into(), "name".into()]);
    assert_eq!(stream.plan().await.unwrap().unwrap().snapshot_id(), Some(1));
    append(&g, people_rows(3..=3)).await;
    g.ctx
        .server
        .set_column_auth("default", "stream_retry", vec!["id".into()]);
    assert_forbidden(stream.plan().await.unwrap_err());
    assert_eq!(stream.checkpoint(), Some(2));
    g.ctx
        .server
        .set_column_auth("default", "stream_retry", vec!["id".into(), "name".into()]);
    assert_eq!(
        ids(
            &read_frame(&builder, &stream.plan().await.unwrap().unwrap(), false)
                .await
                .unwrap()
        ),
        [3]
    );
    g.ctx
        .server
        .set_column_auth("default", "stream_retry", vec!["id".into()]);
    assert_forbidden(stream.plan().await.unwrap_err());
    assert_eq!(stream.checkpoint(), Some(3));
}

fn keyed_people(options: &[(&str, &str)]) -> Schema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("changelog-producer", "input");
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    builder.build().unwrap()
}

fn changes() -> RecordBatch {
    let batch = people_batch(&[(1, "old"), (1, "new"), (2, "deleted")]);
    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(ArrowField::new(
        "_VALUE_KIND",
        ArrowDataType::Int8,
        false,
    )));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(arrow_array::Int8Array::from(vec![1, 2, 3])));
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn authorized_stream_keeps_physical_change_kinds_after_filters_and_masks() {
    let g = written(
        "stream_kinds",
        keyed_people,
        vec![people_batch(&[(1, "old"), (2, "deleted")]), changes()],
    )
    .await;
    g.set_rules(
        &[int_leaf(0, "id", "EQUAL", 1)],
        &[("name", upper(1, "name"))],
    );
    let mut builder = g.table.new_read_builder();
    builder.with_projection(&["name"]).unwrap();
    let mut stream = builder.new_stream_scan().unwrap();
    stream.restore(Some(2)).unwrap();
    let plan = stream.plan().await.unwrap().unwrap();
    let batches = read_frame(&builder, &plan, true).await.unwrap();
    let rows = batches
        .iter()
        .flat_map(|batch| {
            let kinds = column::<StringArray>(batch, "rowkind");
            let names = column::<StringArray>(batch, "name");
            (0..batch.num_rows())
                .map(|i| (kinds.value(i).to_string(), names.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        [("-U".into(), "OLD".into()), ("+U".into(), "NEW".into())]
    );
    assert!(batches.iter().all(|batch| batch.num_columns() == 2));
    let serialized = plan.splits()[0].serialize_split_v1().unwrap();
    let detached = paimon::table::DataSplit::deserialize_split_v1(&serialized).unwrap();
    assert_refused(
        builder
            .new_read()
            .unwrap()
            .to_arrow_with_row_kind(&[detached])
            .err()
            .expect("wire bytes cannot mint a grant"),
    );
    let mut empty_projection = g.table.new_read_builder();
    empty_projection.with_projection(&[]).unwrap();
    let batches = read_frame(&empty_projection, &plan, true).await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    assert!(batches.iter().all(|batch| batch.num_columns() == 1));
}

#[tokio::test]
async fn authorized_row_kinds_keep_blob_resolution_after_filters_for_both_split_kinds() {
    let (_source, good, missing) = blob_payloads();
    for primary_key in [false, true] {
        let g = blob_mask_rows(primary_key, vec![None, Some(&missing), Some(&good)]).await;
        g.set_rules(
            &[
                int_leaf(0, "id", "NOT_EQUAL", 2),
                int_leaf(0, "id", "NOT_EQUAL", 5),
            ],
            &[("payload", field_ref_mask(1, "payload", "BLOB"))],
        );
        let builder = g.table.new_read_builder();
        let mut scan = builder.new_stream_scan().unwrap();
        let initial = scan.plan().await.unwrap().unwrap();
        append(
            &g,
            RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int32Array::from(vec![4, 5])) as arrow_array::ArrayRef,
                ),
                (
                    "payload",
                    Arc::new(LargeBinaryArray::from(vec![
                        Some(good.as_slice()),
                        Some(missing.as_slice()),
                    ])) as arrow_array::ArrayRef,
                ),
                (
                    "label",
                    Arc::new(StringArray::from(vec!["row4", "row5"])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await;
        let follow_up = scan.plan().await.unwrap().unwrap();
        let splits = [initial.splits(), follow_up.splits()].concat();
        let batches: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow_with_row_kind(&splits)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(ids(&batches), [1, 3, 4]);
        for batch in &batches {
            assert!(column::<StringArray>(batch, "rowkind")
                .iter()
                .all(|kind| kind == Some("+I")));
            for payload in column::<LargeBinaryArray>(batch, "payload")
                .iter()
                .flatten()
            {
                assert_eq!(payload, b"good");
            }
        }
        let mut limited = g.table.new_read_builder();
        limited.with_limit(1);
        let batches: Vec<RecordBatch> = limited
            .new_read()
            .unwrap()
            .to_arrow_with_row_kind(&splits)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
}

#[tokio::test]
async fn combined_authorized_events_filter_before_limit_and_stamp_every_split() {
    for keyed in [false, true] {
        let g = if keyed {
            written(
                "events_keyed",
                keyed_people,
                vec![people_rows(1..=2), people_rows(3..=4), people_rows(5..=6)],
            )
            .await
        } else {
            written(
                "events_append",
                people,
                vec![people_rows(1..=2), people_rows(3..=4), people_rows(5..=6)],
            )
            .await
        };
        g.set_rules(
            &[int_leaf(0, "id", "GREATER_THAN", 4)],
            &[("name", upper(1, "name"))],
        );
        let mut builder = g.table.new_read_builder();
        builder
            .with_projection(&["id"])
            .unwrap()
            .with_limit(1)
            .with_filter(
                PredicateBuilder::new(g.table.schema().fields())
                    .equal("name", Datum::String("FRANK".into()))
                    .unwrap(),
            );
        let mode = if keyed {
            paimon::table::IncrementalScanMode::Changelog
        } else {
            paimon::table::IncrementalScanMode::Delta
        };
        let plan = builder
            .new_incremental_scan(mode, 0, 3)
            .plan_combined()
            .await
            .unwrap();
        assert_eq!(ids(&read_frame(&builder, &plan, false).await.unwrap()), [6]);
        assert_eq!(ids(&read_frame(&builder, &plan, true).await.unwrap()), [6]);
        let requested = g.ctx.server.auth_selects("default", g.identifier.object());
        assert!(requested
            .iter()
            .all(|select| select.as_ref().unwrap().len() == 2));
        // Java's high-level timestamp-window path shares this authorization scope.
        let window = g.table.copy_with_options(HashMap::from([(
            "incremental-between-timestamp".into(),
            "0,9223372036854775807".into(),
        )]));
        let mut window_builder = window.new_read_builder();
        window_builder.with_projection(&["id"]).unwrap();
        let plan = window_builder.new_scan().plan().await.unwrap();
        assert_eq!(
            ids(&read_frame(&window_builder, &plan, true).await.unwrap()),
            [5, 6]
        );
        // Zero-length ranges must still request column authorization.
        g.ctx
            .server
            .set_column_auth("default", g.identifier.object(), vec!["id".into()]);
        assert_forbidden(
            builder
                .new_incremental_scan(mode, 3, 3)
                .plan_combined()
                .await
                .unwrap_err(),
        );
    }
}

#[tokio::test]
async fn stream_authorizes_empty_tables_and_filtered_frames_before_advancing() {
    let g = written("stream_empty", people, vec![]).await;
    let mut builder = g.table.new_read_builder();
    builder.with_projection(&["id", "name"]).unwrap();
    let mut stream = builder.new_stream_scan().unwrap();
    g.ctx
        .server
        .set_column_auth("default", "stream_empty", vec!["id".into()]);
    assert_forbidden(stream.plan().await.unwrap_err());
    assert_eq!(stream.checkpoint(), None);
    g.ctx
        .server
        .set_column_auth("default", "stream_empty", vec!["id".into(), "name".into()]);
    assert!(stream.plan().await.unwrap().is_none());
    append(&g, people_rows(1..=2)).await;
    stream.with_bucket_filter(|_| Ok(false));
    let frame = stream.plan().await.unwrap().unwrap();
    assert!(frame.splits().is_empty());
    assert_eq!(stream.checkpoint(), Some(2));
    append(&g, people_rows(3..=3)).await;
    assert!(stream.plan().await.unwrap().is_none());
    assert_eq!(stream.checkpoint(), Some(3));
    g.ctx
        .server
        .set_column_auth("default", "stream_empty", vec!["id".into()]);
    assert_forbidden(stream.plan().await.unwrap_err());
    assert_eq!(stream.checkpoint(), Some(3));
}

#[tokio::test]
async fn stream_accepts_schema_evolution_but_rejects_identity_replacement() {
    for recreate in [false, true] {
        let g = written("stream_drift", people, vec![people_rows(1..=2)]).await;
        let mut stream = g.table.new_stream_scan().unwrap();
        if recreate {
            g.ctx
                .server
                .set_table_uuid("default", "stream_drift", "replacement-uuid");
        } else {
            g.ctx
                .server
                .set_table_schema_id("default", "stream_drift", people(GUARDED), 7);
        }
        if recreate {
            assert_drifted(stream.plan().await.unwrap_err(), "now resolves to uuid");
            assert_eq!(stream.checkpoint(), None);
        } else {
            let plan = stream.plan().await.unwrap().unwrap();
            let batches = read_frame(&g.table.new_read_builder(), &plan, false)
                .await
                .unwrap();
            assert_eq!(ids(&batches), [1, 2]);
            assert_eq!(stream.checkpoint(), Some(2));
        }
    }
}
