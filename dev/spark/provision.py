#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Provisions Paimon tables into the warehouse (file:/tmp/paimon-warehouse)
# for paimon-rust integration tests to read.

import shutil
from pathlib import Path
from urllib.parse import unquote, urlparse

from pyspark.sql import SparkSession


def _warehouse_path_from_spark_conf(spark: SparkSession) -> Path:
    warehouse_uri = spark.conf.get("spark.sql.catalog.paimon.warehouse")
    parsed = urlparse(warehouse_uri)

    if parsed.scheme not in ("", "file"):
        raise ValueError(
            f"Unsupported Paimon warehouse URI scheme {parsed.scheme!r}: {warehouse_uri}"
        )

    if parsed.netloc not in ("", "localhost"):
        raise ValueError(
            f"Unsupported remote Paimon warehouse location {parsed.netloc!r}: {warehouse_uri}"
        )

    warehouse_path = Path(unquote(parsed.path if parsed.scheme else warehouse_uri))
    if not warehouse_path.is_absolute() or str(warehouse_path) == "/":
        raise ValueError(f"Refusing to clear unsafe warehouse path: {warehouse_path}")

    return warehouse_path


def _reset_warehouse_dir(warehouse_path: Path) -> None:
    warehouse_path.mkdir(parents=True, exist_ok=True)

    for child in warehouse_path.iterdir():
        if child.is_symlink() or child.is_file():
            child.unlink()
        else:
            shutil.rmtree(child)


def main():
    spark = SparkSession.builder.getOrCreate()

    warehouse_path = _warehouse_path_from_spark_conf(spark)
    _reset_warehouse_dir(warehouse_path)

    # Use Paimon catalog (configured in spark-defaults.conf with warehouse file:/tmp/paimon-warehouse)
    spark.sql("USE paimon.default")

    # Table: simple log table for read tests
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS simple_log_table (
            id INT,
            name STRING
        ) USING paimon
        """
    )
    spark.sql("INSERT INTO simple_log_table VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

    # Spark SQL here does not accept table constraints like
    # PRIMARY KEY (id) NOT ENFORCED inside the column list, so use
    # Paimon table properties instead.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS simple_pk_table (
            id INT,
            name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '1'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO simple_pk_table VALUES
            (1, 'alice'),
            (2, 'bob'),
            (3, 'carol')
        """
    )

    # Table: primary key table with deletion vectors enabled.
    # Re-inserting the same keys with newer values creates deleted historical
    # rows that readers must filter via deletion vectors.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS simple_dv_pk_table (
            id INT,
            name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'deletion-vectors.enabled' = 'true'
        )
        """
    )

    spark.sql(
        """
        INSERT INTO simple_dv_pk_table VALUES
            (1, 'alice-v1'),
            (2, 'bob-v1'),
            (3, 'carol-v1'),
            (5, 'eve-v1')
        """
    )

    spark.sql(
        """
        INSERT INTO simple_dv_pk_table VALUES
            (2, 'bob-v2'),
            (3, 'carol-v2'),
            (4, 'dave-v1'),
            (6, 'frank-v1')
        """
    )

    spark.sql(
        """
        INSERT INTO simple_dv_pk_table VALUES
            (1, 'alice-v2'),
            (4, 'dave-v2'),
            (5, 'eve-v2')
        """
    )

    # ===== Partitioned table: single partition key (dt) =====
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS partitioned_log_table (
            id INT,
            name STRING,
            dt STRING
        ) USING paimon
        PARTITIONED BY (dt)
        """
    )
    spark.sql(
        """
        INSERT INTO partitioned_log_table VALUES
            (1, 'alice', '2024-01-01'),
            (2, 'bob', '2024-01-01'),
            (3, 'carol', '2024-01-02')
        """
    )

    # ===== Partitioned table: multiple partition keys (dt, hr) =====
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS multi_partitioned_log_table (
            id INT,
            name STRING,
            dt STRING,
            hr INT
        ) USING paimon
        PARTITIONED BY (dt, hr)
        """
    )
    spark.sql(
        """
        INSERT INTO multi_partitioned_log_table VALUES
            (1, 'alice', '2024-01-01', 10),
            (2, 'bob', '2024-01-01', 10),
            (3, 'carol', '2024-01-01', 20),
            (4, 'dave', '2024-01-02', 10)
    """
    )

    # ===== Partitioned table: PK + DV enabled =====
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS partitioned_dv_pk_table (
            id INT,
            name STRING,
            dt STRING
        ) USING paimon
        PARTITIONED BY (dt)
        TBLPROPERTIES (
            'primary-key' = 'id,dt',
            'bucket' = '1',
            'deletion-vectors.enabled' = 'true'
        )
        """
    )

    spark.sql(
        """
        INSERT INTO partitioned_dv_pk_table VALUES
            (1, 'alice-v1', '2024-01-01'),
            (2, 'bob-v1', '2024-01-01'),
            (1, 'alice-v1', '2024-01-02'),
            (3, 'carol-v1', '2024-01-02')
        """
    )

    spark.sql(
        """
        INSERT INTO partitioned_dv_pk_table VALUES
            (1, 'alice-v2', '2024-01-01'),
            (3, 'carol-v2', '2024-01-02'),
            (4, 'dave-v1', '2024-01-02')
        """
    )

    spark.sql(
        """
        INSERT INTO partitioned_dv_pk_table VALUES
            (2, 'bob-v2', '2024-01-01'),
            (4, 'dave-v2', '2024-01-02')
        """
    )

    # ===== Data Evolution table: append-only with row tracking =====
    # data-evolution.enabled + row-tracking.enabled allows partial column updates
    # via MERGE INTO. This produces files with different write_cols covering the
    # same row ID ranges, exercising the column-wise merge read path.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS data_evolution_table (
            id INT,
            name STRING,
            value INT
        ) USING paimon
        TBLPROPERTIES (
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true'
        )
        """
    )

    # First batch: rows with row_id 0, 1, 2 — all columns written
    spark.sql(
        """
        INSERT INTO data_evolution_table VALUES
            (1, 'alice', 100),
            (2, 'bob', 200),
            (3, 'carol', 300)
        """
    )

    # Second batch: rows with row_id 3, 4
    spark.sql(
        """
        INSERT INTO data_evolution_table VALUES
            (4, 'dave', 400),
            (5, 'eve', 500)
        """
    )

    # MERGE INTO: partial column update on existing rows.
    # This writes new files containing only the updated column (name) with the
    # same first_row_id, so the reader must merge columns from multiple files.
    # Paimon 1.3.1 requires the source table to be a Paimon table.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS data_evolution_updates (
            id INT,
            name STRING
        ) USING paimon
        """
    )
    spark.sql(
        """
        INSERT INTO data_evolution_updates VALUES (1, 'alice-v2'), (3, 'carol-v2')
        """
    )
    spark.sql(
        """
        MERGE INTO data_evolution_table t
        USING data_evolution_updates s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.name = s.name
        """
    )
    spark.sql("DROP TABLE data_evolution_updates")

    # ===== Time travel table: multiple snapshots for time travel tests =====
    # Snapshot 1: rows (1, 'alice'), (2, 'bob')
    # Snapshot 2: rows (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave')
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS time_travel_table (
            id INT,
            name STRING
        ) USING paimon
        """
    )
    spark.sql(
        """
        INSERT INTO time_travel_table VALUES
            (1, 'alice'),
            (2, 'bob')
        """
    )
    spark.sql(
        """
        INSERT INTO time_travel_table VALUES
            (3, 'carol'),
            (4, 'dave')
        """
    )

    # Create tags for tag-based time travel tests
    # Tag 'snapshot1' points to snapshot 1 (alice, bob)
    # Tag 'snapshot2' points to snapshot 2 (alice, bob, carol, dave)
    spark.sql("CALL sys.create_tag('default.time_travel_table', 'snapshot1', 1)")
    spark.sql("CALL sys.create_tag('default.time_travel_table', 'snapshot2', 2)")

    # ===== Schema Evolution: Add Column =====
    # Old files have (id, name); after ALTER TABLE ADD COLUMNS, new files have (id, name, age).
    # Reader must fill nulls for 'age' when reading old files.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS schema_evolution_add_column (
            id INT,
            name STRING
        ) USING paimon
        """
    )
    spark.sql(
        "INSERT INTO schema_evolution_add_column VALUES (1, 'alice'), (2, 'bob')"
    )
    spark.sql("ALTER TABLE schema_evolution_add_column ADD COLUMNS (age INT)")
    spark.sql(
        "INSERT INTO schema_evolution_add_column VALUES (3, 'carol', 30), (4, 'dave', 40)"
    )

    # ===== Schema Evolution: Type Promotion (INT -> BIGINT) =====
    # Old files have value as INT; after ALTER TABLE, new files have value as BIGINT.
    # Reader must cast INT to BIGINT when reading old files.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS schema_evolution_type_promotion (
            id INT,
            value INT
        ) USING paimon
        """
    )
    spark.sql(
        "INSERT INTO schema_evolution_type_promotion VALUES (1, 100), (2, 200)"
    )
    spark.sql(
        "ALTER TABLE schema_evolution_type_promotion ALTER COLUMN value TYPE BIGINT"
    )
    spark.sql(
        "INSERT INTO schema_evolution_type_promotion VALUES (3, 3000000000)"
    )

    # ===== Data Evolution + Schema Evolution: Add Column =====
    # Combines data-evolution (row-tracking + MERGE INTO) with ALTER TABLE ADD COLUMNS.
    # Old files lack the new column; MERGE INTO produces partial-column files.
    # Reader must fill nulls for missing columns AND merge columns across files.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS data_evolution_add_column (
            id INT,
            name STRING,
            value INT
        ) USING paimon
        TBLPROPERTIES (
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO data_evolution_add_column VALUES
            (1, 'alice', 100),
            (2, 'bob', 200)
        """
    )
    spark.sql("ALTER TABLE data_evolution_add_column ADD COLUMNS (extra STRING)")
    spark.sql(
        """
        INSERT INTO data_evolution_add_column VALUES
            (3, 'carol', 300, 'new'),
            (4, 'dave', 400, 'new')
        """
    )
    # MERGE INTO to trigger merge_files_by_columns with schema evolution.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS data_evolution_add_column_updates (
            id INT,
            name STRING
        ) USING paimon
        """
    )
    spark.sql(
        "INSERT INTO data_evolution_add_column_updates VALUES (1, 'alice-v2')"
    )
    spark.sql(
        """
        MERGE INTO data_evolution_add_column t
        USING data_evolution_add_column_updates s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.name = s.name
        """
    )
    spark.sql("DROP TABLE data_evolution_add_column_updates")

    # ===== Data Evolution + Schema Evolution: Type Promotion =====
    # Combines data-evolution with ALTER TABLE ALTER COLUMN TYPE (INT -> BIGINT).
    # Old files have INT; new files have BIGINT. MERGE INTO updates some rows.
    # Reader must cast old INT columns to BIGINT AND merge columns across files.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS data_evolution_type_promotion (
            id INT,
            value INT
        ) USING paimon
        TBLPROPERTIES (
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true'
        )
        """
    )
    spark.sql(
        "INSERT INTO data_evolution_type_promotion VALUES (1, 100), (2, 200)"
    )
    spark.sql(
        "ALTER TABLE data_evolution_type_promotion ALTER COLUMN value TYPE BIGINT"
    )
    spark.sql(
        "INSERT INTO data_evolution_type_promotion VALUES (3, 3000000000)"
    )
    # MERGE INTO to trigger merge_files_by_columns with type promotion.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS data_evolution_type_promotion_updates (
            id INT,
            value BIGINT
        ) USING paimon
        """
    )
    spark.sql(
        "INSERT INTO data_evolution_type_promotion_updates VALUES (1, 999)"
    )
    spark.sql(
        """
        MERGE INTO data_evolution_type_promotion t
        USING data_evolution_type_promotion_updates s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.value = s.value
        """
    )
    spark.sql("DROP TABLE data_evolution_type_promotion_updates")

    # ===== Data Evolution + Drop Column: tests NULL-fill when no file provides a column =====
    # After MERGE INTO on old rows, the merge group files all predate ADD COLUMN.
    # SELECT on the new column should return NULLs for old rows (not silently drop them).
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS data_evolution_drop_column (
            id INT,
            name STRING,
            value INT
        ) USING paimon
        TBLPROPERTIES (
            'row-tracking.enabled' = 'true',
            'data-evolution.enabled' = 'true'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO data_evolution_drop_column VALUES
            (1, 'alice', 100),
            (2, 'bob', 200)
        """
    )
    # MERGE INTO to create a partial-column file in the same row_id range.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS data_evolution_drop_column_updates (
            id INT,
            name STRING
        ) USING paimon
        """
    )
    spark.sql(
        "INSERT INTO data_evolution_drop_column_updates VALUES (1, 'alice-v2')"
    )
    spark.sql(
        """
        MERGE INTO data_evolution_drop_column t
        USING data_evolution_drop_column_updates s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.name = s.name
        """
    )
    spark.sql("DROP TABLE data_evolution_drop_column_updates")
    # Add a new column that no existing file contains.
    spark.sql("ALTER TABLE data_evolution_drop_column ADD COLUMNS (extra STRING)")
    # Insert new rows that DO have the extra column.
    spark.sql(
        """
        INSERT INTO data_evolution_drop_column VALUES
            (3, 'carol', 300, 'new')
        """
    )

    # ===== Schema Evolution: Drop Column =====
    # Old files have (id, name, score); after ALTER TABLE DROP COLUMN, table has (id, name).
    # Reader should ignore the dropped column when reading old files.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS schema_evolution_drop_column (
            id INT,
            name STRING,
            score INT
        ) USING paimon
        """
    )
    spark.sql(
        """
        INSERT INTO schema_evolution_drop_column VALUES
            (1, 'alice', 100),
            (2, 'bob', 200)
        """
    )
    spark.sql("ALTER TABLE schema_evolution_drop_column DROP COLUMN score")
    spark.sql(
        """
        INSERT INTO schema_evolution_drop_column VALUES
            (3, 'carol'),
            (4, 'dave')
        """
    )

    # ===== Complex Types table: ARRAY, MAP, STRUCT =====
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS complex_type_table (
            id INT,
            int_array ARRAY<INT>,
            string_map MAP<STRING, INT>,
            row_field STRUCT<name: STRING, value: INT>
        ) USING paimon
        """
    )
    spark.sql(
        """
        INSERT INTO complex_type_table VALUES
            (1, array(1, 2, 3), map('a', 10, 'b', 20), named_struct('name', 'alice', 'value', 100)),
            (2, array(4, 5), map('c', 30), named_struct('name', 'bob', 'value', 200)),
            (3, array(), map(), named_struct('name', 'carol', 'value', 300))
        """
    )

    # ===== Non-PK table with deletion vectors enabled =====
    # Append-only table with DV: level-0 files should NOT be filtered out
    # because there is no primary key merge.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS simple_dv_log_table (
            id INT,
            name STRING
        ) USING paimon
        TBLPROPERTIES (
            'deletion-vectors.enabled' = 'true'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO simple_dv_log_table VALUES
            (1, 'alice'),
            (2, 'bob'),
            (3, 'carol')
        """
    )

    # ===== Postpone bucket PK table (bucket = -2) =====
    # New data lands in bucket-postpone and is NOT visible to readers until compacted.
    # Without running compaction, the table should appear empty to batch readers.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS postpone_bucket_pk_table (
            id INT,
            name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '-2',
            'deletion-vectors.enabled' = 'true'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO postpone_bucket_pk_table VALUES
            (1, 'alice'),
            (2, 'bob'),
            (3, 'carol')
        """
    )


    # ===== Multi-bucket PK table for bucket predicate filtering tests =====
    # PK table with bucket=4 so data distributes across multiple buckets.
    # Bucket key defaults to primary key (id). Used to test that bucket predicate
    # filtering correctly computes target buckets from equality predicates.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS multi_bucket_pk_table (
            id INT,
            name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '4',
            'deletion-vectors.enabled' = 'true'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO multi_bucket_pk_table VALUES
            (1, 'alice'),
            (2, 'bob'),
            (3, 'carol'),
            (4, 'dave'),
            (5, 'eve'),
            (6, 'frank'),
            (7, 'grace'),
            (8, 'heidi')
        """
    )

    # ===== String bucket key tables for variable-length hash tests =====
    # Short string keys (<=7 bytes) use inline encoding in BinaryRow.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS string_bucket_short_key (
            code STRING,
            value INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'code',
            'bucket' = '4',
            'deletion-vectors.enabled' = 'true'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO string_bucket_short_key VALUES
            ('aaa', 1),
            ('bbb', 2),
            ('ccc', 3),
            ('ddd', 4),
            ('eee', 5),
            ('fff', 6),
            ('ggg', 7),
            ('hhh', 8)
        """
    )

    # Long string keys (>7 bytes) use variable-length encoding in BinaryRow.
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS string_bucket_long_key (
            code STRING,
            value INT
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'code',
            'bucket' = '4',
            'deletion-vectors.enabled' = 'true'
        )
        """
    )
    spark.sql(
        """
        INSERT INTO string_bucket_long_key VALUES
            ('alpha-long-key', 1),
            ('bravo-long-key', 2),
            ('charlie-long-key', 3),
            ('delta-long-key', 4),
            ('echo-long-key', 5),
            ('foxtrot-long-key', 6),
            ('golf-long-key', 7),
            ('hotel-long-key', 8)
        """
    )

    # ===== Full types table: parquet, orc, avro =====
    # Note: Spark 3.x does not support parameterized timestamp precision (e.g. TIMESTAMP(3)),
    # so all timestamps here use the default precision 6 (microseconds).
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS full_types_table (
            id INT,
            col_boolean BOOLEAN,
            col_tinyint TINYINT,
            col_smallint SMALLINT,
            col_int INT,
            col_bigint BIGINT,
            col_float FLOAT,
            col_double DOUBLE,
            col_decimal DECIMAL(10, 2),
            col_decimal5 DECIMAL(5),
            col_decimal38 DECIMAL(38, 18),
            col_string STRING,
            col_binary BINARY,
            col_date DATE,
            col_timestamp TIMESTAMP_NTZ,
            col_timestamp_ltz TIMESTAMP,
            col_array ARRAY<INT>,
            col_map MAP<STRING, INT>,
            col_struct STRUCT<name: STRING, value: INT>
        ) USING paimon
        TBLPROPERTIES (
            'file.format' = 'parquet'
        )
        """
    )
    # Parquet row
    spark.sql(
        """
        INSERT INTO full_types_table VALUES
            (1, true, CAST(1 AS TINYINT), CAST(100 AS SMALLINT), 1000, 100000,
             CAST(1.5 AS FLOAT), 2.5, CAST(123.45 AS DECIMAL(10,2)),
             CAST(12345 AS DECIMAL(5)), CAST(12345.678901234567890 AS DECIMAL(38,18)),
             'parquet-hello', X'DEADBEEF',
             DATE '2024-01-01',
             TIMESTAMP_NTZ '2024-01-01 10:00:00.123456',
             TIMESTAMP '2024-01-01 10:00:00.123456',
             array(1, 2, 3), map('a', 10, 'b', 20),
             named_struct('name', 'alice', 'value', 100))
        """
    )
    # Switch to ORC
    spark.sql("ALTER TABLE full_types_table SET TBLPROPERTIES ('file.format' = 'orc')")
    spark.sql(
        """
        INSERT INTO full_types_table VALUES
            (2, false, CAST(2 AS TINYINT), CAST(200 AS SMALLINT), 2000, 200000,
             CAST(3.5 AS FLOAT), 4.5, CAST(678.90 AS DECIMAL(10,2)),
             CAST(99999 AS DECIMAL(5)), CAST(99999.999999999999999 AS DECIMAL(38,18)),
             'orc-world', X'CAFEBABE',
             DATE '2024-06-15',
             TIMESTAMP_NTZ '2024-06-15 12:30:00.456789',
             TIMESTAMP '2024-06-15 12:30:00.456789',
             array(4, 5), map('c', 30),
             named_struct('name', 'bob', 'value', 200))
        """
    )
    # Switch to Avro
    spark.sql("ALTER TABLE full_types_table SET TBLPROPERTIES ('file.format' = 'avro')")
    spark.sql(
        """
        INSERT INTO full_types_table VALUES
            (3, true, CAST(3 AS TINYINT), CAST(300 AS SMALLINT), 3000, 300000,
             CAST(5.5 AS FLOAT), 6.5, CAST(999.99 AS DECIMAL(10,2)),
             CAST(0 AS DECIMAL(5)), CAST(0.000000000000000001 AS DECIMAL(38,18)),
             'avro-test', X'01020304',
             DATE '2025-12-31',
             TIMESTAMP_NTZ '2025-12-31 23:59:59.999999',
             TIMESTAMP '2025-12-31 23:59:59.999999',
             array(6), map('d', 40, 'e', 50),
             named_struct('name', 'carol', 'value', 300))
        """
    )


if __name__ == "__main__":
    main()
