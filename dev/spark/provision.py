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

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.getOrCreate()

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


if __name__ == "__main__":
    main()
