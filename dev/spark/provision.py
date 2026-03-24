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


if __name__ == "__main__":
    main()
