/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.paimon.globalindex.btree;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.CompositeKeySerializer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Generate compatibility fixtures with the Java production composite BTree writer. */
public class GenerateCompositeBTree {
    public static void main(String[] args) throws Exception {
        RowType scalarType = RowType.of(DataTypes.BOOLEAN(), DataTypes.TINYINT(),
                DataTypes.SMALLINT(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.FLOAT(),
                DataTypes.DOUBLE(), DataTypes.DECIMAL(20, 2), DataTypes.TIMESTAMP(6),
                DataTypes.STRING(), DataTypes.INT());
        GenericRow scalarKey = GenericRow.of(true, (byte) -3, (short) -257, -12345,
                1234567890123L, Float.intBitsToFloat(0xffc00001),
                Double.longBitsToDouble(0xfff8000000000001L),
                Decimal.fromBigDecimal(new BigDecimal("-1.29"), 20, 2), Timestamp.fromEpochMillis(-123000, 999999),
                BinaryString.fromString("联合"), null);
        Files.write(Paths.get(args[0], "btree_composite_java_scalar.key"),
                new CompositeKeySerializer(scalarType).serialize(scalarKey));
        for (int version : new int[] {1, 2}) {
            for (BlockCompressionType codec : new BlockCompressionType[] {
                    BlockCompressionType.NONE, BlockCompressionType.LZ4}) {
                final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                GlobalIndexFileWriter files = new GlobalIndexFileWriter() {
                    public String newFileName(String prefix) { return "fixture"; }
                    public PositionOutputStream newOutputStream(String name) {
                        return new PositionOutputStream() {
                            public long getPos() { return bytes.size(); }
                            public void write(int b) { bytes.write(b); }
                            public void write(byte[] b) { bytes.write(b, 0, b.length); }
                            public void write(byte[] b, int off, int len) { bytes.write(b, off, len); }
                            public void flush() {}
                            public void close() {}
                        };
                    }
                };
                RowType type = RowType.of(DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING());
                BTreeIndexWriter writer = new BTreeIndexWriter(
                        files, new CompositeKeySerializer(type),
                        codec == BlockCompressionType.NONE ? 64 : 512, null,
                        BlockCompressionFactory.create(codec), version);
                long id = 0;
                for (String category : new String[] {null, "a", "b"}) {
                    for (Integer item : new Integer[] {null, -2, 0, 1, 2}) {
                        for (String tag : new String[] {null, "", "z"}) {
                            GenericRow key = GenericRow.of(
                                    category == null ? null : BinaryString.fromString(category), item,
                                    tag == null ? null : BinaryString.fromString(tag));
                            writer.write(key, id);
                            writer.write(key, id + 1000);
                            id++;
                        }
                    }
                }
                ResultEntry result = writer.finish().get(0);
                String name = "btree_composite_v" + version + "_java_" + codec.name().toLowerCase();
                Files.write(Paths.get(args[0], name + ".meta"), result.meta());
                Files.write(Paths.get(args[0], name + ".bin"),
                        bytes.toByteArray());
            }
        }
    }
}
