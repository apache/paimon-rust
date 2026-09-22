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
import org.apache.paimon.globalindex.KeySerializer;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.IntType;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/** Generate compatibility fixtures with the Java production BTree V2 writer. */
public class GenerateBTreeV2 {
    public static void main(String[] args) throws Exception {
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
            BTreeIndexWriter writer = new BTreeIndexWriter(
                    files, KeySerializer.create(new IntType()), 64, null,
                    BlockCompressionFactory.create(codec), 2);
            writer.write(0, 7);
            for (long id : new long[] {2, 130, 65538, (1L << 32) + 5}) { writer.write(1, id); }
            for (long id = 60000; id < 140000; id++) { writer.write(2, id); }
            for (long id = (1L << 32) - 10; id < (1L << 32) + 10000; id++) { writer.write(3, id); }
            writer.write(4, Long.MAX_VALUE);
            // Repeated deltas make this block genuinely compressible with LZ4.
            for (long id = 0; id < 4096; id++) { writer.write(5, (1L << 40) + id * 128); }
            writer.write(null, 4);
            writer.write(null, (1L << 32) + 20000);
            writer.finish();
            Files.write(Paths.get(args[0], "btree_v2_java_" + codec.name().toLowerCase() + ".bin"),
                    bytes.toByteArray());
        }
    }
}
