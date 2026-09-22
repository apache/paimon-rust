/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.rangebitmap.RangeBitmapFileIndex;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;

import java.security.MessageDigest;

/** Generates the SHA-256 golden payloads used by the Rust writer tests. */
public class GenerateRangeBitmap {
    private static void emit(String name, DataType type, String chunkSize, Object[] values)
            throws Exception {
        Options options = new Options();
        options.setString("chunk-size", chunkSize);
        FileIndexWriter writer = new RangeBitmapFileIndex(type, options).createWriter();
        for (Object value : values) {
            writer.writeRecord(value);
        }
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(writer.serializedBytes());
        StringBuilder hex = new StringBuilder();
        for (byte b : digest) {
            hex.append(String.format("%02x", b & 0xff));
        }
        System.out.println(name + " " + hex);
    }

    public static void main(String[] args) throws Exception {
        emit("empty", new IntType(), "0b", new Object[] {});
        emit("nulls", new IntType(), "0b", new Object[] {null, null, null});
        emit("singleton", new IntType(), "0b", new Object[] {7, null, 7});
        Object[] ints = {9, -1, 3, null, 1, 7, 5, 3, Integer.MIN_VALUE, Integer.MAX_VALUE};
        emit("int-zero", new IntType(), "0b", ints);
        emit("int-chunks", new IntType(), "8b", ints);
        Object[] strings = {BinaryString.fromString("z"), BinaryString.fromString(""),
            BinaryString.fromString("a\u0000"), null, BinaryString.fromString("\u4f60\u597d"),
            BinaryString.fromString("ab"), BinaryString.fromString("abc"),
            BinaryString.fromString("\ud83e\udd80")};
        emit("string-chunks", new VarCharType(), "8b", strings);
        Object[] containers = new Object[70000];
        for (int i = 0; i < containers.length; i++) {
            containers[i] = i % 11 == 0 ? null : (i * 37) % 101;
        }
        emit("containers", new IntType(), "12b", containers);
    }
}
