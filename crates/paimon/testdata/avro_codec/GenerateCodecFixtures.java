/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;

/** Produces the same records and block boundaries as generate.py. */
public final class GenerateCodecFixtures {
    public static void main(String[] args) throws Exception {
        File directory = new File(args[0]);
        Schema schema = new Schema.Parser().parse(new File(directory, "records.avsc"));
        byte[] sync = new byte[16];
        for (int i = 0; i < sync.length; i++) {
            sync[i] = (byte) i;
        }
        byte[] payload = new byte[32];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        for (String codec : new String[] {"bzip2", "xz"}) {
            try (DataFileWriter<GenericRecord> writer =
                    new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                writer.setCodec(CodecFactory.fromString(codec));
                writer.create(
                        schema,
                        new FileOutputStream(new File(directory, "java_" + codec + ".avro")),
                        sync);
                append(writer, schema, -7L, "before-epoch", new byte[] {0, 1, (byte) 255});
                append(writer, schema, 0L, "\u4e8c", new byte[0]);
                append(writer, schema, Long.MAX_VALUE,
                        "end" + new String(new char[96]).replace('\0', 'x'), payload);
            }
        }
    }

    private static void append(
            DataFileWriter<GenericRecord> writer,
            Schema schema,
            long id,
            String label,
            byte[] payload) throws Exception {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("label", label);
        record.put("payload", ByteBuffer.wrap(payload));
        writer.append(record);
        writer.sync();
    }
}
