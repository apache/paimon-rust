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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.roaringbitmap.longlong.Roaring64Bitmap;

/** Generate a run-compressed Roaring64Bitmap 1.2.1 fixture for Rust union tests. */
public class GenerateRoaring64Dense {

    public static void main(String[] args) throws Exception {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        bitmap.add(0L, 1_000_000L);
        bitmap.runOptimize();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bitmap.serialize(new DataOutputStream(bytes));
        Files.write(Paths.get(args[0]), bytes.toByteArray());
    }
}
