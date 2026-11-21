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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * Deterministic Java fixtures for Rust ↔ Java sketch/bitmap aggregator
 * compatibility tests. Values are fixed ranges — never random.
 *
 * Usage (classpath from this directory's pom.xml, same versions as paimon-common):
 *   mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt
 *   javac -cp "$(cat cp.txt)" GenerateSketchBitmapCompat.java
 *   java -cp ".:$(cat cp.txt)" GenerateSketchBitmapCompat .
 */
public class GenerateSketchBitmapCompat {

    public static void main(String[] args) throws Exception {
        Path out = Paths.get(args.length > 0 ? args[0] : ".");

        writeRbm32(out);
        writeRbm64(out);
        writeHll(out);
        writeTheta(out);
    }

    private static void writeRbm32(Path out) throws Exception {
        write(out, "compat_rbm32_empty.bin", serialize32(new RoaringBitmap()));

        RoaringBitmap a = bitmap32(1, 2, 2, 3, 100);
        RoaringBitmap b = bitmap32(3, 4, 5, 100);
        RoaringBitmap union = a.clone();
        union.or(b);
        write(out, "compat_rbm32_a.bin", serialize32(a));
        write(out, "compat_rbm32_b.bin", serialize32(b));
        write(out, "compat_rbm32_union.bin", serialize32(union));

        RoaringBitmap bounds = bitmap32(0, Integer.MAX_VALUE, -1);
        write(out, "compat_rbm32_bounds.bin", serialize32(bounds));

        RoaringBitmap dups = bitmap32(7, 7, 7, 8, 8);
        write(out, "compat_rbm32_dups.bin", serialize32(dups));
    }

    private static void writeRbm64(Path out) throws Exception {
        write(out, "compat_rbm64_empty.bin", serialize64(new Roaring64Bitmap()));

        Roaring64Bitmap a = bitmap64(1L, 2L, 2L, 3L, (1L << 32) + 5L);
        Roaring64Bitmap b = bitmap64(3L, 4L, (1L << 32) + 5L, (1L << 40));
        Roaring64Bitmap union = a.clone();
        union.or(b);
        write(out, "compat_rbm64_a.bin", serialize64(a));
        write(out, "compat_rbm64_b.bin", serialize64(b));
        write(out, "compat_rbm64_union.bin", serialize64(union));
    }

    private static void writeHll(Path out) throws Exception {
        HllSketch a = new HllSketch(12, TgtHllType.HLL_4);
        HllSketch b = new HllSketch(12, TgtHllType.HLL_4);
        for (int v = 0; v < 200; v++) {
            a.update(v);
        }
        for (int v = 100; v < 300; v++) {
            b.update(v);
        }
        write(out, "compat_hll_a.bin", a.toCompactByteArray());
        write(out, "compat_hll_b.bin", b.toCompactByteArray());

        Union union = new Union(12);
        union.update(a);
        union.update(b);
        HllSketch merged = union.getResult(TgtHllType.HLL_4);
        write(out, "compat_hll_union.bin", merged.toCompactByteArray());

        // Empty sketch (no updates).
        write(out, "compat_hll_empty.bin", new HllSketch(12, TgtHllType.HLL_4).toCompactByteArray());
    }

    private static void writeTheta(Path out) throws Exception {
        UpdateSketch a = UpdateSketch.builder().setNominalEntries(16).build();
        UpdateSketch b = UpdateSketch.builder().setNominalEntries(16).build();
        for (int v = 0; v < 10; v++) {
            a.update(v);
        }
        for (int v = 5; v < 15; v++) {
            b.update(v);
        }
        byte[] aBytes = a.compact().toByteArray();
        byte[] bBytes = b.compact().toByteArray();
        write(out, "compat_theta_a.bin", aBytes);
        write(out, "compat_theta_b.bin", bBytes);

        org.apache.datasketches.theta.Union union =
                Sketches.setOperationBuilder().buildUnion();
        union.union(org.apache.datasketches.memory.Memory.wrap(aBytes));
        union.union(org.apache.datasketches.memory.Memory.wrap(bBytes));
        write(out, "compat_theta_union.bin", union.getResult().toByteArray());

        write(
                out,
                "compat_theta_empty.bin",
                UpdateSketch.builder().setNominalEntries(16).build().compact().toByteArray());
    }

    private static RoaringBitmap bitmap32(int... values) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int value : values) {
            bitmap.add(value);
        }
        bitmap.runOptimize();
        return bitmap;
    }

    private static Roaring64Bitmap bitmap64(long... values) {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long value : values) {
            bitmap.addLong(value);
        }
        bitmap.runOptimize();
        return bitmap;
    }

    private static byte[] serialize32(RoaringBitmap bitmap) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
        bitmap.serialize(buffer);
        return buffer.array();
    }

    private static byte[] serialize64(Roaring64Bitmap bitmap) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bitmap.serialize(new DataOutputStream(bytes));
        return bytes.toByteArray();
    }

    private static void write(Path dir, String name, byte[] bytes) throws Exception {
        Files.write(dir.resolve(name), bytes);
        System.out.println(name + " " + bytes.length);
    }
}
