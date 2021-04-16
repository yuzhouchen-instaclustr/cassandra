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

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.XXHashPartitioner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1,jvmArgsAppend = { "-Xmx512M", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class PartitionersBench {

    private static final IPartitioner partitioner = new XXHashPartitioner();
    private static final IPartitioner partitioner2 = new Murmur3Partitioner();

    private static final Random random = new Random(12345678);

    // intentionally not on power-of-2 values
    @Param({ "31", "131", "517", "2041" })
    private int bufferSize;

    private byte[] array;

    @Setup
    public void setup() throws NoSuchAlgorithmException
    {
        array = new byte[bufferSize];
        random.nextBytes(array);
    }

    @Benchmark
    public Token benchXXHashPartitioner()
    {
        return partitioner.getToken(ByteBuffer.wrap(array));
    }

    @Benchmark
    public Token benchMurmur3Partitioner()
    {
        return partitioner2.getToken(ByteBuffer.wrap(array));
    }
}
