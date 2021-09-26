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

package org.apache.cassandra.io.sstable.format;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.CipherFactoryTest;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.ByteBufferUtil;

public class EncryptedSummaryTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    @Test
    public void roundTrip() throws IOException
    {
        File f = File.createTempFile("enc-summary-" + UUID.randomUUID(), ".db");
        f.deleteOnExit();
        EncryptionContext context = EncryptionContextGenerator.createContext();
        CompressionParams params = CompressionParams.DEFAULT;

        ByteBuffer buf = ByteBufferUtil.bytes(CipherFactoryTest.ULYSSEUS);
        try (DataOutputStreamPlus outputStream = new BufferedDataOutputStreamPlus(
                                                     new EncryptedSummaryWritableByteChannel(
                                                         new FileOutputStream(f), params, context)))
        {
            ByteBufferUtil.bytes(CipherFactoryTest.ULYSSEUS);
            outputStream.write(buf);
        }

        ByteBuffer readBuf = ByteBuffer.allocate(buf.capacity());
        try (DataInputStream inputStream = new DataInputStream(
                                              new EncryptedSummaryInputStream(
                                                  new FileInputStream(f), params, context)))
        {
            inputStream.read(readBuf.array());
            Assert.assertEquals(buf, readBuf);
            Assert.assertEquals(0, inputStream.available());
        }
    }
}
