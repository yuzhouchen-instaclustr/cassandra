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

package org.apache.cassandra.schema;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

public class CompressionParamsTest
{
    private static final int MESSAGING_VERSION = MessagingService.current_version;

    @Test
    public void serialize_Unencrypted() throws IOException
    {
        serialize(false);
    }

    @Test
    public void serialize_Encrypted() throws IOException
    {
        serialize(true);
    }

    void serialize(boolean encrypted) throws IOException
    {
        CompressionParams params = CompressionParams.DEFAULT;
        params.setEncrypted(encrypted);
        long size = CompressionParams.serializer.serializedSize(params, MESSAGING_VERSION);

        DataOutputBuffer dataOutputBuffer = new DataOutputBuffer(1024);
        CompressionParams.serializer.serialize(params, dataOutputBuffer, MESSAGING_VERSION);
        ByteBuffer serializedBuffer = dataOutputBuffer.buffer();
        Assert.assertEquals(size, serializedBuffer.remaining());

        CompressionParams result = CompressionParams.serializer.deserialize(new DataInputBuffer(serializedBuffer, true), MESSAGING_VERSION);
        Assert.assertEquals(params, result);
        Assert.assertEquals(encrypted, result.isEncrypted());
    }
}
