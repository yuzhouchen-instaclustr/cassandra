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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.ByteBufferUtil;

class EncryptedSummaryInputStream extends InputStream implements AutoCloseable
{
    private final FileChannel channel;
    private final EncryptionContext encryptionContext;
    private ByteBuffer workingBuffer;

    EncryptedSummaryInputStream(FileInputStreamPlus file, CompressionParams compressionParams, EncryptionContext baseEncryptionContext)
    {
        channel = file.getChannel();
        encryptionContext = EncryptionContext.create(baseEncryptionContext.getTransparentDataEncryptionOptions(), compressionParams, compressionParams.getSstableCompressor());
        workingBuffer = ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    public int read() throws IOException
    {
        if (workingBuffer == null || workingBuffer.remaining() == 0)
        {
            if (channel.position() == channel.size())
                return -1;
            rebuffer();
        }
        return workingBuffer.get() & 0xFF;
    }

    private void rebuffer() throws IOException
    {
        workingBuffer = encryptionContext.decrypt(channel, workingBuffer, true);
        workingBuffer.flip();
    }

    public void close() throws IOException
    {
        workingBuffer = null;
        channel.close();
    }
}
