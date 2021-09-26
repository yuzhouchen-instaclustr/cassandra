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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionContext;

/**
 * Writes out an encrypted version of the summary file data.
 * As the encryption algorithm (cipher) and key_alias is inherited from the owing sstable, we just encrypt and write out
 * each block as it is passed in.
 */
class EncryptedSummaryWritableByteChannel implements WritableByteChannel, AutoCloseable
{
    private final FileChannel channel;
    private final EncryptionContext encryptionContext;

    EncryptedSummaryWritableByteChannel(FileOutputStream fos, CompressionParams params, EncryptionContext baseEncryptionContext)
    {
        encryptionContext = EncryptionContext.create(baseEncryptionContext.getTransparentDataEncryptionOptions(),
                                                     params, params.getSstableCompressor());
        channel = fos.getChannel();
    }

    public int write(ByteBuffer src) throws IOException
    {
        return encryptionContext.encryptAndWrite(src, channel);
    }

    public boolean isOpen()
    {
        return channel.isOpen();
    }

    public void close() throws IOException
    {
        channel.close();
    }
}
