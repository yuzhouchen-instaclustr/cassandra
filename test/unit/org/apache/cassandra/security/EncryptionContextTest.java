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
package org.apache.cassandra.security;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.RandomAccessReader;

public class EncryptionContextTest
{
    private final Random random = new Random();
    private EncryptionContext encryptionContext;

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        encryptionContext = EncryptionContextGenerator.createContext();
    }

    @Test
    public void encrypt() throws BadPaddingException, ShortBufferException, IllegalBlockSizeException, IOException
    {
        byte[] buf = new byte[(1 << 12) - 7];
        random.nextBytes(buf);

        // encrypt
        File f = File.createTempFile("commitlog-enc-utils-", ".tmp");
        f.deleteOnExit();

        FileChannel channel = new RandomAccessFile(f, "rw").getChannel();
        encryptionContext.encryptAndWrite(ByteBuffer.wrap(buf), channel);
        channel.close();
        // decrypt
        ByteBuffer decryptedBuffer = encryptionContext.decrypt(RandomAccessReader.open(f), ByteBuffer.allocate(0), true);
        decryptedBuffer.flip();
        Assert.assertEquals(buf.length, decryptedBuffer.remaining());
        byte[] ret = new byte[decryptedBuffer.remaining()];
        decryptedBuffer.get(ret);
        Assert.assertArrayEquals(buf, ret);

    }
}
