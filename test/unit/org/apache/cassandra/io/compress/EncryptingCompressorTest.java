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

package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.ByteBufferUtil;

public class EncryptingCompressorTest
{
    private static final String FINNEGANS_WAKE = "riverrun, past Eve and Adam's, from swerve of shore to bend of bay, " +
                                                 "brings us by a commodius vicus of recirculation back to Howth Castle and Environs.";

    EncryptingCompressor encryptingCompressor;
    EncryptingCompressor decryptingCompressor;

    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        TransparentDataEncryptionOptions tdeOptions = EncryptionContextGenerator.createEncryptionOptions();
        Map<String, String> map = new HashMap<>();
        map.put(EncryptionContext.ENCRYPTION_KEY_ALIAS, tdeOptions.key_alias);
        map.put(EncryptionContext.ENCRYPTION_CIPHER, tdeOptions.cipher);

        EncryptionContext encryptionContext = EncryptionContextGenerator.createContext();
        DatabaseDescriptor.setEncryptionContext(encryptionContext);
        encryptingCompressor = EncryptingCompressor.create(map);

        decryptingCompressor = EncryptingCompressor.create(map);
    }

    @Test
    public void roundtrip_Text_fromBuffer() throws IOException
    {
        ByteBuffer encrypted = encrypt(FINNEGANS_WAKE.getBytes(Charset.forName("UTF-8")));

        ByteBuffer decrypted = ByteBuffer.allocate(1024);
        decryptingCompressor.uncompress(encrypted, decrypted);
        decrypted.flip();
        String retVal = ByteBufferUtil.string(decrypted, Charset.forName("UTF-8"));
        Assert.assertEquals(FINNEGANS_WAKE, retVal);
    }

    @Test
    public void roundtrip_Text_fromByteArray() throws IOException
    {
        ByteBuffer encrypted = encrypt(FINNEGANS_WAKE.getBytes(Charset.forName("UTF-8")));

        byte[] decrypted = new byte[1024];
        int len = decryptingCompressor.uncompress(encrypted.array(), 0, encrypted.remaining(), decrypted, 0);
        String retVal = new String(decrypted, 0, len, Charset.forName("UTF-8"));
        Assert.assertEquals(FINNEGANS_WAKE, retVal);
    }

    private ByteBuffer encrypt(byte[] data) throws IOException
    {
        ByteBuffer input = ByteBuffer.wrap(data);
        ByteBuffer encrypted = ByteBuffer.allocate(encryptingCompressor.initialCompressedBufferLength(data.length));
        encryptingCompressor.compress(input, encrypted);
        encrypted.flip();

        return encrypted;
    }

    @Test
    public void roundtrip_ByteArray_fromBuffer() throws IOException
    {
        SecureRandom secureRandom = new SecureRandom();
        byte[] b = new byte[(1 << 10) - 23];
        secureRandom.nextBytes(b);
        ByteBuffer encrypted = encrypt(b);

        ByteBuffer decrypted = ByteBuffer.allocate(1024);
        decryptingCompressor.uncompress(encrypted, decrypted);
        byte[] retVal = new byte[b.length];
        System.arraycopy(decrypted.array(), 0, retVal, 0, retVal.length);
        Assert.assertArrayEquals(b, retVal);
    }

    @Test
    public void roundtrip_ByteArray_fromByteArray() throws IOException
    {
        SecureRandom secureRandom = new SecureRandom();
        byte[] b = new byte[(1 << 10) - 23];
        secureRandom.nextBytes(b);
        ByteBuffer encrypted = encrypt(b);

        byte[] decrypted = new byte[1024];
        int len = decryptingCompressor.uncompress(encrypted.array(), 0, encrypted.remaining(), decrypted, 0);

        byte[] truncated = new byte[len];
        System.arraycopy(decrypted, 0, truncated, 0, len);
        Assert.assertArrayEquals(b, truncated);
    }
}
