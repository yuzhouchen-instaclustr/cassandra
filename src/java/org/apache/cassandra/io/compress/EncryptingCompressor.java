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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.crypto.Cipher;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
//import org.apache.cassandra.io.util.CompressedSegmentedFile;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.security.EncryptionContext;

import static org.apache.cassandra.security.EncryptionContext.ENCRYPTION_CIPHER;
import static org.apache.cassandra.security.EncryptionContext.ENCRYPTION_KEY_ALIAS;

/**
 * Custom {@link ICompressor} implementation that performs encryption in addition to compression.
 * Unlike other {@link ICompressor} implementations, this class requires state to be maintained
 * (the current {@link EncryptionContext}), and thus needs a bit of special care.
 *
 * Each sstable will need a unique instance of this class, as the metadata about encryption will be different for each.
 * Further, as each sstable can be read by multiple threads, we need thread-local instances of the {@link Cipher},
 * as Cipher is not thread-safe (it's not documented that Cipher is not thread safe, but a simple test run wil bear that out).
 * As a last wrinkle, when an sstable is being written, and subsequently opened (either early, or otherwise),
 * the instance of this class will be reused from writing to reading stages (see {@link CompressedSegmentedFile.Builder#metadata(String, long)}.
 */
public class EncryptingCompressor implements ICompressor
{
    private static final Set<String> ENCRYPTION_OPTIONS = ImmutableSet.of(ENCRYPTION_CIPHER, ENCRYPTION_KEY_ALIAS);

    private final EncryptionContext encryptionContext;

    private EncryptingCompressor(Map<String, String> options, EncryptionContext baseEncryptionContext)
    {
        if (!options.containsKey(ENCRYPTION_CIPHER) || !options.containsKey(ENCRYPTION_KEY_ALIAS))
            throw new IllegalArgumentException("must set both the cipher algorithm and key alias");

        encryptionContext = EncryptionContext.createFromMap(options, baseEncryptionContext);
    }

    public EncryptingCompressor(CompressionParams params, ICompressor compressor, EncryptionContext baseEncryptionContext)
    {
        encryptionContext = EncryptionContext.create(baseEncryptionContext.getTransparentDataEncryptionOptions(), params, compressor);
    }

    public static EncryptingCompressor create(Map<String, String> compressionOptions)
    {
        return new EncryptingCompressor(compressionOptions, DatabaseDescriptor.getEncryptionContext());
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        return encryptionContext.encryptedBufferLength(chunkLength);
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        encryptionContext.encrypt(input, output);
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        encryptionContext.decrypt(input, output, false);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        ByteBuffer inBuf = ByteBuffer.wrap(input, inputOffset, inputLength);
        ByteBuffer outBuf = ByteBuffer.wrap(output, outputOffset, output.length - outputOffset);
        encryptionContext.decrypt(inBuf, outBuf, false);
        return outBuf.limit() - outputOffset;
    }

    public Set<String> supportedOptions()
    {
        Set<String> compressorOpts = encryptionContext.getCompressor().supportedOptions();
        Set<String> opts = new HashSet<>(ENCRYPTION_OPTIONS.size() + compressorOpts.size());
        opts.addAll(compressorOpts);
        opts.addAll(ENCRYPTION_OPTIONS);
        return opts;
    }

    public boolean isEncrypting()
    {
        return true;
    }

    public boolean supports(BufferType bufferType)
    {
        return true;
    }

    public BufferType preferredBufferType()
    {
        return BufferType.ON_HEAP;
    }
}
