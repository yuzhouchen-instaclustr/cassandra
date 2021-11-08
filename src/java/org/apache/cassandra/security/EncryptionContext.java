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

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

/**
 * A (largely) immutable wrapper for the application-wide file-level encryption settings.
 */
public class EncryptionContext
{
    public static final String ENCRYPTION_CIPHER = "encCipher";
    public static final String ENCRYPTION_KEY_ALIAS = "encKeyAlias";
    public static final String ENCRYPTION_IV = "encIV";
    public static final String COMPRESSION_CLASS_NAME = "compressionClass";
    static final String COMPRESSION_PARAMETERS_KEY = "compressionParameters";

    private static final int COMPRESSED_BLOCK_HEADER_SIZE = 4;
    private static final int ENCRYPTED_BLOCK_HEADER_SIZE = 9;

    private final TransparentDataEncryptionOptions tdeOptions;
    private final ICompressor compressor;
    private final CipherFactory cipherFactory;

    /**
     * We need the compression parameters if we serialize this instance.
     */
    private final ParameterizedClass compressorClass;

    private final int chunkLength;

    public EncryptionContext()
    {
        this(new TransparentDataEncryptionOptions());
    }

    public EncryptionContext(TransparentDataEncryptionOptions tdeOptions)
    {
        this(tdeOptions,
             new ParameterizedClass(CompressionParams.DEFAULT.getSstableCompressor().getClass().getName(), CompressionParams.DEFAULT.getOtherOptions()),
             CompressionParams.DEFAULT.getSstableCompressor());
    }

    @VisibleForTesting
    EncryptionContext(TransparentDataEncryptionOptions tdeOptions, ParameterizedClass compressorClass, ICompressor compressor)
    {
        assert tdeOptions != null;
        this.tdeOptions = tdeOptions;
        this.compressorClass = compressorClass;
        this.compressor = compressor;

        chunkLength = tdeOptions.chunk_length_kb * 1024;

        CipherFactory factory = null;
        if (tdeOptions.enabled)
        {
            try
            {
                factory = CipherFactory.instance(tdeOptions);
            }
            catch (Exception e)
            {
                throw new ConfigurationException("failed to load key provider for transparent data encryption", e);
            }
        }
        cipherFactory = factory;
    }

    public ICompressor getCompressor()
    {
        return compressor;
    }

    /**
     * Retrieves an instance of {@link Cipher}, possibly with a new initialization vector (IV).
     */
    public Cipher getEncryptor(boolean reinitialize) throws IOException
    {
        return cipherFactory.getEncryptor(tdeOptions.cipher, tdeOptions.key_alias, reinitialize);
    }

    /**
     * Retrieves an instance of {@link Cipher}, using the provided initialization vector ({@code iv}).
     */
    private Cipher getDecryptor(@Nullable byte[] iv) throws IOException
    {
        return cipherFactory.getDecryptor(tdeOptions.cipher, tdeOptions.key_alias, iv);
    }

    public Cipher getEncryptor() throws IOException
    {
        return cipherFactory.getEncryptor(tdeOptions.cipher, tdeOptions.key_alias);
    }

    public boolean isEnabled()
    {
        return tdeOptions.enabled;
    }

    public int getChunkLength()
    {
        return chunkLength;
    }

    public TransparentDataEncryptionOptions getTransparentDataEncryptionOptions()
    {
        return tdeOptions;
    }

    public boolean equals(Object o)
    {
        return o instanceof EncryptionContext && equals((EncryptionContext) o);
    }

    public boolean equals(EncryptionContext other)
    {
        return Objects.equal(tdeOptions, other.tdeOptions)
	       && Objects.equal(compressor, other.compressor);
    }

    public Map<String, String> toHeaderParameters()
    {
        Map<String, String> map = new HashMap<>();
        // add compression options, someday ...
        if (tdeOptions.enabled)
        {
            map.put(ENCRYPTION_CIPHER, tdeOptions.cipher);
            map.put(ENCRYPTION_KEY_ALIAS, tdeOptions.key_alias);
        }
        return map;
    }
     
     public static EncryptionContext create(TransparentDataEncryptionOptions tdeOptions, CompressionParams compressionParams, ICompressor compressor)
    {
        ParameterizedClass compressorClass = new ParameterizedClass(compressor.getClass().getName(), compressionParams.getOtherOptions());
        return new EncryptionContext(tdeOptions, compressorClass, compressor);
    }

    public static EncryptionContext create(TransparentDataEncryptionOptions tdeOptions, CompressionParams compressionParams)
    {
        ParameterizedClass compressorClass = new ParameterizedClass(compressionParams.getSstableCompressor().getClass().getName(), compressionParams.getOtherOptions());
        return new EncryptionContext(tdeOptions, compressorClass, compressionParams.getSstableCompressor());
    }

    /**
     * Create a new instance based with compression data from {@code compressionClass}. If no compression data is provided,
     * will use the default compressor from {@link #defaultCompressor()}.
     */
    public static EncryptionContext create(TransparentDataEncryptionOptions tdeOptions, @Nullable ParameterizedClass compressionClass)
    {
        ICompressor compressor;
        if (compressionClass != null)
        {
            compressor = CompressionParams.createCompressor(compressionClass);
        }
        else
        {
            Pair<ParameterizedClass, ICompressor> compressorPair = defaultCompressor();
            compressionClass = compressorPair.left;
            compressor = compressorPair.right;
        }

        return new EncryptionContext(tdeOptions, compressionClass, compressor);
    }

    /**
     * If encryption headers are found in the {@code parameters}, those headers are merged with the {@code encryptionContext}
     * to create a new instance. Further, the headers are inspected for any compression entries, which are used if found.
     */
    public static EncryptionContext createFromMap(Map<?, ?> parameters, EncryptionContext encryptionContext)
    {
        if (parameters == null || parameters.isEmpty())
            return new EncryptionContext(new TransparentDataEncryptionOptions(false));

        Map<String, String> params = new HashMap<>();
        params.putAll((Map<String, String>) parameters);
        String keyAlias = params.remove(ENCRYPTION_KEY_ALIAS);
        String cipher = params.remove(ENCRYPTION_CIPHER);
        if (keyAlias == null || cipher == null)
            return new EncryptionContext(new TransparentDataEncryptionOptions(false));

        TransparentDataEncryptionOptions tdeOptions = new TransparentDataEncryptionOptions(cipher, keyAlias, encryptionContext.getTransparentDataEncryptionOptions().key_provider);
        String compressionClassName = params.remove(COMPRESSION_CLASS_NAME);
        ParameterizedClass compressor = null;

        if (compressionClassName != null)
            compressor = new ParameterizedClass(compressionClassName, params);

        return create(tdeOptions, compressor);
    }

    @VisibleForTesting
    static Pair<ParameterizedClass, ICompressor> defaultCompressor()
    {
        CompressionParams cp = CompressionParams.DEFAULT;
        return Pair.create(new ParameterizedClass(cp.getSstableCompressor().getClass().getName(), cp.getOtherOptions()),
                           cp.getSstableCompressor());
    }

    public int encryptedBufferLength(int chunkLength)
    {
        int compressedInitialSize = compressor.initialCompressedBufferLength(chunkLength) + COMPRESSED_BLOCK_HEADER_SIZE;

        try
        {
            Cipher cipher = getEncryptor(true);
            return cipher.getOutputSize(compressedInitialSize)
                   + EncryptionContext.ENCRYPTED_BLOCK_HEADER_SIZE
                   + cipher.getIV().length;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Encrypt the input data, and writes out to the same input buffer; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer.
     * Writes the cipher text and headers out to the channel, as well.
     *
     * Note: channel is a parameter as we cannot write header info to the output buffer as we assume the input and output
     * buffers can be the same buffer (and writing the headers to a shared buffer will corrupt any input data). Hence,
     * we write out the headers directly to the channel, and then the cipher text (once encrypted).
     *
     * @return the buffer that encrypted data was written to (which may have been the input buffer), and the number of bytes written.
     */
    public int encryptAndWrite(ByteBuffer inputBuffer, WritableByteChannel channel) throws IOException
    {
        return encryptAndWrite(inputBuffer, channel, null);
    }

    public int encryptAndWrite(ByteBuffer inputBuffer, WritableByteChannel channel, Consumer<ByteBuffer> outBufferFunction) throws IOException
    {
        int inputLength = inputBuffer.remaining();
        final int compressedLength = compressor.initialCompressedBufferLength(inputLength);
        final int compressedBufferSize = (int)(compressedLength * 1.2) + COMPRESSED_BLOCK_HEADER_SIZE;
        BufferPool bufferPool = BufferPools.forNetworking();
        ByteBuffer compressedBuffer = bufferPool.get(compressedBufferSize, BufferType.typeOf(inputBuffer));
        boolean reallocatedOutputBuffer = false;
        ByteBuffer outputBuffer = null;
        try
        {
            compressedBuffer.putInt(inputLength);
            compressor.compress(inputBuffer, compressedBuffer);
            compressedBuffer.flip();

            final Cipher cipher = getEncryptor(true);
            final int plainTextLength = compressedBuffer.remaining();
            final int encryptLength = cipher.getOutputSize(plainTextLength);

            int serializedIvLen = ivLength(cipher);
            final int headerSize = ENCRYPTED_BLOCK_HEADER_SIZE + serializedIvLen;
            // it's unfortunate that we need to grab a small buffer here just for the header, but if we reuse the #inputBuffer
            // for the output, then we would overwrite the first n bytes of the real data with the header data.
            ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize);
            headerBuffer.putInt(encryptLength);
            headerBuffer.putInt(plainTextLength);
            headerBuffer.put((byte) serializedIvLen);
            if (serializedIvLen > 0)
                headerBuffer.put(cipher.getIV());
            headerBuffer.flip();
            channel.write(headerBuffer);
             outputBuffer = compressedBuffer.duplicate();
            if (outputBuffer.capacity() < encryptLength)
            {
                outputBuffer = bufferPool.get(encryptLength, BufferType.typeOf(compressedBuffer));
                reallocatedOutputBuffer = true;
            }
            else
            {
                outputBuffer.position(0).limit(encryptLength);
            }
            try
            {
                cipher.doFinal(compressedBuffer, outputBuffer);
            }
            catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e)
            {
                throw new IOException("failed to encrypt commit log block", e);
            }

            outputBuffer.position(0).limit(encryptLength);
            channel.write(outputBuffer);

            if (outBufferFunction != null)
            {
                outputBuffer.position(0).limit(encryptLength);
                outBufferFunction.accept(outputBuffer.duplicate());
            }

            return headerSize + encryptLength;
        }
        finally
        {
            bufferPool.put(compressedBuffer);
            if (reallocatedOutputBuffer)
                bufferPool.put(outputBuffer);
        }
    }

    /**
     * Determine the length of the initialization vector (IV) for the provided {@link Cipher}.
     */
    private static int ivLength(Cipher cipher)
    {
        byte[] iv = cipher.getIV();
        if (iv != null)
            return iv.length;
        return 0;
    }

    public int encrypt(ByteBuffer inputBuffer, ByteBuffer outputBuffer) throws IOException
    {
        Preconditions.checkNotNull(outputBuffer, "output buffer may not be null");
        try (WritableByteChannel wbc = new ChannelAdapter(outputBuffer))
        {
            return encryptAndWrite(inputBuffer, wbc);
        }
    }

    /**
     * Decrypt the input data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer.
     *
     * @return the byte buffer that was actaully written to; it may be the {@code outputBuffer} if it had enough capacity,
     * or it may be a new, larger instance. Callers should capture the return buffer (if calling multiple times).
     */
    public ByteBuffer decrypt(ReadableByteChannel channel, ByteBuffer outputBuffer, boolean allowBufferResize) throws IOException
    {
        // a best-guess as to the max size of IV
        final int maxIvLength = 32;
        BufferPool bufferPool = BufferPools.forNetworking();
        ByteBuffer metadataBuffer = bufferPool.get(maxIvLength, BufferType.ON_HEAP);
        final int encryptedLength, compressedLength;
        Cipher cipher;
        try
        {
            metadataBuffer.limit(ENCRYPTED_BLOCK_HEADER_SIZE);
            int readCount = channel.read(metadataBuffer);
            if (readCount < ENCRYPTED_BLOCK_HEADER_SIZE)
                throw new IOException("could not read encrypted blocked metadata header");
            metadataBuffer.flip();
            encryptedLength = metadataBuffer.getInt();
            compressedLength = metadataBuffer.getInt();

            byte ivLen = metadataBuffer.get();
            if (ivLen < 0 || ivLen > maxIvLength)
                throw new IllegalArgumentException("IV length out of normal/expected bounds: " + ivLen);

            if (ivLen > 0)
            {
                byte[] iv = new byte[ivLen];
                metadataBuffer.position(0).limit(ivLen);
                readCount = channel.read(metadataBuffer);
                if (readCount < ivLen)
                    throw new IOException("could not read encrypted blocked's IV");
                metadataBuffer.flip();
                metadataBuffer.get(iv);
                cipher = getDecryptor(iv);
            }
            else
            {
                try
                {
                    cipher = getDecryptor(null);
                }
                catch (Exception e)
                {
                    throw new IllegalStateException("no initialization vector (IV) found in this context");
                }
            }
        }
        finally
        {
            bufferPool.put(metadataBuffer);
        }

        ByteBuffer decryptBuffer = bufferPool.get(Math.max(compressedLength, encryptedLength), BufferType.typeOf(outputBuffer));
        try
        {
            decryptBuffer.position(0).limit(encryptedLength);
            channel.read(decryptBuffer);
            decryptBuffer.flip();

            ByteBuffer dupe = decryptBuffer.duplicate();
            dupe.clear();

            try
            {
                cipher.doFinal(decryptBuffer, dupe);
            }
            catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e)
            {
                throw new IOException("failed to decrypt commit log block", e);
            }

            dupe.position(0).limit(compressedLength);

            int outputLength = dupe.getInt();
            ByteBuffer plainTextBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, outputLength, allowBufferResize);
            compressor.uncompress(dupe, plainTextBuffer);
            return plainTextBuffer;
        }
        finally
        {
            bufferPool.put(decryptBuffer);
        }
    }

    // path used when decrypting sstables
    public ByteBuffer decrypt(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize) throws IOException
    {
        try (ReadableByteChannel rbc = new ByteByfferReadChannel(inputBuffer))
        {
            return decrypt(rbc, outputBuffer, allowBufferResize);
        }
    }

    // path used when decrypting commit log files
    public ByteBuffer decrypt(FileDataInput fileDataInput, ByteBuffer outputBuffer, boolean allowBufferResize) throws IOException
    {
        try (ReadableByteChannel rbc = new DataInputReadChannel(fileDataInput))
        {
            return decrypt(rbc, outputBuffer, allowBufferResize);
        }
    }

    /**
     * A simple {@link java.nio.channels.Channel} adapter for ByteBuffers.
     */
    private static final class ChannelAdapter implements WritableByteChannel
    {
        private final ByteBuffer buffer;

        private ChannelAdapter(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        public int write(ByteBuffer src)
        {
            int count = src.remaining();
            buffer.put(src);
            return count;
        }

        public boolean isOpen()
        {
            return true;
        }

        public void close()
        {
            // nop
        }
    }

    private static class ByteByfferReadChannel implements ReadableByteChannel
    {
        private final ByteBuffer buffer;

        private ByteByfferReadChannel(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        public int read(ByteBuffer dst) throws IOException
        {
            int startPosition = buffer.position();
            ByteBuffer dupe = buffer.duplicate();
            dupe.limit(startPosition + dst.remaining());
            dst.put(dupe);

            buffer.position(startPosition + dst.limit());
            return dst.limit();
        }

        public boolean isOpen()
        {
            return true;
        }

        public void close()
        {
            // nop
        }
    }

    private static class DataInputReadChannel implements ReadableByteChannel
    {
        private final FileDataInput fileDataInput;

        private DataInputReadChannel(FileDataInput dataInput)
        {
            this.fileDataInput = dataInput;
        }

        public int read(ByteBuffer dst) throws IOException
        {
            int readLength = dst.remaining();
            // we should only be performing encrypt/decrypt operations with on-heap buffers, so calling BB.array() should be legit here
            fileDataInput.readFully(dst.array(), dst.position(), readLength);
            dst.position(dst.limit());
            return readLength;
        }

        public boolean isOpen()
        {
            try
            {
                return fileDataInput.isEOF();
            }
            catch (IOException e)
            {
                return true;
            }
        }

        public void close()
        {
            // nop
        }
    }

    public static class ChannelProxyReadChannel implements ReadableByteChannel
    {
        private final ChannelProxy channelProxy;
        private volatile long currentPosition;

        public ChannelProxyReadChannel(ChannelProxy channelProxy, long currentPosition)
        {
            this.channelProxy = channelProxy;
            this.currentPosition = currentPosition;
        }

        public int read(ByteBuffer dst) throws IOException
        {
            int bytesRead = channelProxy.read(dst, currentPosition);
            currentPosition += bytesRead;
            return bytesRead;
        }

        public long getCurrentPosition()
        {
            return currentPosition;
        }

        public boolean isOpen()
        {
            return channelProxy.isCleanedUp();
        }

        public void close()
        {
            // nop
        }
    }
}
