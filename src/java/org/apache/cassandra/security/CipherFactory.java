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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.CompletionException;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;

/**
 * A factory for loading encryption keys from {@link KeyProvider} instances.
 * Maintains a cache of loaded keys to avoid invoking the key provider on every call.
 */
public class CipherFactory
{
    private static final Logger logger = LoggerFactory.getLogger(CipherFactory.class);

    private static final SecureRandom secureRandom;
    /**
     * A cache of keyProvider-specific instances. The cache size will almost always 1, but this cache acts as a memoization
     * mechanism more than anything as it assumes initializing {@link KeyProvider} instances is expensive.
     */
    private static final LoadingCache<FactoryCacheKey, CipherFactory> factories;

    static
    {
        try
        {
            secureRandom = SecureRandom.getInstance("SHA1PRNG");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("unable to create SecureRandom", e);
        }

        factories = Caffeine.newBuilder() // by default cache is unbounded
                            .maximumSize(8) // a value large enough that we should never even get close (so nothing gets evicted)
                            .build(entry -> new CipherFactory(entry.options));
    }

    /**
     * Retains thread-local instances of {@link Cipher} as they are quite expensive to instantiate (via @code Cipher#getInstance).
     * <p>
     * Note: we don't perform reads and writes on the same thread, so we won't have to worry about decrypting versus
     * encrypting ciphers being on the same {@link ThreadLocal}.
     */
    // TODO maybe use a Map<CachedCipher> in case there are multiple, active keys (think of key rotation); else,
    // we'll be thrashing Cipher instances swapping between the various keys/ciphers.
    private static final ThreadLocal<CachedCipher> cachedCiphers = new ThreadLocal<>();

    /**
     * A cache of loaded {@link Key} instances. The cache size is expected to be almost always 1,
     * but this cache acts as a memoization mechanism more than anything as it assumes loading keys is expensive.
     */
    private final LoadingCache<String, Key> cache;
    private final int ivLength;
    private final KeyProvider keyProvider;

    public CipherFactory(TransparentDataEncryptionOptions options)
    {
        logger.info("initializing CipherFactory");
        ivLength = options.iv_length;

        try
        {
            //    secureRandom = SecureRandom.getInstance("SHA1PRNG");
            Class<KeyProvider> keyProviderClass = (Class<KeyProvider>) Class.forName(options.key_provider.class_name);
            Constructor ctor = keyProviderClass.getConstructor(TransparentDataEncryptionOptions.class);
            keyProvider = (KeyProvider) ctor.newInstance(options);
        }
        catch (Exception e)
        {
            throw new RuntimeException("couldn't load cipher factory", e);
        }
        cache = Caffeine.newBuilder() // by default cache is unbounded
                        .maximumSize(64) // a value large enough that we should never even get close (so nothing gets evicted)
                        .executor(MoreExecutors.directExecutor())
                        .removalListener((key, value, cause) ->
                                         {
                                             // maybe reload the key? (to avoid the reload being on the user's dime)
                                             logger.info("key {} removed from cipher key cache", key);
                                         })
                        .build(alias ->
                               {
                                   logger.info("loading secret key for alias {}", alias);
                                   return keyProvider.getSecretKey(alias);
                               });
    }


    public static CipherFactory instance(TransparentDataEncryptionOptions options)
    {
        try
        {
            return factories.get(new FactoryCacheKey(options));
        }
        catch (Exception e)
        {
            throw new RuntimeException("failed to get cipher factory instance");
        }
    }

    /**
     * Retrieve an instance of a {@link Cipher}. If a thread-local cached instance is found, it can be
     * reinitialized with a new initiailization vector.
     * <p>
     * Note: there are cases when we need a reference to a sample Cipher for the given {@code #transformation}
     * (checking the cipher's IV length). In that case we don't need to go through
     * the effort of reinitiailizing the cipher.
     */
    Cipher getEncryptor(String transformation, String keyAlias, boolean reinitialize) throws IOException
    {
        CachedCipher cachedCipher = cachedCiphers.get();
        if (cachedCipher != null)
        {
            boolean differingCipherModes = cachedCipher.mode != Cipher.ENCRYPT_MODE;
            if (logger.isDebugEnabled() && differingCipherModes)
                logger.debug("cached cipher is set for decryption, but we're on the encrypt path");

            if (reinitialize || differingCipherModes)
                reinitEncryptor(cachedCipher.cipher, keyAlias);
            return cachedCipher.cipher;
        }

        Cipher cipher = buildCipher(transformation, keyAlias, generateIv(secureRandom, ivLength), Cipher.ENCRYPT_MODE);
        cachedCiphers.set(new CachedCipher(Cipher.ENCRYPT_MODE, keyAlias, cipher));
        return cipher;
    }

    /**
     * Retrieve an instance of a {@link Cipher}. If a thread-local cached instance is found, it can be
     * reinitialized with a new initiailization vector.
     * <p>
     * Note: there are cases when we need a reference to a sample Cipher for the given {@code #transformation}
     * (checking the cipher's IV length). In that case we don't need to go through
     * the effort of reinitiailizing the cipher.
     *
     * @param iv May be null if the cipher alogrithm ({@code #transformation}) does not require an IV.
     */
    Cipher getDecryptor(String transformation, String keyAlias, @Nullable byte[] iv) throws IOException
    {
        CachedCipher cachedCipher = cachedCiphers.get();
        if (cachedCipher != null)
        {
            boolean differingCipherModes = cachedCipher.mode != Cipher.DECRYPT_MODE;
            if (logger.isDebugEnabled() && differingCipherModes)
                logger.debug("cached cipher is set for encryption, but we're on the decrypt path");

            reinitDecryptor(cachedCipher.cipher, keyAlias, iv);
            return cachedCipher.cipher;
        }

        Cipher cipher = buildCipher(transformation, keyAlias, iv, Cipher.DECRYPT_MODE);
        cachedCiphers.set(new CachedCipher(Cipher.DECRYPT_MODE, keyAlias, cipher));
        return cipher;
    }

    public Cipher getEncryptor(String transformation, String keyAlias) throws IOException
    {
        byte[] iv = new byte[ivLength];
        secureRandom.nextBytes(iv);
        return buildCipher(transformation, keyAlias, iv, Cipher.ENCRYPT_MODE);
    }

    /**
     * Generate a new initialization vector (IV).
     *
     * @return will return null if the {@link #ivLength} is less than 1, which indicates no IV is required.
     */
    private static byte[] generateIv(SecureRandom secureRandom, int ivLength)
    {
        if (ivLength > 0)
        {
            byte[] iv = new byte[ivLength];
            secureRandom.nextBytes(iv);
            return iv;
        }
        return null;
    }

    @VisibleForTesting
    Cipher buildCipher(String transformation, String keyAlias, byte[] iv, int cipherMode) throws IOException
    {
        try
        {
            Key key = retrieveKey(keyAlias);
            Cipher cipher = Cipher.getInstance(transformation);
            int GCM_TAG_LENGTH = 16;
            if (iv != null)
                cipher.init(cipherMode, key, new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv));
            else
                cipher.init(cipherMode, key);
            return cipher;
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidAlgorithmParameterException | InvalidKeyException e)
        {
            logger.error("could not build cipher", e);
            throw new IOException("cannot load cipher", e);
        }
    }

    private Key retrieveKey(String keyAlias) throws IOException
    {
        try
        {
            return cache.get(keyAlias);
        }
        catch (CompletionException e)
        {
            if (e.getCause() instanceof IOException)
                throw (IOException) e.getCause();
            throw new IOException("failed to load key from cache: " + keyAlias, e);
        }
    }

    /**
     * Reinitialize an existing encrypting {@link Cipher} with a new initialization vector. This is more efficient than creating
     * a new instance (via {@link #getEncryptor(String, String, boolean)}.
     */
    void reinitEncryptor(Cipher cipher, String keyAlias) throws IOException
    {
        reinit(cipher, Cipher.ENCRYPT_MODE, keyAlias, null);
    }

    private void reinit(Cipher cipher, int cipherMode, String keyAlias, byte[] iv) throws IOException
    {
        Preconditions.checkNotNull(cipher, "cipher must not be null");
        Preconditions.checkNotNull(keyAlias, "key alias must not be null");

        try
        {
            // create an IV, if necessary
            if (iv == null)
                iv = generateIv(secureRandom, ivLength);

            int GCM_TAG_LENGTH = 16;
            if (iv != null)
                cipher.init(cipherMode, retrieveKey(keyAlias), new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv));
            else
                cipher.init(cipherMode, retrieveKey(keyAlias));
        }
        catch (InvalidKeyException | InvalidAlgorithmParameterException e)
        {
            throw new IOException(e);
        }
    }

    /**
     * Reinitialize an existing decrypting {@link Cipher} with a new initialization vector. This is more efficient than creating
     * a new instance (via {@link #getDecryptor(String, String, byte[])}.
     */
    void reinitDecryptor(Cipher cipher, String keyAlias, byte[] iv) throws IOException
    {
        Preconditions.checkNotNull(iv, "initialization vector must not be null");
        reinit(cipher, Cipher.DECRYPT_MODE, keyAlias, iv);
    }

    private static class FactoryCacheKey
    {
        private final TransparentDataEncryptionOptions options;
        private final String key;

        FactoryCacheKey(TransparentDataEncryptionOptions options)
        {
            this.options = options;
            key = options.key_provider.class_name;
        }

        public boolean equals(Object o)
        {
            return o instanceof FactoryCacheKey && key.equals(((FactoryCacheKey) o).key);
        }

        public int hashCode()
        {
            return key.hashCode();
        }
    }

    /**
     * A simple struct to use with the thread local caching of Cipher as we can't get the mode (encrypt/decrypt) nor
     * key_alias (or key!) from the Cipher itself to use for comparisons
     */
    private static class CachedCipher
    {
        public int mode;
        public final String keyAlias;
        public final Cipher cipher;

        private CachedCipher(int mode, String keyAlias, Cipher cipher)
        {
            this.mode = mode;
            this.keyAlias = keyAlias;
            this.cipher = cipher;
        }
    }
}
