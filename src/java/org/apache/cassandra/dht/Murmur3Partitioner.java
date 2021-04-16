/**
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
package org.apache.cassandra.dht;

import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PreHashedDecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * This class generates a BigIntegerToken using a Murmur3 hash.
 */
public class Murmur3Partitioner extends LongTokenProducingPartitioner
{
    public static final Murmur3Partitioner instance = new Murmur3Partitioner();
    public static final AbstractType<?> partitionOrdering = new PartitionerDefinedOrder(Murmur3Partitioner.instance);

    @Override
    public DecoratedKey decorateKey(ByteBuffer key)
    {
        long[] hash = getHash(key);
        return new PreHashedDecoratedKey(getToken(key, hash), key, hash[0], hash[1]);
    }

    private LongToken getToken(ByteBuffer key, long[] hash)
    {
        if (key.remaining() == 0)
            return getMinimumToken();

        return new Murmur3Token(normalize(hash[0]));
    }

    private long[] getHash(ByteBuffer key)
    {
        long[] hash = new long[2];
        MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0, hash);
        return hash;
    }

    @Override
    public LongToken getToken(ByteBuffer key)
    {
        return getToken(key, getHash(key));
    }

    @Override
    public Token toToken(long token)
    {
        return new Murmur3Token(token);
    }

    @Override
    public LongToken getMinimumToken()
    {
        return new Murmur3Token(Long.MIN_VALUE);
    }

    @Override
    public LongToken getRandomToken(Random r)
    {
        return new Murmur3Token(normalize(r.nextLong()));
    }

    @Override
    public AbstractType<?> partitionOrdering()
    {
        return Murmur3Partitioner.partitionOrdering;
    }

    public static class Murmur3Token extends LongToken
    {
        private static final int HEAP_SIZE = (int) ObjectSizes.measureDeep(Murmur3Partitioner.instance.getMinimumToken());

        public Murmur3Token(long token)
        {
            super(token);
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return Murmur3Partitioner.instance;
        }

        @Override
        public long getHeapSize()
        {
            return HEAP_SIZE;
        }

        @Override
        public Token increaseSlightly()
        {
            return new Murmur3Token(token + 1);
        }

        /**
         * Reverses murmur3 to find a possible 16 byte key that generates a given token
         */
        @VisibleForTesting
        public static ByteBuffer keyForToken(LongToken token)
        {
            ByteBuffer result = ByteBuffer.allocate(16);
            long[] inv = MurmurHash.inv_hash3_x64_128(new long[] { token.token, 0L});
            result.putLong(inv[0]).putLong(inv[1]).position(0);
            return result;
        }
    }
}
