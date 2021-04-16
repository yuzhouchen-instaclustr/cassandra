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

package org.apache.cassandra.dht;

import java.nio.ByteBuffer;
import java.util.Random;

import net.openhft.hashing.LongHashFunction;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.utils.ObjectSizes;

public class XXHashPartitioner extends LongTokenProducingPartitioner
{
    public static final XXHashPartitioner instance = new XXHashPartitioner();
    public static final AbstractType<?> partitionOrdering = new PartitionerDefinedOrder(XXHashPartitioner.instance);

    private static final LongHashFunction hashFunction = LongHashFunction.xx(0);

    @Override
    public Token toToken(long token)
    {
        return new XXHashPartitioner.XXHashToken(token);
    }

    @Override
    public DecoratedKey decorateKey(ByteBuffer key)
    {
        long hash = getHash(key);
        return new BufferDecoratedKey(getToken(key, hash), key);
    }

    @Override
    public LongToken getMinimumToken()
    {
        return new XXHashToken(Long.MIN_VALUE);
    }

    @Override
    public LongToken getToken(ByteBuffer key)
    {
        return getToken(key, getHash(key));
    }

    @Override
    public Token getRandomToken(Random random)
    {
        return new XXHashPartitioner.XXHashToken(normalize(random.nextLong()));
    }

    @Override
    public AbstractType<?> partitionOrdering()
    {
        return XXHashPartitioner.partitionOrdering;
    }

    private long getHash(ByteBuffer key)
    {
        return hashFunction.hashBytes(key, key.position(), key.remaining());
    }

    private LongToken getToken(ByteBuffer key, long hash)
    {
        if (key.remaining() == 0)
            return getMinimumToken();

        return new XXHashToken(normalize(hash));
    }

    public static class XXHashToken extends LongToken
    {
        private static final int HEAP_SIZE = (int) ObjectSizes.measureDeep(XXHashPartitioner.instance.getMinimumToken());

        public XXHashToken(long token)
        {
            super(token);
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return XXHashPartitioner.instance;
        }

        @Override
        public long getHeapSize()
        {
            return HEAP_SIZE;
        }

        @Override
        public Token increaseSlightly()
        {
            return new XXHashToken(token + 1);
        }
    }
}
