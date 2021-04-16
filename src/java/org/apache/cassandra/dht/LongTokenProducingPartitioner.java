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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import com.google.common.primitives.Longs;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class LongTokenProducingPartitioner implements IPartitioner
{
    public static final long MAXIMUM = Long.MAX_VALUE;
    protected static final int MAXIMUM_TOKEN_SIZE = TypeSizes.sizeof(MAXIMUM);

    protected final Splitter splitter = new Splitter(this)
    {
        public Token tokenForValue(BigInteger value)
        {
            return toToken(value.longValue());
        }

        public BigInteger valueForToken(Token token)
        {
            return BigInteger.valueOf(((LongToken) token).token);
        }
    };

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory()
    {
        public ByteBuffer toByteArray(Token token)
        {
            LongToken longToken = (LongToken) token;
            return ByteBufferUtil.bytes(longToken.token);
        }

        @Override
        public void serialize(Token token, DataOutputPlus out) throws IOException
        {
            out.writeLong(((LongToken) token).token);
        }

        @Override
        public void serialize(Token token, ByteBuffer out)
        {
            out.putLong(((LongToken) token).token);
        }

        @Override
        public int byteSize(Token token)
        {
            return 8;
        }

        public Token fromByteArray(ByteBuffer bytes)
        {
            return toToken(ByteBufferUtil.toLong(bytes));
        }

        @Override
        public Token fromByteBuffer(ByteBuffer bytes, int position, int length)
        {
            return toToken(bytes.getLong(position));
        }

        public String toString(Token token)
        {
            return token.toString();
        }

        public void validate(String token) throws ConfigurationException
        {
            try
            {
                fromString(token);
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(e.getMessage());
            }
        }

        public Token fromString(String string)
        {
            try
            {
                return toToken(Long.parseLong(string));
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException(String.format("Invalid token. Got %s but expected a long value (unsigned 8 bytes integer).", string));
            }
        }
    };

    public boolean preservesOrder()
    {
        return false;
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        Map<Token, Float> ownerships = new HashMap<Token, Float>();
        Iterator<Token> i = sortedTokens.iterator();

        // 0-case
        if (!i.hasNext())
            throw new RuntimeException("No nodes present in the cluster. Has this node finished starting up?");
        // 1-case
        if (sortedTokens.size() == 1)
            ownerships.put(i.next(), new Float(1.0));
        // n-case
        else
        {
            final BigInteger ri = BigInteger.valueOf(LongTokenProducingPartitioner.MAXIMUM).subtract(BigInteger.valueOf(getMinimumToken().token + 1));  //  (used for addition later)
            final BigDecimal r  = new BigDecimal(ri);
            Token start = i.next();BigInteger ti = BigInteger.valueOf(((LongToken)start).token);  // The first token and its value
            Token t; BigInteger tim1 = ti;                                                                // The last token and its value (after loop)

            while (i.hasNext())
            {
                t = i.next(); ti = BigInteger.valueOf(((LongToken) t).token); // The next token and its value
                float age = new BigDecimal(ti.subtract(tim1).add(ri).mod(ri)).divide(r, 6, BigDecimal.ROUND_HALF_EVEN).floatValue(); // %age = ((T(i) - T(i-1) + R) % R) / R
                ownerships.put(t, age);                           // save (T(i) -> %age)
                tim1 = ti;                                        // -> advance loop
            }

            // The start token's range extends backward to the last token, which is why both were saved above.
            float x = new BigDecimal(BigInteger.valueOf(((LongToken)start).token).subtract(ti).add(ri).mod(ri)).divide(r, 6, BigDecimal.ROUND_HALF_EVEN).floatValue();
            ownerships.put(start, x);
        }

        return ownerships;
    }

    public Token.TokenFactory getTokenFactory()
    {
        return tokenFactory;
    }

    public AbstractType<?> getTokenValidator()
    {
        return LongType.instance;
    }

    public Token getMaximumToken()
    {
        return toToken(Long.MAX_VALUE);
    }

    public Optional<Splitter> splitter()
    {
        return Optional.of(splitter);
    }

    public Token midpoint(Token lToken, Token rToken)
    {
        // using BigInteger to avoid long overflow in intermediate operations
        BigInteger l = BigInteger.valueOf(((LongToken) lToken).token),
                   r = BigInteger.valueOf(((LongToken) rToken).token),
                   midpoint;

        if (l.compareTo(r) < 0)
        {
            BigInteger sum = l.add(r);
            midpoint = sum.shiftRight(1);
        }
        else // wrapping case
        {
            BigInteger max = BigInteger.valueOf(MAXIMUM);
            BigInteger min = BigInteger.valueOf(getMinimumToken().token);
            // length of range we're bisecting is (R - min) + (max - L)
            // so we add that to L giving
            // L + ((R - min) + (max - L) / 2) = (L + R + max - min) / 2
            midpoint = (max.subtract(min).add(l).add(r)).shiftRight(1);
            if (midpoint.compareTo(max) > 0)
                midpoint = min.add(midpoint.subtract(max));
        }

        return toToken(midpoint.longValue());
    }

    public Token split(Token lToken, Token rToken, double ratioToLeft)
    {
        BigDecimal l = BigDecimal.valueOf(((LongToken) lToken).token),
                   r = BigDecimal.valueOf(((LongToken) rToken).token),
                   ratio = BigDecimal.valueOf(ratioToLeft);
        long newToken;

        if (l.compareTo(r) < 0)
        {
            newToken = r.subtract(l).multiply(ratio).add(l).toBigInteger().longValue();
        }
        else
        {
            // wrapping case
            // L + ((R - min) + (max - L)) * pct
            BigDecimal max = BigDecimal.valueOf(MAXIMUM);
            BigDecimal min = BigDecimal.valueOf(getMinimumToken().token);

            BigInteger token = max.subtract(min).add(r).subtract(l).multiply(ratio).add(l).toBigInteger();

            BigInteger maxToken = BigInteger.valueOf(MAXIMUM);

            if (token.compareTo(maxToken) <= 0)
            {
                newToken = token.longValue();
            }
            else
            {
                // if the value is above maximum
                BigInteger minToken = BigInteger.valueOf(getMinimumToken().token);
                newToken = minToken.add(token.subtract(maxToken)).longValue();
            }
        }
        return toToken(newToken);
    }

    public abstract Token toToken(long token);

    public abstract LongToken getMinimumToken();

    public int getMaxTokenSize()
    {
        return MAXIMUM_TOKEN_SIZE;
    }

    protected long normalize(long v)
    {
        // We exclude the MINIMUM value; see getToken()
        return v == Long.MIN_VALUE ? Long.MAX_VALUE : v;
    }


    @Override
    public Token getRandomToken()
    {
        return getRandomToken(new Random());
    }

    public static class LongToken extends Token
    {
        static final long serialVersionUID = -5833580143318243006L;

        final long token;

        public LongToken(long token)
        {
            this.token = token;
        }

        public String toString()
        {
            return Long.toString(token);
        }

        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null || this.getClass() != obj.getClass())
                return false;

            return token == (((LongToken)obj).token);
        }

        public int hashCode()
        {
            return Longs.hashCode(token);
        }

        public int compareTo(Token o)
        {
            return Long.compare(token, ((LongToken) o).token);
        }

        @Override
        public Object getTokenValue()
        {
            return token;
        }

        @Override
        public double size(Token next)
        {
            LongToken n = (LongToken) next;
            long v = n.token - token;  // Overflow acceptable and desired.
            double d = Math.scalb((double) v, -Long.SIZE); // Scale so that the full range is 1.
            return d > 0.0 ? d : (d + 1.0); // Adjust for signed long, also making sure t.size(t) == 1.
        }

        @Override
        public IPartitioner getPartitioner()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getHeapSize()
        {
            throw new UnsupportedOperationException();
        }


        @Override
        public Token increaseSlightly()
        {
            throw new UnsupportedOperationException();
        }
    }
}
