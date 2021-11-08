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

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableWriterTestBase;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingTransferTest;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

public class EncryptedSSTableTest
{
    public static final InetAddressAndPort LOCAL = FBUtilities.getBroadcastAddressAndPort();
    private static final int maxPartitons = 1024;
    private static final int maxCqlRows = 32;
    private static final String colName = "val";
    protected static final String KEYSPACE = "SSTableRewriterTest";
    protected static final String CF = "Standard1";
    protected static final String CF_SMALL_MAX_VALUE = "Standard_SmallMaxValue";
    private static boolean hasSetEncryption;
    private static TableMetadata metadata;
    private static Config.DiskAccessMode standardMode;
    private static Config.DiskAccessMode indexMode;

    private static int maxValueSize;

    private ColumnFamilyStore cfs;
    private static final Logger logger = LoggerFactory.getLogger(StreamingTransferTest.class);
    @Before
    public void setUp()
    {
        DatabaseDescriptor.daemonInitialization();

        standardMode = DatabaseDescriptor.getDiskAccessMode();
        indexMode = DatabaseDescriptor.getIndexAccessMode();
        DatabaseDescriptor.setDiskAccessMode(Config.DiskAccessMode.standard);
        DatabaseDescriptor.setIndexAccessMode(Config.DiskAccessMode.standard);
        SchemaLoader.prepareServer();

        if (!hasSetEncryption)
        {
            hasSetEncryption = true;
            DatabaseDescriptor.setEncryptionContext(EncryptionContextGenerator.createContext());
            TransparentDataEncryptionOptions tdeOptions = EncryptionContextGenerator.createEncryptionOptions();
            Map<String, String> map = new HashMap<>();
            map.put(EncryptionContext.ENCRYPTION_KEY_ALIAS, tdeOptions.key_alias);
            map.put(EncryptionContext.ENCRYPTION_CIPHER, tdeOptions.cipher);
            CompressionParams params = (CompressionParams.encryptingCompressor(map));
            params.setEncrypted(true);
            metadata = SchemaLoader.standardCFMD(KEYSPACE, CF)
                                   .compression(params).build();
            SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                        metadata);
        }
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        cfs = keyspace.getColumnFamilyStore(CF);
        cfs.setEncryption(true);

        SSTableWriterTestBase.truncate(cfs);
    }


    @AfterClass
    public static void revertConfiguration()
    {
        DatabaseDescriptor.setMaxValueSize(maxValueSize);
        DatabaseDescriptor.setDiskAccessMode(standardMode);
        DatabaseDescriptor.setIndexAccessMode(indexMode);
    }
    @Test
    public void insertFlushAndRead() throws CharacterCodingException
    {
        insertAndFlush();
        checkSstable(checkCfs());
    }

    private void insertAndFlush()
    {
        for (int i = 0; i < maxPartitons; i++)
        {
            UpdateBuilder builder = UpdateBuilder.create(metadata, ByteBufferUtil.bytes(i));
            for (int j = 0; j < maxCqlRows; j++)
                builder.newRow(ByteBufferUtil.bytes(Integer.toString(j))).add(colName, ByteBufferUtil.bytes(j));
            builder.apply();
        }

        cfs.forceBlockingFlush();
    }

    private SSTableReader checkCfs()
    {
        Assert.assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        Assert.assertNotNull(sstable);
        Assert.assertTrue(sstable.compression);
        Assert.assertTrue(sstable.indexCompression);
        return sstable;
    }

    private void checkSstable(SSTableReader sstable)
    {
        for (int i = 0; i < maxPartitons; i++)
        {
            DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(i));
            int cqlRowCount;
            try (UnfilteredRowIterator iterator = sstable.iterator(key, Slices.ALL, ColumnFilter.all(cfs.metadata()), false, SSTableReadsListener.NOOP_LISTENER))
            {
                Assert.assertEquals(key, iterator.partitionKey());

                cqlRowCount = 0;
                while (iterator.hasNext())
                {
                    Unfiltered unfiltered = iterator.next();
                    Assert.assertTrue(unfiltered.isRow());
                    Row row = (Row) unfiltered;

                    Collection<ColumnMetadata> columns = row.columns();
                    Assert.assertEquals(1, columns.size());
                    ColumnMetadata def = columns.iterator().next();
                    Assert.assertEquals(colName, def.name.toCQLString());


                    Cell cell = row.getCell(def);
                    ByteBufferUtil.toInt(cell.buffer());

                    cqlRowCount++;
                }
            }
            Assert.assertEquals(maxCqlRows, cqlRowCount);
        }
    }

    // basic structure of this test is borrowed from StreamingTransferTest
    @Test
    public void stream() throws ExecutionException, InterruptedException
    {
        //TODO: need to fix streaming test
        /*insertAndFlush();
        SSTableReader sstable = checkCfs();
        cfs.clearUnsafe();
        Assert.assertTrue(cfs.getLiveSSTables().isEmpty());

        IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes(Integer.toString(maxCqlRows)))));

        Refs<SSTableReader> ssTableReaders = Refs.tryRef(Arrays.asList(sstable));
        StreamPlan streamPlan = new StreamPlan(StreamOperation.OTHER).transferStreams(LOCAL, makeOutgoingStreams(StreamOperation.OTHER, ranges, ssTableReaders));
        streamPlan.execute().get();
        checkSstable(checkCfs());*/
    }
    private Collection<OutgoingStream> makeOutgoingStreams(StreamOperation operation, List<Range<Token>> ranges, Refs<SSTableReader> sstables)
    {
        ArrayList<OutgoingStream> streams = new ArrayList<>();
        for (SSTableReader sstable : sstables)
        {
            streams.add(new CassandraOutgoingFile(operation,
                                                  sstables.get(sstable),
                                                  sstable.getPositionsForRanges(ranges),
                                                  ranges,
                                                  sstable.estimatedKeysForRanges(ranges)));
        }
        return streams;
    }


}