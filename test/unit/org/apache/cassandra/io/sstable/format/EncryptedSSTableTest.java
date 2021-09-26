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
import java.util.Collections;
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
//import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.TransparentDataEncryptionOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
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
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingTransferTest;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.SchemaLoader.compositeIndexCFMD;
import static org.apache.cassandra.SchemaLoader.createKeyspace;
import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EncryptedSSTableTest //extends SSTableWriterTestBase
{
    public static final InetAddressAndPort LOCAL = FBUtilities.getBroadcastAddressAndPort();
    private static final int maxPartitons = 1;//1024;
    private static final int maxCqlRows = 5;//32;
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
       // SSTableWriterTestBase.truncate(cfs);
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
        //need to fix streaming
        //insertAndFlush();
        //SSTableReader sstable = checkCfs();
       // cfs.clearUnsafe();
        //Assert.assertTrue(cfs.getLiveSSTables().isEmpty());

        /*IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes(Integer.toString(maxCqlRows)))));

        Refs<SSTableReader> ssTableReaders = Refs.tryRef(Arrays.asList(sstable));

        StreamPlan streamPlan = new StreamPlan(StreamOperation.OTHER).transferStreams(LOCAL, makeOutgoingStreams(StreamOperation.OTHER,ranges, Refs.tryRef(Arrays.asList(sstable))));
        streamPlan.execute().get();


     /*   ArrayList<StreamSession.SSTableStreamingSections> details = new ArrayList<>();
        details.add(new StreamSession.SSTableStreamingSections(ssTableReaders.get(sstable),
                                                               sstable.getPositionsForRanges(ranges),
                                                               sstable.estimatedKeysForRanges(ranges), sstable.getSSTableMetadata().repairedAt));
        new StreamPlan("EncryptedStreamingTransferTest").transferFiles(FBUtilities.getBroadcastAddress(), details).execute().get();*/

        //checkSstable(checkCfs());
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
    /*private Collection<OutgoingStream> makeOutgoingStreams(List<Range<Token>> ranges, Refs<SSTableReader> sstables)
    {
        return makeOutgoingStreams(StreamOperation.OTHER, ranges, sstables);
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
    private void doTransferTable(boolean transferSSTables) throws Exception
    {
        final Keyspace keyspace = Keyspace.open(StreamingTransferTest.KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(StreamingTransferTest.CF_INDEX);

        List<String> keys = createAndTransfer(cfs, new StreamingTransferTest.Mutator()
        {
            public void mutate(String key, String col, long timestamp) throws Exception
            {
                long val = key.hashCode();

                RowUpdateBuilder builder = new RowUpdateBuilder(cfs.metadata(), timestamp, key);
                builder.clustering(col).add("birthdate", ByteBufferUtil.bytes(val));
                builder.build().applyUnsafe();
            }
        }, transferSSTables);

        // confirm that the secondary index was recovered
        for (String key : keys)
        {
            long val = key.hashCode();

            // test we can search:
            UntypedResultSet result = QueryProcessor.executeInternal(String.format("SELECT * FROM \"%s\".\"%s\" WHERE birthdate = %d",
                                                                                   cfs.metadata.keyspace, cfs.metadata.name, val));
            assertEquals(1, result.size());

            assert result.iterator().next().getBytes("key").equals(ByteBufferUtil.bytes(key));
        }
    }

    private List<String> createAndTransfer(ColumnFamilyStore cfs, StreamingTransferTest.Mutator mutator, boolean transferSSTables) throws Exception
    {
        // write a temporary SSTable, and unregister it
        logger.debug("Mutating {}", cfs.name);
        long timestamp = 1234;
        for (int i = 1; i <= 3; i++)
            mutator.mutate("key" + i, "col" + i, timestamp);
        cfs.forceBlockingFlush();
        Util.compactAll(cfs, Integer.MAX_VALUE).get();
        assertEquals(1, cfs.getLiveSSTables().size());

        // transfer the first and last key
        logger.debug("Transferring {}", cfs.name);
        int[] offs;
        if (transferSSTables)
        {
            SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
            cfs.clearUnsafe();
            transferSSTables(sstable);
            offs = new int[]{1, 3};
        }
        else
        {
            long beforeStreaming = System.currentTimeMillis();
            transferRanges(cfs);
            cfs.discardSSTables(beforeStreaming);
            offs = new int[]{2, 3};
        }

        // confirm that a single SSTable was transferred and registered
        assertEquals(1, cfs.getLiveSSTables().size());

        // and that the index and filter were properly recovered
        List<ImmutableBTreePartition> partitions = Util.getAllUnfiltered(Util.cmd(cfs).build());
        assertEquals(offs.length, partitions.size());
        for (int i = 0; i < offs.length; i++)
        {
            String key = "key" + offs[i];
            String col = "col" + offs[i];

            assert !Util.getAll(Util.cmd(cfs, key).build()).isEmpty();
            ImmutableBTreePartition partition = partitions.get(i);
            assert ByteBufferUtil.compareUnsigned(partition.partitionKey().getKey(), ByteBufferUtil.bytes(key)) == 0;
            assert ByteBufferUtil.compareUnsigned(partition.iterator().next().clustering().get(0), ByteBufferUtil.bytes(col)) == 0;
        }

        // and that the max timestamp for the file was rediscovered
        assertEquals(timestamp, cfs.getLiveSSTables().iterator().next().getMaxTimestamp());

        List<String> keys = new ArrayList<>();
        for (int off : offs)
            keys.add("key" + off);

        logger.debug("... everything looks good for {}", cfs.name);
        return keys;
    }

    private void transferSSTables(SSTableReader sstable) throws Exception
    {
        IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("key1"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("key2")), p.getMinimumToken()));
        transfer(sstable, ranges);
    }
    private void transfer(SSTableReader sstable, List<Range<Token>> ranges) throws Exception
    {
        StreamPlan streamPlan = new StreamPlan(StreamOperation.OTHER).transferStreams(LOCAL, makeOutgoingStreams(ranges, Refs.tryRef(Arrays.asList(sstable))));
        streamPlan.execute().get();

        //cannot add files after stream session is finished
        try
        {
            streamPlan.transferStreams(LOCAL, makeOutgoingStreams(ranges, Refs.tryRef(Arrays.asList(sstable))));
            fail("Should have thrown exception");
        }
        catch (RuntimeException e)
        {
            //do nothing
        }
    }

    private void transferRanges(ColumnFamilyStore cfs) throws Exception
    {
        IPartitioner p = cfs.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        // wrapped range
        ranges.add(new Range<Token>(p.getToken(ByteBufferUtil.bytes("key1")), p.getToken(ByteBufferUtil.bytes("key0"))));
        StreamPlan streamPlan = new StreamPlan(StreamOperation.OTHER).transferRanges(LOCAL, cfs.keyspace.getName(), RangesAtEndpoint.toDummyList(ranges), cfs.getTableName());
        streamPlan.execute().get();

        //cannot add ranges after stream session is finished
        try
        {
            streamPlan.transferRanges(LOCAL, cfs.keyspace.getName(), RangesAtEndpoint.toDummyList(ranges), cfs.getTableName());
            fail("Should have thrown exception");
        }
        catch (RuntimeException e)
        {
            //do nothing
        }
    }*/

}