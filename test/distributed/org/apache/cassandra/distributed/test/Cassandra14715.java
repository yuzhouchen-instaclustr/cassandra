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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import com.google.common.util.concurrent.Futures;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.ShutdownException;
import org.apache.cassandra.exceptions.ReadTimeoutException;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.net.MessagingService.Verb.READ_REPAIR;
import static org.apache.commons.io.FileUtils.cleanDirectory;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class Cassandra14715 extends TestBaseImpl
{
    @Test
    public void test() throws Exception
    {
        RuntimeException expectedException = null;

        try (Cluster c = builder().withNodes(6)
                                  .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                  .withDC("dc1", 3)
                                  .withDC("dc2", 3)
                                  .start())
        {
            c.schemaChange("CREATE KEYSPACE IF NOT EXISTS weather WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3'} AND durable_writes = true;");
            c.schemaChange("CREATE TABLE IF NOT EXISTS weather.city (\n" +
                           "cityid int PRIMARY KEY,\n" +
                           "name text\n" +
                           ") WITH bloom_filter_fp_chance = 0.01\n" +
                           "AND dclocal_read_repair_chance = 1.0\n" +
                           "AND read_repair_chance = 1.0\n" +
                           "AND speculative_retry = 'NONE';");

            c.coordinator(1).execute("INSERT INTO weather.city(cityid, name) VALUES (1, 'Canberra');", ALL);

            IInvokableInstance faultyNode = c.get(2);

            c.forEach(i -> {
                i.flush("system");
                i.flush("weather");
            });

//            c.get(2).executeInternal("UPDATE weather.city SET name = 'Sydney' WHERE cityid = 1");

            Futures.getUnchecked(faultyNode.shutdown());
            String cityId = c.get(1).callOnInstance((SerializableCallable<String>) () -> {
                return Keyspace.open("weather").getMetadata().tables.get("city").get().cfId.toString().replaceAll("-", "");
            });

            removeData((String[]) faultyNode.config().get("data_file_directories"), cityId);
            faultyNode.startup();

            IMessageFilters.Filter drop = c.filters().verbs(READ_REPAIR.ordinal()).from(1).to(2).drop();

            c.coordinator(1).executeWithResult("SELECT * FROM weather.city WHERE cityid = 1;", LOCAL_QUORUM);

        }
//        catch (RuntimeException ex)
//        {
//            expectedException = ex;
//            if (ex instanceof ShutdownException)
//            {
//                Throwable[] suppressed = ex.getSuppressed();
//                Assert.assertEquals(1, suppressed.length);
//                assertTrue(suppressed[0] instanceof ReadTimeoutException);
//            }
//            else
//            {
//                assertTrue(ex.getCause() instanceof ReadTimeoutException);
//            }
//        }
//
//        assertNotNull(expectedException);
    }

    private void removeData(String[] dataDirs, String tableId) throws IOException
    {
        for (String dataDir : dataDirs)
        {
            cleanDirectory(Paths.get(dataDir).resolve("weather").resolve("city-" + tableId).toFile());
        }

        cleanDirectory(Paths.get(dataDirs[0]).getParent().resolve("commitlog").toFile());
    }
}
