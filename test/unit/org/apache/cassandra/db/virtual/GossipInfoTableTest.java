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

package org.apache.cassandra.db.virtual;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.assertj.core.api.Assertions.assertThat;

public class GossipInfoTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @SuppressWarnings("FieldCanBeLocal")
    private GossipInfoTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Before
    public void config()
    {
        table = new GossipInfoTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Test
    public void testSelectAllWhenGossipInfoIsEmpty() throws Throwable
    {
        assertEmpty(execute("SELECT * FROM vts.gossip_info"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSelectAllWithStateTransitions() throws Throwable
    {
        try
        {
            requireNetwork(); // triggers gossiper

            UntypedResultSet resultSet = execute("SELECT * FROM vts.gossip_info");

            assertThat(resultSet.size()).isEqualTo(1);
            assertThat(Gossiper.instance.endpointStateMap.size()).isEqualTo(1);

            Optional<Entry<InetAddressAndPort, EndpointState>> entry = Gossiper.instance.endpointStateMap.entrySet()
                                                                                                         .stream()
                                                                                                         .findFirst();
            assertThat(entry).isNotEmpty();

            UntypedResultSet.Row row = resultSet.one();
            assertThat(row.getColumns().size()).isEqualTo(40);

            InetAddressAndPort endpoint = entry.get().getKey();
            EndpointState localState = entry.get().getValue();

            assertThat(endpoint).isNotNull();
            assertThat(localState).isNotNull();
            assertThat(row.getInetAddress("address")).isEqualTo(endpoint.getAddress());
            assertThat(row.getInt("port")).isEqualTo(endpoint.getPort());
            assertThat(row.getString("hostname")).isEqualTo(endpoint.getHostName());
            assertThat(row.getInt("generation")).isEqualTo(localState.getHeartBeatState().getGeneration());

            Arrays.stream(ApplicationState.values())
                  .filter(GossipInfoTable.eligibleStatesForValues)
                  .forEach(s -> assertValue(row, s.name().toLowerCase(), localState, s));

            Arrays.stream(ApplicationState.values())
                  .filter(GossipInfoTable.eligibleStatesForVersions)
                  .forEach(s -> assertVersion(row, s.name().toLowerCase() + "_version", localState, s));
        }
        finally
        {
            // clean up the gossip states
            Gossiper.instance.clearUnsafe();
        }
    }

    private void assertValue(UntypedResultSet.Row row, String column, EndpointState localState, ApplicationState key)
    {
        if (row.has(column))
        {
            assertThat(localState.getApplicationState(key)).as("'%s' is expected to be not-null", key)
                                                           .isNotNull();
            assertThat(row.getString(column)).as("'%s' is expected to match column '%s'", key, column)
                                             .isEqualTo(localState.getApplicationState(key).value);
        }
        else
        {
            assertThat(localState.getApplicationState(key)).as("'%s' is expected to be null", key)
                                                           .isNull();
        }
    }

    private void assertVersion(UntypedResultSet.Row row, String column, EndpointState localState, ApplicationState key)
    {
        if (row.has(column))
        {
            assertThat(localState.getApplicationState(key)).as("'%s' is expected to be not-null", key)
                                                           .isNotNull();
            assertThat(row.getInt(column)).as("'%s' is expected to match column '%s'", key, column)
                                          .isEqualTo(localState.getApplicationState(key).version);
        }
        else
        {
            assertThat(localState.getApplicationState(key)).as("'%s' is expected to be null", key)
                                                           .isNull();
        }
    }
}
