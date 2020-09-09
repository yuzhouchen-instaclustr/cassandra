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

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.MigrationTask;

import static java.lang.String.format;
import static net.bytebuddy.matcher.ElementMatchers.named;

public class BootstrappingSchemaAgreementTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(BootstrappingSchemaAgreementTest.class);

    @Test
    public void testSchemaAgreementIsReached() throws Exception
    {
        // this test is considered successful when all nodes have same schema version

        final Cluster.Builder builder = builder().withNodes(1)
                                                 .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                                 .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                                 .withInstanceInitializer(BBSchemaAgreementHelper::install)
                                                 .withConfig(config -> config.with(Feature.NETWORK,
                                                                                   Feature.GOSSIP,
                                                                                   Feature.NATIVE_PROTOCOL));

        try (final Cluster cluster = builder.withNodes(2).start())
        {
            for (int i = 0; i < 100; i++)
            {
                if (i % 10 == 0)
                {
                    logger.info("Number of keyspaces created: {}", i);
                }

                cluster.schemaChange(format("CREATE KEYSPACE IF NOT EXISTS keyspace_%s WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 2};", i));
                Thread.sleep(1000);

                StringBuilder sb = new StringBuilder();

                // create a table with 100 text columns to have big schema
                for (int j = 0; j < 1000; j++)
                {
                    sb.append(", col_" + j + " text");
                }

                cluster.schemaChange(format("CREATE TABLE IF NOT EXISTS keyspace_%s.mytable (id uuid primary key %s)", i, sb.toString()));
            }

            // we drop all migration requests from node which we want to bootstrap to already started nodes,
            // by doing so, we simulate some network error so logic in StorageService#waitForSchema needs
            // to deal with it and iterate over nodes until some schema is eventually merged.
            cluster.verbs(MessagingService.Verb.MIGRATION_REQUEST).from(3).to(1, 2).messagesMatching(new IMessageFilters.Matcher()
            {

                // we keep track of what migration requests we have already sent
                // in this particular test, we force that a migration request will be dropped for each already
                // fully bootstrapped node at least once
                final Set<Integer> migrationRequests = new HashSet<>();

                public boolean matches(int from, int to, IMessage message)
                {
                    boolean newlyAdded = migrationRequests.add(to);

                    // drop it if it was added from the first time
                    // we drop a mutation message at least once for each already existing node
                    // and if we have not dropped for each node yet
                    logger.info(format("Dropping MUTATION message from %s to %s", from, to));

                    if (newlyAdded || migrationRequests.size() < 2)
                    {
                        final InetAddress receiverAddress = cluster.get(to).broadcastAddress().getAddress();

                        // act as if that request has failed
                        // if we dropped that message from sending, it would never be removed from MigrationTask
                        cluster.get(2).runOnInstance(() -> {
                            // sleep here for one more second than configured timout threshold to mimic that
                            // a message is sent but it took some time to process it and the other side was slow to respond
                            // in the meanwhile, we will be issuing other requests to other nodes.
                            Uninterruptibles.sleepUninterruptibly(2000, TimeUnit.MILLISECONDS);

                            new MigrationTask.MigrationTaskCallback().onFailure(receiverAddress, RequestFailureReason.UNKNOWN);
                        });

                        return true;
                    }

                    return false;
                }
            }).drop();

            final IInstanceConfig secondNodeConfig = cluster.newInstanceConfig();
            secondNodeConfig.set("auto_bootstrap", true);

            final IInvokableInstance secondNode = cluster.bootstrap(secondNodeConfig);

            try
            {
                secondNode.startup();
            }
            finally
            {
                secondNode.shutdown();
            }

            System.out.println("here");
        }
    }

    public static class BBSchemaAgreementHelper
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            // doing nothing on the third node only to enforce the schema agreement logic in StorageService#waitForSchema
            // schema pulling is done on "onJoin", "onAlive" and on respective "onChange" call in StorageService as well.
            // normally, this is how a node react when a node has joined a cluster but it also happens while a node is bootstrapping
            // and it may happen that schema will be agreed on sooner than we had a chance to trigger StorageService#waitForSchema.
            if (nodeNumber == 3)
            {
                new ByteBuddy().redefine(MigrationManager.class)
                               .method(named("scheduleSchemaPull"))
                               .intercept(MethodDelegation.to(BootstrappingSchemaAgreementTest.BBSchemaAgreementHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static void scheduleSchemaPull(InetAddress endpoint, EndpointState state)
        {
            // we intercept this method, doing nothing on purpose
        }
    }
}
