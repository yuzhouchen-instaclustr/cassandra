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

import org.junit.Test;

import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.gms.GossiperEvent;

public class DiagnosticEventsTest extends TestBaseImpl
{

    @Test
    public void testDiagnosticEventVTableEnabled() throws Throwable
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(config -> config.with(Feature.NETWORK)
                                                                  .with(Feature.GOSSIP)
                                                                  .set("diagnostic_events_enabled", "true")
                                                                  .set("diagnostic_events_vtable_enabled", "true"))
                                      .start())
        {
            IInvokableInstance node = cluster.get(1);

            // assert that enabling persistence will enable it in vtable
            node.runOnInstance(() -> DiagnosticEventService.instance().enableEventPersistence(GossiperEvent.class.getName()));
            assertState(readDiagnosticsTable(cluster), GossiperEvent.class.getName(), true);

            // assert that disabling in persistence will disable it in vtable
            node.runOnInstance(() -> DiagnosticEventService.instance().disableEventPersistence(GossiperEvent.class.getName()));
            assertState(readDiagnosticsTable(cluster), GossiperEvent.class.getName(), false);
        }
    }

    private void assertState(Object[][] result, String event, boolean expectingEnabled)
    {
        // TODO
    }

    private Object[][] readDiagnosticsTable(final Cluster cluster)
    {
        return cluster.coordinator(1).execute("SELECT * FROM system_views.diagnostic", ConsistencyLevel.ONE);
    }
}
