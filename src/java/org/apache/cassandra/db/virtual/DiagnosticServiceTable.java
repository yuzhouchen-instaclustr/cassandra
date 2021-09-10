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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.audit.AuditEvent;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.BootstrapEvent;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorEvent;
import org.apache.cassandra.gms.GossiperEvent;
import org.apache.cassandra.hints.HintEvent;
import org.apache.cassandra.hints.HintsServiceEvent;
import org.apache.cassandra.locator.TokenMetadataEvent;
import org.apache.cassandra.schema.SchemaAnnouncementEvent;
import org.apache.cassandra.schema.SchemaEvent;
import org.apache.cassandra.schema.SchemaMigrationEvent;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.PendingRangeCalculatorServiceEvent;

// TODO - this class needs to be mutable - will be done after 16806 is in.
public class DiagnosticServiceTable extends AbstractVirtualTable
{
    private static final String EVENT = "event";
    private static final String ENABLED = "enabled";

    private final Map<String, Boolean> backingMap = new ConcurrentHashMap<>();

    public static final Set<String> eventClasses = ImmutableSet.<String>builder()
                                                               .add(AuditEvent.class.getName(),
                                                                    BootstrapEvent.class.getName(),
                                                                    GossiperEvent.class.getName(),
                                                                    HintEvent.class.getName(),
                                                                    HintsServiceEvent.class.getName(),
                                                                    HintsServiceEvent.class.getName(),
                                                                    PendingRangeCalculatorServiceEvent.class.getName(),
                                                                    SchemaAnnouncementEvent.class.getName(),
                                                                    SchemaEvent.class.getName(),
                                                                    SchemaMigrationEvent.class.getName(),
                                                                    TokenAllocatorEvent.class.getName(),
                                                                    TokenMetadataEvent.class.getName())
                                                               .build();

    DiagnosticServiceTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "diagnostic")
                           .comment("Enabled and disabled diagnostic events")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(EVENT, UTF8Type.instance)
                           .addRegularColumn(ENABLED, BooleanType.instance)
                           .build());

        if (!DatabaseDescriptor.diagnosticEventsVTableEnabled())
            return;

        eventClasses.forEach(event -> backingMap.put(event, false));
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet data = new SimpleDataSet(metadata());
        backingMap.forEach((type, enabled) -> data.row(type).column("enabled", enabled));
        return data;
    }
}
