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

package org.apache.cassandra.db.virtual.diag;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventPersistence;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.schema.TableMetadata.Kind.VIRTUAL;

public abstract class AbstractDiagnosticsVirtualTable extends AbstractVirtualTable
{
    static final String TYPE = "type";
    static final String TIMESTAMP = "ts";
    static final String THREAD = "thread";

    protected TableMetadata.Builder builder;

    AbstractDiagnosticsVirtualTable(String keyspace,
                                    String table,
                                    String comment,
                                    Map<String, AbstractType> columns)
    {
        super(TableMetadata.builder(keyspace, table)
                           .kind(VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(TYPE, UTF8Type.instance)
                           .addClusteringColumn(TIMESTAMP, LongType.instance)
                           .addClusteringColumn(THREAD, UTF8Type.instance)
                           .addRegularColumns(columns)
                           .comment(comment)
                           .build());
    }

    boolean isEnabled(Class<? extends DiagnosticEvent> event)
    {
        return DiagnosticEventService.instance().isEnabled(event);
    }

    Map<Long, Map<String, Serializable>> getEvents(Class<? extends DiagnosticEvent> event)
    {
        if (!isEnabled(event))
        {
            return Collections.emptyMap();
        }

        return DiagnosticEventPersistence.instance().getEvents(event.getName(),
                                                               Long.MAX_VALUE,
                                                               Integer.MAX_VALUE,
                                                               true);
    }

    SimpleDataSet addRow(SimpleDataSet dataSet, Map<String, Serializable> entry)
    {
        return dataSet.row(entry.get(TYPE),
                           entry.get(TIMESTAMP),
                           entry.get(THREAD));
    }
}
