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

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.diag.DiagnosticEventPersistence;
import org.apache.cassandra.schema.TableMetadata;
import org.json.simple.JSONObject;

final class DiagnosticEventsTable extends AbstractVirtualTable
{
    private static final String EVENT = "event";
    private static final String TYPE = "type";
    private static final String TIMESTAMP = "ts";
    private static final String THREAD_NAME = "thread_name";
    private static final String DATA = "data";

    DiagnosticEventsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "diagnostic_events")
                           .comment("Diagnostic Events, when enabled")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(EVENT, UTF8Type.instance)
                           .addClusteringColumn(TIMESTAMP, ReversedType.getInstance(TimestampType.instance))
                           .addClusteringColumn(TYPE, UTF8Type.instance)
                           .addRegularColumn(THREAD_NAME, UTF8Type.instance)
                           .addRegularColumn(DATA, MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false))
                           .build());
    }

    public DataSet data()
    {
        final SimpleDataSet result = new SimpleDataSet(metadata());

        DiagnosticEventPersistence.instance()
                                  .getEvents(0L, Integer.MAX_VALUE, true, true)
                                  .values()
                                  .forEach(m -> addRow(result, m));

        return result;
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        String name = UTF8Type.instance.compose(partitionKey.getKey());

        DiagnosticEventPersistence.instance()
                                  .getEvents(name, 0L, Integer.MAX_VALUE, true, true)
                                  .values()
                                  .forEach(m -> addRow(result, m));

        return result;
    }

    private static void addRow(SimpleDataSet result, Map<String, Serializable> event)
    {
        SimpleDataSet row = result.row(event.remove("class"),
                                       new Date((Long) event.remove(TIMESTAMP)),
                                       event.remove(TYPE))
                                  .column(THREAD_NAME, event.remove("thread"));


        JSONObject json = new JSONObject();
        event.forEach((k, v) -> {
            if (v instanceof Map)
            {
                json.putAll((Map) v);
                event.put(k, json.toString());
            }
            else if (!(v instanceof String))
            {
                event.put(k, v.toString());
            }
        });

        row.column(DATA, event);
    }
}
