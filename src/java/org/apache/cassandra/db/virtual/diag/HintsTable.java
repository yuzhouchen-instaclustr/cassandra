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
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.hints.HintsServiceEvent;

import static org.apache.cassandra.hints.HintsServiceEvent.DISPATCH_EXECUTOR_HAS_SCHEDULED_DISPATCHES_FIELD;
import static org.apache.cassandra.hints.HintsServiceEvent.DISPATCH_EXECUTOR_IS_PAUSED_FIELD;
import static org.apache.cassandra.hints.HintsServiceEvent.IS_DISPATCH_PAUSED_FIELD;
import static org.apache.cassandra.hints.HintsServiceEvent.IS_SHUTDOWN_FIELD;

public class HintsTable extends AbstractDiagnosticsVirtualTable
{
    private static final String IS_DISPATCH_PAUSED_COLUMN = "is_dispatch_paused";
    private static final String IS_SHUTDOWN_COLUMN = "is_shutdown";
    private static final String DISPATCH_EXECUTOR_IS_PAUSED_COLUMN = "dispatch_executor_is_paused";
    private static final String DISPATCH_EXECUTOR_HAS_SCHEDULED_DISPATCHES_COLUMN = "dispatch_executor_has_scheduled_dispatches";

    HintsTable(String keyspace)
    {
        super(keyspace,
              "hints",
              "system hints",
              new HashMap<String, AbstractType>()
              {{
                  put(IS_DISPATCH_PAUSED_COLUMN, BooleanType.instance);
                  put(IS_SHUTDOWN_COLUMN, BooleanType.instance);
                  put(DISPATCH_EXECUTOR_IS_PAUSED_COLUMN, BooleanType.instance);
                  put(DISPATCH_EXECUTOR_HAS_SCHEDULED_DISPATCHES_COLUMN, BooleanType.instance);
              }});
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataSet = new SimpleDataSet(metadata);

        for (Map.Entry<Long, Map<String, Serializable>> event : getEvents(HintsServiceEvent.class).entrySet())
        {
            Map<String, Serializable> value = event.getValue();

            addRow(dataSet, event.getValue())
            .column(IS_DISPATCH_PAUSED_COLUMN, value.get(IS_DISPATCH_PAUSED_FIELD))
            .column(IS_SHUTDOWN_COLUMN, value.get(IS_SHUTDOWN_FIELD))
            .column(DISPATCH_EXECUTOR_IS_PAUSED_COLUMN, value.get(DISPATCH_EXECUTOR_IS_PAUSED_FIELD))
            .column(DISPATCH_EXECUTOR_HAS_SCHEDULED_DISPATCHES_COLUMN, value.get(DISPATCH_EXECUTOR_HAS_SCHEDULED_DISPATCHES_FIELD));
        }

        return dataSet;
    }
}
