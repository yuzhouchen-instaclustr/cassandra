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
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.gms.GossiperEvent;

public class GossiperTable extends AbstractDiagnosticsVirtualTable
{
    GossiperTable(String keyspace)
    {
        super(keyspace,
              "gossiper",
              "system gossiper",
              new HashMap<String, AbstractType>()
              {{
                  put("endpoint", UTF8Type.instance);
                  put("quarantine_expiration", LongType.instance);
                  put("local_state", UTF8Type.instance);
                  put("endpoint_state_map", UTF8Type.instance);
                  put("in_shadow_round", BooleanType.instance);
                  put("just_removed_endpoints", UTF8Type.instance);
                  put("last_processed_message_at", LongType.instance);
                  put("live_endpoints", UTF8Type.instance);
                  put("seeds", UTF8Type.instance);
                  put("seeds_in_shadow_round", UTF8Type.instance);
                  put("unreachable_endpoints", UTF8Type.instance);
              }});
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataSet = new SimpleDataSet(metadata);

        for (Map.Entry<Long, Map<String, Serializable>> event : getEvents(GossiperEvent.class).entrySet())
        {
            Map<String, Serializable> value = event.getValue();

            addRow(dataSet, event.getValue())
            .column("endpoint", value.get("endpoint"))
            .column("quarantine_expiration", value.get("quarantineExpiration"))
            .column("local_state", value.get("localState"))
            .column("endpoint_state_map", value.get("endpointStateMap"))
            .column("in_shadow_round", value.get("inShadowRound"))
            .column("just_removed_endpoints", value.get("justRemovedEndpoints"))
            .column("last_processed_message_at", value.get("lastProcessedMessageAt"))
            .column("live_endpoints", value.get("liveEndpoints"))
            .column("seeds", value.get("seeds"))
            .column("seeds_in_shadow_round", value.get("seedsInShadowRound"))
            .column("unreachable_endpoints", value.get("unreachableEndpoints"));
        }

        return dataSet;
    }
}