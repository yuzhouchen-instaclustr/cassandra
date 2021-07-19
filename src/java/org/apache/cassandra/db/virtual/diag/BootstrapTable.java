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
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.BootstrapEvent;

public class BootstrapTable extends AbstractDiagnosticsVirtualTable
{
    BootstrapTable(String keyspace)
    {
        super(keyspace,
              "bootstrap",
              "system bootstrapping",
              new HashMap<String, AbstractType>()
              {{
                  put("token_metadata", UTF8Type.instance);
                  put("allocation_keyspace", UTF8Type.instance);
                  put("rf", Int32Type.instance);
                  put("num_tokens", Int32Type.instance);
                  put("tokens", UTF8Type.instance);
              }});
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataSet = new SimpleDataSet(metadata);

        if (!isEnabled(BootstrapEvent.class))
        {
            return dataSet;
        }

        for (Map.Entry<Long, Map<String, Serializable>> event : getEvents(BootstrapEvent.class).entrySet())
        {
            Map<String, Serializable> value = event.getValue();

            addRow(dataSet, event.getValue())
            .column("token_metadata", value.get("tokenMetadata"))
            .column("allocation_keyspace", value.get("allocationKeyspace"))
            .column("rf", value.get("rf"))
            .column("num_tokens", value.get("numTokens"))
            .column("tokens", value.get("tokens"));
        }

        return dataSet;
    }
}
