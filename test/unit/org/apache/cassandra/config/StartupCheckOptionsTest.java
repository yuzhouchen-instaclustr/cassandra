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

package org.apache.cassandra.config;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.service.StartupChecks;
import org.apache.cassandra.service.StartupChecks.GcGraceSecondsOnStartupCheck;
import org.apache.cassandra.service.StartupChecks.StartupCheckType;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.config.StartupChecksOptions.ENABLED_PROPERTY;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.FILESYSTEM_OWNERSHIP_CHECK;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.NON_CONFIGURABLE_CHECK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StartupCheckOptionsTest
{
    @Test
    public void testStartupOptionsConfigApplication()
    {
        Map<StartupCheckType, Map<String, Object>> config = new EnumMap<StartupCheckType, Map<String, Object>>(StartupCheckType.class) {{
            put(FILESYSTEM_OWNERSHIP_CHECK, new HashMap<String, Object>() {{
                put(ENABLED_PROPERTY, true);
                put("key", "value");
            }});
        }};

        StartupChecksOptions options = new StartupChecksOptions(config);

        assertTrue(Boolean.parseBoolean(options.getConfig(FILESYSTEM_OWNERSHIP_CHECK)
                                               .get(ENABLED_PROPERTY)
                                               .toString()));

        assertEquals("value", options.getConfig(FILESYSTEM_OWNERSHIP_CHECK).get("key"));
        options.set(FILESYSTEM_OWNERSHIP_CHECK, "key", "value2");
        assertEquals("value2", options.getConfig(FILESYSTEM_OWNERSHIP_CHECK).get("key"));

        assertTrue(options.isEnabled(FILESYSTEM_OWNERSHIP_CHECK));
        options.disable(FILESYSTEM_OWNERSHIP_CHECK);
        assertFalse(options.isEnabled(FILESYSTEM_OWNERSHIP_CHECK));
        assertTrue(options.isDisabled(FILESYSTEM_OWNERSHIP_CHECK));
    }

    @Test
    public void testNoOptions()
    {
        StartupChecksOptions options = new StartupChecksOptions();

        assertTrue(options.isEnabled(NON_CONFIGURABLE_CHECK));

        // disabling does not to anything on non-configurable check
        options.disable(NON_CONFIGURABLE_CHECK);
        assertTrue(options.isEnabled(NON_CONFIGURABLE_CHECK));

        options.set(NON_CONFIGURABLE_CHECK, "key", "value");

        // we can not put anything into non-configurable check
        assertFalse(options.getConfig(NON_CONFIGURABLE_CHECK).containsKey("key"));
    }

    @Test
    public void testEmptyEnabledValues()
    {
        Map<StartupCheckType, Map<String, Object>> emptyConfig = new EnumMap<StartupCheckType, Map<String, Object>>(StartupCheckType.class) {{
            put(FILESYSTEM_OWNERSHIP_CHECK, new HashMap<String, Object>() {{
            }});
        }};

        Map<StartupCheckType, Map<String, Object>> emptyEnabledConfig = new EnumMap<StartupCheckType, Map<String, Object>>(StartupCheckType.class) {{
            put(FILESYSTEM_OWNERSHIP_CHECK, new HashMap<String, Object>() {{
                put(ENABLED_PROPERTY, null);
            }});
        }};

        // empty enabled property or enabled property with null value are still counted as enabled

        StartupChecksOptions options1 = new StartupChecksOptions(emptyConfig);
        assertTrue(options1.isEnabled(FILESYSTEM_OWNERSHIP_CHECK));

        StartupChecksOptions options2 = new StartupChecksOptions(emptyEnabledConfig);
        assertTrue(options2.isEnabled(FILESYSTEM_OWNERSHIP_CHECK));
    }

    @Test
    public void testExcludedKeyspacesInGCGracePeriodCheckOptions()
    {
        Map<String, Object> config = new HashMap<String, Object>(){{
            put("excluded_keyspaces", "ks1,ks2,ks3");
        }};
        GcGraceSecondsOnStartupCheck check = StartupChecks.checkGcGraceSecondsOnStartup;
        check.getExcludedKeyspaces(config);

        Set<String> excludedKeyspaces = check.getExcludedKeyspaces(config);
        assertEquals(3, excludedKeyspaces.size());
        assertTrue(excludedKeyspaces.contains("ks1"));
        assertTrue(excludedKeyspaces.contains("ks2"));
        assertTrue(excludedKeyspaces.contains("ks3"));
    }

    @Test
    public void testExcludedTablesInGCGracePeriodCheckOptions()
    {
        for (String input : new String[]{
        "ks1.tb1,ks1.tb2,ks3.tb3",
        " ks1 . tb1,  ks1 .tb2  ,ks3 .tb3  "
        })
        {
            Map<String, Object> config = new HashMap<String, Object>(){{
                put("excluded_tables", input);
            }};

            GcGraceSecondsOnStartupCheck check = StartupChecks.checkGcGraceSecondsOnStartup;
            Set<Pair<String, String>> excludedTables = check.getExcludedTables(config);
            assertEquals(3, excludedTables.size());
            assertTrue(excludedTables.contains(Pair.create("ks1", "tb1")));
            assertTrue(excludedTables.contains(Pair.create("ks1", "tb2")));
            assertTrue(excludedTables.contains(Pair.create("ks3", "tb3")));
        }
    }
}
