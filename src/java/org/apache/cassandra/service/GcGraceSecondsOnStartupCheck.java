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

package org.apache.cassandra.service;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static java.time.Instant.ofEpochMilli;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.cassandra.config.CassandraRelevantProperties.LINE_SEPARATOR;
import static org.apache.cassandra.exceptions.StartupException.ERR_WRONG_DISK_STATE;
import static org.apache.cassandra.exceptions.StartupException.ERR_WRONG_MACHINE_STATE;
import static org.apache.cassandra.io.util.FileUtils.readLines;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class GcGraceSecondsOnStartupCheck implements StartupCheck
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GcGraceSecondsOnStartupCheck.class);

    public static final String HEARTBEAT_FILE_CONFIG_PROPERTY = "heartbeat_file";
    public static final String EXCLUDED_KEYSPACES_CONFIG_PROPERTY = "excluded_keyspaces";
    public static final String EXCLUDED_TABLES_CONFIG_PROPERTY = "excluded_tables";

    public static final String DEFAULT_HEARTBEAT_FILE = ".cassandra-heartbeat";

    @VisibleForTesting
    static class TableGCPeriod
    {
        String table;
        int gcPeriod;

        TableGCPeriod(String table, int gcPeriod)
        {
            this.table = table;
            this.gcPeriod = gcPeriod;
        }
    }

    @Override
    public StartupChecks.StartupCheckType getStartupCheckType()
    {
        return StartupChecks.StartupCheckType.gc_grace_period;
    }

    @Override
    public void execute(StartupChecksOptions options) throws StartupException
    {
        if (options.isDisabled(getStartupCheckType()))
            return;

        Map<String, Object> config = options.getConfig(getStartupCheckType());
        File heartbeatFile = getHeartbeatFile(config);

        if (!heartbeatFile.exists())
        {
            LOGGER.debug("Heartbeat file {} not found! Skipping heartbeat startup check.", heartbeatFile.absolutePath());
            return;
        }

        // we expect heartbeat value to be on the first line
        Optional<Instant> heartbeatOptional = parseHeartbeatFile(heartbeatFile);
        if (!heartbeatOptional.isPresent())
            return;

        Instant heartbeat = heartbeatOptional.get();
        long heartbeatMillis = heartbeat.toEpochMilli();

        List<Pair<String, String>> violations = new ArrayList<>();

        Set<String> excludedKeyspaces = getExcludedKeyspaces(config);
        Set<Pair<String, String>> excludedTables = getExcludedTables(config);

        long currentTimeMillis = currentTimeMillis();

        for (String keyspace : getKeyspaces())
        {
            if (excludedKeyspaces.contains(keyspace))
                continue;

            for (TableGCPeriod userTable : getTablesGcPeriods(keyspace))
            {
                if (excludedTables.contains(Pair.create(keyspace, userTable.table)))
                    continue;

                long gcGraceMillis = ((long) userTable.gcPeriod) * 1000;
                if (heartbeatMillis + gcGraceMillis < currentTimeMillis)
                    violations.add(Pair.create(keyspace, userTable.table));
            }
        }

        if (!violations.isEmpty())
        {
            String invalidTables = violations.stream()
                                             .map(p -> format("%s.%s", p.left, p.right))
                                             .collect(joining(","));

            String exceptionMessage = format("There are tables for which gcGraceSeconds is older " +
                                             "then the lastly known time Cassandra node was up based " +
                                             "on its heartbeat %s. Cassandra node will not start " +
                                             "as it would likely introduce data consistency " +
                                             "issues (zombies etc). Please resolve these issues manually " +
                                             "and start the node again. Invalid tables: %s",
                                             heartbeat, invalidTables);

            throw new StartupException(ERR_WRONG_MACHINE_STATE, exceptionMessage);
        }

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(() -> FileUtils.write(heartbeatFile,
                                                                         ofEpochMilli(currentTimeMillis()).toString()),
                                                              0, 1, TimeUnit.MINUTES);
    }

    static File getHeartbeatFile(Map<String, Object> config)
    {
        String heartbeatFileConfigValue = (String) config.get(HEARTBEAT_FILE_CONFIG_PROPERTY);

        return heartbeatFileConfigValue == null
               ? new File(DEFAULT_HEARTBEAT_FILE)
               : new File(heartbeatFileConfigValue);
    }

    @VisibleForTesting
    public Set<String> getExcludedKeyspaces(Map<String, Object> config)
    {
        String excludedKeyspacesConfigValue = (String) config.get(EXCLUDED_KEYSPACES_CONFIG_PROPERTY);

        if (excludedKeyspacesConfigValue == null)
            return Collections.emptySet();
        else
            return Arrays.stream(excludedKeyspacesConfigValue.trim().split(","))
                         .map(String::trim)
                         .collect(toSet());
    }

    @VisibleForTesting
    public Set<Pair<String, String>> getExcludedTables(Map<String, Object> config)
    {
        String excludedKeyspacesConfigValue = (String) config.get(EXCLUDED_TABLES_CONFIG_PROPERTY);

        if (excludedKeyspacesConfigValue == null)
            return Collections.emptySet();

        Set<Pair<String, String>> pairs = new HashSet<>();

        for (String keyspaceTable : excludedKeyspacesConfigValue.trim().split(","))
        {
            String[] pair = keyspaceTable.trim().split("\\.");
            if (pair.length != 2)
                continue;

            pairs.add(Pair.create(pair[0].trim(), pair[1].trim()));
        }

        return pairs;
    }

    @VisibleForTesting
    List<String> getKeyspaces()
    {
        return Schema.instance.getUserKeyspaces()
                              .stream()
                              .map(keyspaceMetadata -> keyspaceMetadata.name)
                              .collect(toList());
    }

    @VisibleForTesting
    List<TableGCPeriod> getTablesGcPeriods(String userKeyspace)
    {
        return StreamSupport.stream(Schema.instance.getTablesAndViews(userKeyspace).spliterator(), false)
                            .map(metadata -> new TableGCPeriod(metadata.name, metadata.params.gcGraceSeconds))
                            .collect(toList());
    }

    @VisibleForTesting
    static Optional<Instant> parseHeartbeatFile(File heartbeatFile) throws StartupException
    {
        List<String> heartbeatFileLines;

        try
        {
            heartbeatFileLines = unmodifiableList(readLines(heartbeatFile));
        }
        catch (Exception ex)
        {
            throw new StartupException(ERR_WRONG_DISK_STATE,
                                       format("Unable to read heartbeat file %s", heartbeatFile.absolutePath()));
        }

        if (heartbeatFileLines.isEmpty())
        {
            LOGGER.debug("Heartbeat file {} is empty! Skipping heartbeat startup check.", heartbeatFile.absolutePath());
            return Optional.empty();
        }

        // we expect heartbeat value to be on the first line
        Instant heartbeat = parseHeartbeat(heartbeatFileLines.get(0));
        LOGGER.debug("Resolved heartbeat with the lastly know time this node was running: {}", heartbeat);

        return Optional.of(heartbeat);
    }

    private static Instant parseHeartbeat(String heartbeatLine) throws StartupException
    {
        String heartbeat = heartbeatLine.replaceAll(LINE_SEPARATOR.getString(), "");
        try
        {
            return Instant.parse(heartbeat);
        }
        catch (DateTimeParseException ex)
        {
            throw new StartupException(ERR_WRONG_MACHINE_STATE, format("Unable to parse heartbeat '%s'!", heartbeat));
        }
    }
}
