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
package org.apache.cassandra.service.snapshot;


import java.time.Instant;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Keyspace;

import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.ScheduledExecutors;

public class SnapshotManager {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private volatile ScheduledFuture cleanupTaskFuture;

    /**
     * Expiring ssnapshots ordered by expiration date, to allow only iterating over snapshots
     * that need to be removed on {@link this#clearExpiredSnapshots()}
     */
    private final PriorityQueue<TableSnapshotDetails> expiringSnapshots = new PriorityQueue<>(Comparator.comparing(x -> x.getExpiresAt()));

    public synchronized void start() {
        loadSnapshots();
        resumeSnapshotCleanup();
    }

    public synchronized void addSnapshot(TableSnapshotDetails snapshot) {
        // We currently only care about expiring snapshots
        if (snapshot.isExpiring()) {
            logger.debug("Adding expiring snapshot {}", snapshot);
            expiringSnapshots.add(snapshot);
        }
    }

    @VisibleForTesting
    protected synchronized void loadSnapshots() {
        logger.debug("Loading snapshots");
        StreamSupport.stream(Keyspace.all().spliterator(), false)
                     .flatMap(k -> k.getColumnFamilyStores().stream())
                     .flatMap(t -> t.getSnapshotDetails().values().stream())
                     .forEach(this::addSnapshot);
    }

    // TODO: Support pausing snapshot cleanup
    private synchronized void resumeSnapshotCleanup()
    {
        if (cleanupTaskFuture == null) {
            logger.info("Scheduling expired snapshot cleanup");
            cleanupTaskFuture = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(
            this::clearExpiredSnapshots, CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
            CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt(),
            TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    protected synchronized void clearExpiredSnapshots() {
        Instant now = Instant.now();
        while (!expiringSnapshots.isEmpty() && expiringSnapshots.peek().isExpired(now)) {
            TableSnapshotDetails expiredSnapshot = expiringSnapshots.poll();
            expiredSnapshot.deleteSnapshot();
        }
    }
}
