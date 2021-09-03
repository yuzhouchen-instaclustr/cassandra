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


import java.io.File;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;

import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.ExecutorUtils;

public class SnapshotManager
{

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private static final DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("SnapshotCleanup");
    private final Supplier<Stream<TableSnapshot>> snapshotLoader;
    private final long initialDelaySeconds;
    private final long cleanupPeriodSeconds;
    private boolean snapshotsLoaded = false;

    @VisibleForTesting
    protected volatile ScheduledFuture cleanupTaskFuture;

    /**
     * Expiring snapshots ordered by expiration date, to allow only iterating over snapshots
     * that need to be removed on {@link this#clearExpiredSnapshots()}
     */
    private final PriorityQueue<TableSnapshot> expiringSnapshots = new PriorityQueue<>(Comparator.comparing(TableSnapshot::getExpiresAt));
    private final PriorityQueue<TableSnapshot> ephemeralSnapshots = new PriorityQueue<>(Comparator.comparing(TableSnapshot::getCreatedAt));

    private static final Supplier<Stream<TableSnapshot>> allSnapshotsLoader = () -> StreamSupport.stream(Keyspace.all().spliterator(), false).flatMap(Keyspace::getAllSnapshots);

    public static SnapshotManager createAllSnapshotsManager()
    {
        return new SnapshotManager(allSnapshotsLoader);
    }

    public SnapshotManager(Supplier<Stream<TableSnapshot>> snapshotLoader)
    {
        this(CassandraRelevantProperties.SNAPSHOT_CLEANUP_INITIAL_DELAY_SECONDS.getInt(),
             CassandraRelevantProperties.SNAPSHOT_CLEANUP_PERIOD_SECONDS.getInt(),
             snapshotLoader);
    }

    @VisibleForTesting
    protected SnapshotManager(long initialDelaySeconds, long cleanupPeriodSeconds,
                              Supplier<Stream<TableSnapshot>> snapshotLoader)
    {
        this.initialDelaySeconds = initialDelaySeconds;
        this.cleanupPeriodSeconds = cleanupPeriodSeconds;
        this.snapshotLoader = snapshotLoader;
    }

    public Collection<TableSnapshot> getExpiringSnapshots()
    {
        return expiringSnapshots;
    }

    public Collection<TableSnapshot> getEphemeralSnapshots()
    {
        return ephemeralSnapshots;
    }

    public synchronized void start()
    {
        loadSnapshots();
        resumeSnapshotCleanup();
    }

    public synchronized void stop() throws InterruptedException, TimeoutException
    {
        expiringSnapshots.clear();
        ephemeralSnapshots.clear();
        if (cleanupTaskFuture != null)
        {
            cleanupTaskFuture.cancel(false);
            cleanupTaskFuture = null;
        }
    }

    public synchronized void addSnapshot(TableSnapshot snapshot)
    {
        if (snapshot.isExpiring())
        {
            logger.debug("Adding expiring snapshot {}", snapshot);
            expiringSnapshots.add(snapshot);
        }
        else if (snapshot.isEphemeral())
        {
            logger.debug("Adding ephemeral snapshot {}", snapshot);
            ephemeralSnapshots.add(snapshot);
        }
    }

    public synchronized void loadSnapshots()
    {
        logger.debug("Loading snapshots");
        snapshotLoader.get().forEach(this::addSnapshot);
        snapshotsLoaded = true;
    }

    // TODO: Support pausing snapshot cleanup
    public synchronized void resumeSnapshotCleanup()
    {
        if (!snapshotsLoaded)
            loadSnapshots();

        if (cleanupTaskFuture == null)
        {
            logger.info("Scheduling expired snapshot cleanup with initialDelaySeconds={} and cleanupPeriodSeconds={}",
                        initialDelaySeconds,
                        cleanupPeriodSeconds);
            cleanupTaskFuture = executor.scheduleWithFixedDelay(this::clearExpiredSnapshots, initialDelaySeconds,
                                                                cleanupPeriodSeconds, TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    protected synchronized void clearExpiredSnapshots()
    {
        Instant now = Instant.now();
        while (!expiringSnapshots.isEmpty() && expiringSnapshots.peek().isExpired(now))
        {
            TableSnapshot expiredSnapshot = expiringSnapshots.peek();
            logger.debug("Removing expired snapshot {}.", expiredSnapshot);
            clearSnapshot(expiredSnapshot);
        }
    }

    public void clearEphemeralSnapshots()
    {
        for (TableSnapshot ephemeralSnapshot : ephemeralSnapshots)
        {
            clearSnapshot(ephemeralSnapshot);
            ephemeralSnapshots.remove(ephemeralSnapshot);
        }
    }

    /**
     * Deletes snapshot and remove it from manager
     */
    protected void clearSnapshot(TableSnapshot snapshot)
    {
        for (File snapshotDir : snapshot.getDirectories())
        {
            Directories.removeSnapshotDirectory(DatabaseDescriptor.getSnapshotRateLimiter(), snapshotDir);
        }
        expiringSnapshots.remove(snapshot);
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        try
        {
            ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
        }
        catch (Exception ex)
        {
            logger.error(String.format("Snapshot manager executor was not shut down in %s %s", timeout, unit), ex);
            throw ex;
        }
    }
}
