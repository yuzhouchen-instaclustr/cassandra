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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.io.util.File;

import static java.time.Instant.ofEpochMilli;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.io.util.FileUtils.write;
import static org.apache.cassandra.service.GcGraceSecondsOnStartupCheck.getHeartbeatFile;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.gc_grace_period;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.ExecutorUtils.shutdownNowAndWait;

public class HeartbeatService
{
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatService.class);

    public static final HeartbeatService instance = new HeartbeatService();
    private ScheduledExecutorPlus executor;
    private long delay = 1;
    private TimeUnit delayTimeUnit = MINUTES;

    private boolean started = false;
    private Runnable heartbeat;

    @VisibleForTesting
    HeartbeatService()
    {
    }

    @VisibleForTesting
    void setHeartbeat(final Runnable heartbeat)
    {
        this.heartbeat = heartbeat;
    }

    @VisibleForTesting
    void setDelay(long delay, TimeUnit delayTimeUnit)
    {
        if (delay > 0 && delayTimeUnit != null)
        {
            this.delay = delay;
            this.delayTimeUnit = delayTimeUnit;
        }
    }

    @VisibleForTesting
    void setExecutor(final ScheduledExecutorPlus executor)
    {
        if (started)
            throw new IllegalStateException("Can not set executor when service is started. Stop it first.");

        this.executor = executor;
    }

    @VisibleForTesting
    StartupChecksOptions getStartupChecksOptions()
    {
        return DatabaseDescriptor.getStartupChecksOptions();
    }

    public synchronized void start()
    {
        if (started)
            return;

        if (!getStartupChecksOptions().isEnabled(gc_grace_period))
        {
            logger.debug("Heartbeat service is disabled.");
            return;
        }

        if (heartbeat == null)
            heartbeat = new Heartbeat(getHeartbeatFile(getStartupChecksOptions().getConfig(gc_grace_period)));

        if (executor == null)
            executor = executorFactory().scheduled(false, "HeartbeatService");

        if (executor.isShutdown())
            throw new IllegalStateException("Executor to run heartbeats on is shut down!");

        executor.scheduleWithFixedDelay(heartbeat, 0, delay, delayTimeUnit);

        started = true;
    }

    public synchronized void stop() throws InterruptedException, TimeoutException
    {
        if (executor != null)
            shutdownNowAndWait(1L, MINUTES, executor);

        executor = null;
        started = false;
    }

    static class Heartbeat implements Runnable
    {
        final File heartbeatFile;

        Heartbeat(File heartbeatFile)
        {
            this.heartbeatFile = heartbeatFile;
        }

        @Override
        public void run()
        {
            write(heartbeatFile, ofEpochMilli(currentTimeMillis()).toString());
        }
    }
}
