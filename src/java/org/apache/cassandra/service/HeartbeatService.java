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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.DatabaseDescriptor.getStartupChecksOptions;
import static org.apache.cassandra.service.StartupChecks.GcGraceSecondsOnStartupCheck.getHeartbeatFile;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.GC_GRACE_PERIOD_CHECK;

public class HeartbeatService
{
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatService.class);

    public static final HeartbeatService instance = new HeartbeatService();
    private ScheduledExecutorPlus executor;
    private long delay = 1;
    private TimeUnit delayTimeUnit = TimeUnit.MINUTES;

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

    public synchronized void start()
    {
        if (started)
            return;

        if (!getStartupChecksOptions().isEnabled(GC_GRACE_PERIOD_CHECK))
        {
            logger.debug("Heartbeat service is disabled.");
            return;
        }

        if (heartbeat == null)
            heartbeat = new Heartbeat();

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
            ExecutorUtils.shutdownNowAndWait(1L, TimeUnit.MINUTES, executor);

        executor = null;
        started = false;
    }

    private static class Heartbeat implements Runnable
    {
        private final File heartbeatFile;

        public Heartbeat()
        {
            Map<String, Object> config = getStartupChecksOptions().getConfig(GC_GRACE_PERIOD_CHECK);
            heartbeatFile = getHeartbeatFile(config);
        }

        @Override
        public void run()
        {
            FileUtils.write(heartbeatFile, Long.toString(Clock.Global.currentTimeMillis()));
        }
    }
}
