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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Clock;
import org.awaitility.Awaitility;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class HeartbeatServiceTest
{
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatServiceTest.class);

    private HeartbeatService heartbeatService;

    @BeforeClass
    public static void beforeAll()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void beforeTest()
    {
        heartbeatService = new HeartbeatService();
        heartbeatService.setDelay(2, TimeUnit.SECONDS);
        heartbeatService.setHeartbeat(() -> {});
    }

    @Test
    public void testHeartbeatService() throws Exception
    {
        TestHeartbeat heartbeat = new TestHeartbeat();
        heartbeatService.setHeartbeat(heartbeat);

        ScheduledExecutorPlus executor = executorFactory().scheduled(false, "HeartbeatServiceForTest");
        heartbeatService.setExecutor(executor);

        heartbeatService.start();

        Awaitility.await()
                  .atMost(1, TimeUnit.MINUTES)
                  .until(() -> heartbeat.heartbeats.size() == 5);

        heartbeatService.stop();

        List<Long> heartbeats = heartbeat.heartbeats;

        Assert.assertTrue(executor.isShutdown());
        Assert.assertFalse(heartbeats.isEmpty());
        Assert.assertEquals(5, heartbeats.size());
    }

    @Test
    public void testHeartbeatServiceRestart() throws Exception
    {
        heartbeatService.start();
        heartbeatService.stop();
        heartbeatService.start();
        heartbeatService.stop();
    }

    @Test
    public void testHeartbeatServiceWithShutdownExecutorFails() throws Exception
    {
        ScheduledExecutorPlus executor = executorFactory().scheduled(false, "HeartbeatServiceForTest");
        heartbeatService.setExecutor(executor);

        heartbeatService.start();
        heartbeatService.stop();

        // at this point, executor is already shut down so we can not reuse it
        heartbeatService.setExecutor(executor);

        try
        {
            heartbeatService.start();
            Assert.fail("heartbeat service should fail as executor is already shut down");
        } catch (final Exception ex)
        {
            // intentionally empty
        }
    }

    private static class TestHeartbeat implements Runnable
    {
        private List<Long> heartbeats = new ArrayList<>();
        private boolean shouldRun = true;

        @Override
        public void run()
        {
            if (shouldRun)
            {
                long heartbeatTimestamp = Clock.Global.currentTimeMillis();
                logger.info("heartbeat timestamp: {}", heartbeatTimestamp);
                heartbeats.add(heartbeatTimestamp);
            }

            if (heartbeats.size() == 5)
            {
                shouldRun = false;
            }
        }
    }
}
