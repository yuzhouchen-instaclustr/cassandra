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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.service.GcGraceSecondsOnStartupCheck.parseHeartbeatFile;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.gc_grace_period;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HeartbeatServiceTest
{
    private static final Logger logger = LoggerFactory.getLogger(HeartbeatServiceTest.class);

    private static Path heartbeatFile;

    private HeartbeatService heartbeatService;

    @BeforeClass
    public static void beforeAll()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        heartbeatFile = Files.createTempFile("cassandra-heartbeat-test-", null);
    }

    @Before
    public void beforeTest() throws Exception
    {

        heartbeatService = new HeartbeatService() {
            @Override
            StartupChecksOptions getStartupChecksOptions()
            {
                StartupChecksOptions options = new StartupChecksOptions();
                options.enable(gc_grace_period);
                return options;
            }
        };
        heartbeatService.setDelay(2, SECONDS);
        heartbeatService.setHeartbeat(() -> {});
    }

    @Test
    public void testHeartbeatService() throws Exception
    {
        TestHeartbeat heartbeat = new TestHeartbeat(new File(heartbeatFile));
        heartbeatService.setHeartbeat(heartbeat);

        ScheduledExecutorPlus executor = executorFactory().scheduled(false, "HeartbeatServiceForTest");
        heartbeatService.setExecutor(executor);

        heartbeatService.start();

        await().atMost(1, MINUTES).until(() -> heartbeat.heartbeats.size() == 5);

        heartbeatService.stop();

        List<Instant> heartbeats = heartbeat.heartbeats;

        assertTrue(executor.isShutdown());
        assertFalse(heartbeats.isEmpty());
        assertEquals(5, heartbeats.size());
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
            fail("heartbeat service should fail as executor is already shut down");
        } catch (final Exception ex)
        {
            assertTrue(ex.getMessage().contains("Executor to run heartbeats on is shut down!"));
        }
    }

    private static class TestHeartbeat extends HeartbeatService.Heartbeat
    {
        private final List<Instant> heartbeats = new ArrayList<>();
        private boolean shouldRun = true;

        public TestHeartbeat(File heartbeatFile)
        {
            super(heartbeatFile);
        }

        @Override
        public void run()
        {
            if (shouldRun)
            {
                super.run();
                try
                {
                    Optional<Instant> instant = parseHeartbeatFile(heartbeatFile);
                    assertTrue(instant.isPresent());
                    logger.info("heartbeat timestamp: {}", instant.get());
                    heartbeats.add(instant.get());
                }
                catch (StartupException e)
                {
                    fail("Could not parse the heatbeat!");
                }
            }

            if (heartbeats.size() == 5)
            {
                shouldRun = false;
            }
        }
    }
}
