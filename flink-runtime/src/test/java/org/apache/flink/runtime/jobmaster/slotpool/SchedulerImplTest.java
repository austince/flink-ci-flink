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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.TestingComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.scheduler.DummyScheduledUnit;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.jobmaster.slotpool.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SchedulerImpl}.
 */
public class SchedulerImplTest extends TestLogger {

	private static final Time timeout = Time.seconds(1L);

	@ClassRule
	public static final TestingComponentMainThreadExecutor.Resource EXECUTOR_RESOURCE =
		new TestingComponentMainThreadExecutor.Resource(10L);

	private final TestingComponentMainThreadExecutor testMainThreadExecutor =
		EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

	private TaskManagerLocation taskManagerLocation;

	private SimpleAckingTaskManagerGateway taskManagerGateway;

	private TestingResourceManagerGateway resourceManagerGateway;

	@Before
	public void setUp() throws Exception {
		taskManagerLocation = new LocalTaskManagerLocation();
		taskManagerGateway = new SimpleAckingTaskManagerGateway();
		resourceManagerGateway = new TestingResourceManagerGateway();
	}

	@Test
	public void testAllocateSlot() throws Exception {
		CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequestFuture::complete);

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(new JobID())) {
			testMainThreadExecutor.execute(() -> slotPool.registerTaskManager(taskManagerLocation.getResourceID()));

			Scheduler scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
			scheduler.start(testMainThreadExecutor.getMainThreadExecutor());

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = testMainThreadExecutor.execute(() -> scheduler.allocateSlot(
				requestId,
				new DummyScheduledUnit(),
				SlotProfile.noRequirements(),
				timeout));
			assertFalse(future.isDone());

			final SlotRequest slotRequest = slotRequestFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			assertTrue(testMainThreadExecutor.execute(() -> slotPool.offerSlot(
				taskManagerLocation,
				taskManagerGateway,
				slotOffer)));

			LogicalSlot slot = future.get(1, TimeUnit.SECONDS);
			assertTrue(future.isDone());
			assertTrue(slot.isAlive());
			assertEquals(taskManagerLocation, slot.getTaskManagerLocation());
		}
	}

	/**
	 * This case make sure when allocateSlot in ProviderAndOwner timeout,
	 * it will automatically call cancelSlotAllocation as will inject future.whenComplete in ProviderAndOwner.
	 */
	@Test
	public void testProviderAndOwnerSlotAllocationTimeout() throws Exception {
		final JobID jid = new JobID();

		try (TestingSlotPoolImpl pool = createTestingSlotPool(jid)) {

			final CompletableFuture<SlotRequestId> releaseSlotFuture = new CompletableFuture<>();

			pool.setReleaseSlotConsumer(releaseSlotFuture::complete);

			pool.start(JobMasterId.generate(), "foobar",
				testMainThreadExecutor.getMainThreadExecutor());
			ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			pool.connectToResourceManager(resourceManagerGateway);

			Scheduler scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), pool);
			scheduler.start(testMainThreadExecutor.getMainThreadExecutor());

			// test the pending request is clear when timed out
			CompletableFuture<LogicalSlot> future = testMainThreadExecutor.execute(() -> scheduler.allocateSlot(
				new DummyScheduledUnit(),
				SlotProfile.noRequirements(),
				timeout));
			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait for the cancel call on the SlotPoolImpl
			releaseSlotFuture.get();

			assertEquals(0L, pool.getNumberOfPendingRequests());
		}
	}

	@Nonnull
	private TestingSlotPoolImpl createTestingSlotPool(JobID jid) {
		return new TestingSlotPoolImpl(jid);
	}

	private static void setupSlotPool(
		SlotPoolImpl slotPool,
		ResourceManagerGateway resourceManagerGateway,
		ComponentMainThreadExecutor mainThreadExecutable) throws Exception {
		final String jobManagerAddress = "foobar";

		slotPool.start(JobMasterId.generate(), jobManagerAddress, mainThreadExecutable);

		slotPool.connectToResourceManager(resourceManagerGateway);
	}

	private SlotPoolImpl createAndSetUpSlotPool(JobID jid) throws Exception {
		final SlotPoolImpl slotPool = createTestingSlotPool(jid);
		setupSlotPool(slotPool, resourceManagerGateway, testMainThreadExecutor.getMainThreadExecutor());
		return slotPool;
	}
}
