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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.resourcemanager.exceptions.UnfulfillableSlotRequestException;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * Tests for batch slot requests.
 */
public class SlotPoolBatchSlotRequestTest extends TestLogger {

	private static final ComponentMainThreadExecutor mainThreadExecutor =
		ComponentMainThreadExecutorServiceAdapter.forMainThread();

	private static final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1024);
	private static final ResourceProfile smallerResourceProfile = ResourceProfile.fromResources(0.5, 512);
	public static final CompletableFuture[] COMPLETABLE_FUTURES_EMPTY_ARRAY = new CompletableFuture[0];

	private SlotPoolBuilder slotPoolBuilder;
	private TestingResourceManagerGateway testingResourceManagerGateway;

	@Before
	public void setup() throws Exception {
		testingResourceManagerGateway = new TestingResourceManagerGateway();
		slotPoolBuilder = new SlotPoolBuilder(mainThreadExecutor);
	}

	/**
	 * Tests that a batch slot request fails if there is no slot which can fulfill the
	 * slot request.
	 */
	@Test
	public void testPendingBatchSlotRequestTimeout() throws Exception {
		try (final SlotPoolImpl slotPool = slotPoolBuilder
				.setBatchSlotTimeout(Time.milliseconds(2L))
				.build()) {
			final CompletableFuture<PhysicalSlot> slotFuture = SlotPoolUtils.requestNewAllocatedBatchSlot(
				slotPool,
				mainThreadExecutor,
				ResourceProfile.UNKNOWN);

			try {
				slotFuture.get();
				fail("Expected that slot future times out.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.stripExecutionException(ee), instanceOf(TimeoutException.class));
			}
		}
	}

	/**
	 * Tests that a batch slot request won't time out if there exists a slot in the
	 * SlotPool which fulfills the requested {@link ResourceProfile}.
	 */
	@Test
	public void testPendingBatchSlotRequestDoesNotTimeoutIfFulfillingSlotExists() throws Exception {
		final Time batchSlotTimeout = Time.milliseconds(2L);
		final ManualClock clock = new ManualClock();

		try (final TestingSlotPoolImpl slotPool = slotPoolBuilder
				.setClock(clock)
				.setBatchSlotTimeout(batchSlotTimeout)
				.build()) {

			SlotPoolUtils.offerSlots(slotPool, mainThreadExecutor, Collections.singletonList(resourceProfile));

			final CompletableFuture<PhysicalSlot> firstSlotFuture =
				SlotPoolUtils.requestNewAllocatedBatchSlot(slotPool, mainThreadExecutor, resourceProfile);
			final CompletableFuture<PhysicalSlot> secondSlotFuture =
				SlotPoolUtils.requestNewAllocatedBatchSlot(slotPool, mainThreadExecutor, ResourceProfile.UNKNOWN);
			final CompletableFuture<PhysicalSlot> thirdSlotFuture =
				SlotPoolUtils.requestNewAllocatedBatchSlot(slotPool, mainThreadExecutor, smallerResourceProfile);

			final List<CompletableFuture<PhysicalSlot>> slotFutures =
				Arrays.asList(firstSlotFuture, secondSlotFuture, thirdSlotFuture);
			advanceTimeAndTriggerCheckBatchSlotTimeout(slotPool, clock, batchSlotTimeout);

			for (CompletableFuture<PhysicalSlot> slotFuture : slotFutures) {
				assertThat(slotFuture.isDone(), is(false));
			}
		}

	}

	/**
	 * Tests that a batch slot request does not react to {@link SlotPool#failAllocation(AllocationID, Exception)}
	 * signals whose exception is not {@link UnfulfillableSlotRequestException}.
	 */
	@Test
	public void testPendingBatchSlotRequestDoesNotFailIfAllocationFails() throws Exception {
		final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
		testingResourceManagerGateway.setRequestSlotConsumer(
			slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

		final Time batchSlotTimeout = Time.milliseconds(1000L);
		try (final SlotPoolImpl slotPool = slotPoolBuilder
				.setBatchSlotTimeout(batchSlotTimeout)
				.setResourceManagerGateway(testingResourceManagerGateway)
				.build()) {

			final CompletableFuture<PhysicalSlot> slotFuture = SlotPoolUtils.requestNewAllocatedBatchSlot(
				slotPool,
				mainThreadExecutor,
				resourceProfile);

			SlotPoolUtils.failAllocation(
				slotPool,
				mainThreadExecutor,
				allocationIdFuture.get(),
				new FlinkException("Failed request"));

			assertThat(slotFuture.isDone(), is(false));
		}
	}

	/**
	 * Tests that a batch slot request does react to {@link SlotPool#failAllocation(AllocationID, Exception)}
	 * signals whose exception is {@link UnfulfillableSlotRequestException}.
	 */
	@Test
	public void testPendingBatchSlotRequestFailsIfAllocationFailsUnfulfillably() throws Exception {
		final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
		testingResourceManagerGateway.setRequestSlotConsumer(
			slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

		try (final SlotPoolImpl slotPool = slotPoolBuilder
				.setResourceManagerGateway(testingResourceManagerGateway)
				.build()) {

			final CompletableFuture<PhysicalSlot> slotFuture = SlotPoolUtils.requestNewAllocatedBatchSlot(
				slotPool,
				mainThreadExecutor,
				resourceProfile);

			SlotPoolUtils.failAllocation(slotPool,
				mainThreadExecutor,
				allocationIdFuture.get(),
				new UnfulfillableSlotRequestException(new AllocationID(), ResourceProfile.UNKNOWN));

			assertThat(slotFuture.isCompletedExceptionally(), is(true));
		}
	}

	/**
	 * Tests that a batch slot request won't fail if its resource manager request fails with exceptions other than
	 * {@link UnfulfillableSlotRequestException}.
	 */
	@Test
	public void testPendingBatchSlotRequestDoesNotFailIfRMRequestFails() throws Exception {
		testingResourceManagerGateway.setRequestSlotFuture(FutureUtils.completedExceptionally(new FlinkException("Failed request")));

		final Time batchSlotTimeout = Time.milliseconds(1000L);
		try (final SlotPoolImpl slotPool = slotPoolBuilder
			.setBatchSlotTimeout(batchSlotTimeout)
			.setResourceManagerGateway(testingResourceManagerGateway)
			.build()) {

			final CompletableFuture<PhysicalSlot> slotFuture =
				SlotPoolUtils.requestNewAllocatedBatchSlot(slotPool, mainThreadExecutor, resourceProfile);

			assertThat(slotFuture.isDone(), is(false));
		}
	}

	/**
	 * Tests that a batch slot request fails if its resource manager request fails with {@link UnfulfillableSlotRequestException}.
	 */
	@Test
	public void testPendingBatchSlotRequestFailsIfRMRequestFailsUnfulfillably() throws Exception {
		testingResourceManagerGateway.setRequestSlotFuture(FutureUtils.completedExceptionally(
			new UnfulfillableSlotRequestException(new AllocationID(), ResourceProfile.UNKNOWN)));

		try (final SlotPoolImpl slotPool = slotPoolBuilder
			.setResourceManagerGateway(testingResourceManagerGateway)
			.build()) {

			final CompletableFuture<PhysicalSlot> slotFuture =
				SlotPoolUtils.requestNewAllocatedBatchSlot(slotPool, mainThreadExecutor, resourceProfile);

			assertThat(slotFuture.isCompletedExceptionally(), is(true));
		}
	}

	/**
	 * Tests that a pending batch slot request times out after the last fulfilling slot gets
	 * released.
	 */
	@Test
	public void testPendingBatchSlotRequestTimeoutAfterSlotRelease() throws Exception {
		final ManualClock clock = new ManualClock();
		final Time batchSlotTimeout = Time.milliseconds(1000L);

		try (final TestingSlotPoolImpl slotPool = slotPoolBuilder
				.setClock(clock)
				.setBatchSlotTimeout(batchSlotTimeout)
				.build()) {
			final ResourceID taskManagerResourceId =
				SlotPoolUtils.offerSlots(slotPool, mainThreadExecutor, Collections.singletonList(resourceProfile));
			final CompletableFuture<PhysicalSlot> firstSlotFuture =
				SlotPoolUtils.requestNewAllocatedBatchSlot(slotPool, mainThreadExecutor, resourceProfile);
			final CompletableFuture<PhysicalSlot> secondSlotFuture =
				SlotPoolUtils.requestNewAllocatedBatchSlot(slotPool, mainThreadExecutor, ResourceProfile.UNKNOWN);
			final CompletableFuture<PhysicalSlot> thirdSlotFuture =
				SlotPoolUtils.requestNewAllocatedBatchSlot(slotPool, mainThreadExecutor, smallerResourceProfile);

			final List<CompletableFuture<PhysicalSlot>> slotFutures =
				Arrays.asList(firstSlotFuture, secondSlotFuture, thirdSlotFuture);

			// initial batch slot timeout check
			advanceTimeAndTriggerCheckBatchSlotTimeout(slotPool, clock, batchSlotTimeout);

			assertThat(CompletableFuture.anyOf(slotFutures.toArray(COMPLETABLE_FUTURES_EMPTY_ARRAY)).isDone(),
				is(false));

			SlotPoolUtils.releaseTaskManager(slotPool, mainThreadExecutor, taskManagerResourceId);

			advanceTimeAndTriggerCheckBatchSlotTimeout(slotPool, clock, batchSlotTimeout);

			for (CompletableFuture<PhysicalSlot> slotFuture : slotFutures) {
				assertThat(slotFuture.isCompletedExceptionally(), is(true));

				try {
					slotFuture.get();
					fail("Expected that the slot future times out.");
				} catch (ExecutionException ee) {
					assertThat(ExceptionUtils.stripExecutionException(ee), instanceOf(TimeoutException.class));
				}
			}
		}
	}

	private void advanceTimeAndTriggerCheckBatchSlotTimeout(
		TestingSlotPoolImpl slotPool,
		ManualClock clock,
		Time batchSlotTimeout) {
		// trigger batch slot timeout check which marks unfulfillable slots
		slotPool.triggerCheckBatchSlotTimeout();

		// advance clock behind timeout
		clock.advanceTime(batchSlotTimeout.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);

		// timeout all as unfulfillable marked slots
		slotPool.triggerCheckBatchSlotTimeout();
	}
}
