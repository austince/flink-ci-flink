package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.util.TestLogger;

import org.junit.Before;

import java.util.concurrent.CompletableFuture;

/**
 * Test base for {@link SlotPool} related test cases.
 */
public abstract class SlotPoolTestBase extends TestLogger {
	protected static final Time TIMEOUT = Time.seconds(10L);

	protected final ComponentMainThreadExecutor mainThreadExecutor =
		ComponentMainThreadExecutorServiceAdapter.forMainThread();

	protected TestingResourceManagerGateway resourceManagerGateway;
	protected SlotPoolBuilder slotPoolBuilder;

	@Before
	public void setup() throws Exception {
		resourceManagerGateway = new TestingResourceManagerGateway();
		slotPoolBuilder = new SlotPoolBuilder(mainThreadExecutor).setResourceManagerGateway(resourceManagerGateway);
	}

	protected void requestNewAllocatedSlots(final SlotPool slotPool, final SlotRequestId... slotRequestIds) {
		for (SlotRequestId slotRequestId : slotRequestIds) {
			requestNewAllocatedSlot(slotPool, slotRequestId);
		}
	}

	protected CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
		final SlotPool slotPool,
		final SlotRequestId slotRequestId) {
		return requestNewAllocatedSlot(slotPool, slotRequestId, TIMEOUT);
	}

	protected CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
		final SlotPool slotPool,
		final SlotRequestId slotRequestId,
		final Time timeout) {
		return slotPool.requestNewAllocatedSlot(slotRequestId, ResourceProfile.UNKNOWN, timeout);
	}
}
