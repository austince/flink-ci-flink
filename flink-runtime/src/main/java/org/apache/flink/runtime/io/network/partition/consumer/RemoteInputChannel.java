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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferReceivedListener;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel, which requests a remote partition queue.
 */
public class RemoteInputChannel extends RecoveredInputChannel {

	/** ID to distinguish this channel from other channels sharing the same TCP connection. */
	private final InputChannelID id = new InputChannelID();

	/** The connection to use to request the remote partition. */
	private final ConnectionID connectionId;

	/** The connection manager to use connect to the remote partition provider. */
	private final ConnectionManager connectionManager;

	/**
	 * Flag indicating whether this channel has been released. Either called by the receiving task
	 * thread or the task manager actor.
	 */
	private final AtomicBoolean isReleased = new AtomicBoolean();

	/** Client to establish a (possibly shared) TCP connection and request the partition. */
	private volatile PartitionRequestClient partitionRequestClient;

	/**
	 * The next expected sequence number for the next buffer. This is modified by the network
	 * I/O thread only.
	 */
	private int expectedSequenceNumber = 0;

	/** The initial number of exclusive buffers assigned to this channel. */
	private int initialCredit;

	/** The number of available buffers that have not been announced to the producer yet. */
	private final AtomicInteger unannouncedCredit = new AtomicInteger(0);

	/** The latest already triggered checkpoint id which would be updated during {@link #requestInflightBuffers(long)}.*/
	@GuardedBy("receivedBuffers")
	private long lastRequestedCheckpointId = -1;

	/** The current received checkpoint id from the network. */
	private long receivedCheckpointId = -1;

	private final BufferManager bufferManager;

	public RemoteInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ConnectionID connectionId,
		ConnectionManager connectionManager,
		int initialBackOff,
		int maxBackoff,
		InputChannelMetrics metrics) {

		super(inputGate, channelIndex, partitionId, initialBackOff, maxBackoff, metrics);

		this.connectionId = checkNotNull(connectionId);
		this.connectionManager = checkNotNull(connectionManager);
		// In theory it should get the total number of states to indicate the numRequiredBuffers.
		// Since we can not get this information in advance, and considering only one input channel
		// will read state at the same time by design, then we give a maximum value here to reduce
		// unnecessary interactions with buffer pool during recovery.
		this.bufferManager = new BufferManager(this, Integer.MAX_VALUE);
	}

	/**
	 * Assigns exclusive buffers to this input channel, and this method should be called only once
	 * after this input channel is created.
	 */
	void assignExclusiveSegments() throws IOException {
		checkState(initialCredit == 0, "Bug in input channel setup logic: exclusive buffers have " +
			"already been set for this input channel.");

		initialCredit = bufferManager.requestExclusiveBuffers();
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * Requests a remote subpartition.
	 */
	@VisibleForTesting
	@Override
	public void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		if (partitionRequestClient == null) {
			// Create a client and request the partition
			try {
				partitionRequestClient = connectionManager.createPartitionRequestClient(connectionId);
			} catch (IOException e) {
				// IOExceptions indicate that we could not open a connection to the remote TaskExecutor
				throw new PartitionConnectionException(partitionId, e);
			}

			partitionRequestClient.requestSubpartition(partitionId, subpartitionIndex, this, 0);
		}
	}

	/**
	 * Retriggers a remote subpartition request.
	 */
	void retriggerSubpartitionRequest(int subpartitionIndex) throws IOException {
		checkState(partitionRequestClient != null, "Missing initial subpartition request.");

		if (increaseBackoff()) {
			partitionRequestClient.requestSubpartition(
				partitionId, subpartitionIndex, this, getCurrentBackoff());
		} else {
			failPartitionRequest();
		}
	}

	@Override
	Optional<BufferAndAvailability> getNextBuffer() throws IOException {
		checkState(!isReleased.get(), "Queried for a buffer after channel has been closed.");

		checkError();

		final Buffer next;
		final boolean moreAvailable;

		synchronized (receivedBuffers) {
			next = receivedBuffers.poll();
			moreAvailable = !receivedBuffers.isEmpty();
		}

		updateMetrics(next);
		return Optional.of(new BufferAndAvailability(next, moreAvailable, getSenderBacklog()));
	}

	@Override
	public List<Buffer> requestInflightBuffers(long checkpointId) throws IOException {
		synchronized (receivedBuffers) {
			checkState(checkpointId > lastRequestedCheckpointId, "Need to request the next checkpointId");

			final List<Buffer> inflightBuffers = new ArrayList<>(receivedBuffers.size());
			for (Buffer buffer : receivedBuffers) {
				CheckpointBarrier checkpointBarrier = parseCheckpointBarrierOrNull(buffer);
				if (checkpointBarrier != null && checkpointBarrier.getId() >= checkpointId) {
					break;
				}
				if (buffer.isBuffer()) {
					inflightBuffers.add(buffer.retainBuffer());
				}
			}

			lastRequestedCheckpointId = checkpointId;

			return inflightBuffers;
		}
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	void sendTaskEvent(TaskEvent event) throws IOException {
		checkState(!isReleased.get(), "Tried to send task event to producer after channel has been released.");
		checkState(partitionRequestClient != null, "Tried to send task event to producer before requesting a queue.");

		checkError();

		partitionRequestClient.sendTaskEvent(partitionId, event, this);
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	public boolean isReleased() {
		return isReleased.get();
	}

	/**
	 * Releases all exclusive and floating buffers, closes the partition request client.
	 */
	@Override
	void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			super.releaseAllResources();

			// The released flag has to be set before closing the connection to ensure that
			// buffers received concurrently with closing are properly recycled.
			if (partitionRequestClient != null) {
				partitionRequestClient.close(this);
			} else {
				connectionManager.closeOpenChannelConnections(connectionId);
			}
		}
	}

	private void failPartitionRequest() {
		setError(new PartitionNotFoundException(partitionId));
	}

	@Override
	public String toString() {
		return "RemoteInputChannel [" + partitionId + " at " + connectionId + "]";
	}

	// ------------------------------------------------------------------------
	// Credit-based
	// ------------------------------------------------------------------------

	private void mayNotifyCreditAvailable(int numNewCredit) {
		if (partitionRequestClient != null) {
			if (numNewCredit > 0 && unannouncedCredit.getAndAdd(numNewCredit) == 0) {
				partitionRequestClient.notifyCreditAvailable(this);
			}
		}
	}

	public int getNumberOfAvailableBuffers() {
		return bufferManager.getNumberOfAvailableBuffers();
	}

	public int getNumberOfRequiredBuffers() {
		return bufferManager.getNumberOfRequiredBuffers();
	}

	public int getSenderBacklog() {
		return bufferManager.getNumberOfRequiredBuffers() - initialCredit;
	}

	@VisibleForTesting
	boolean isWaitingForFloatingBuffers() {
		return bufferManager.isWaitingForFloatingBuffers();
	}

	@VisibleForTesting
	public Buffer getNextReceivedBuffer() {
		return receivedBuffers.poll();
	}

	public BufferManager getBufferManager() {
		return bufferManager;
	}

	@VisibleForTesting
	PartitionRequestClient getPartitionRequestClient() {
		return partitionRequestClient;
	}


	/**
	 * The unannounced credit is increased by thee given amount and might notify
	 * increased credit to the producer.
	 */
	@Override
	public void notifyBufferAvailable(int numAvailableBuffers) {
		mayNotifyCreditAvailable(numAvailableBuffers);
	}

	@Override
	public void resumeConsumption() {
		checkState(!isReleased.get(), "Channel released.");
		checkState(partitionRequestClient != null, "Trying to send event to producer before requesting a queue.");

		// notifies the producer that this channel is ready to
		// unblock from checkpoint and resume data consumption
		partitionRequestClient.resumeConsumption(this);
	}

	// ------------------------------------------------------------------------
	// Network I/O notifications (called by network I/O thread)
	// ------------------------------------------------------------------------

	/**
	 * Gets the currently unannounced credit.
	 *
	 * @return Credit which was not announced to the sender yet.
	 */
	public int getUnannouncedCredit() {
		return unannouncedCredit.get();
	}

	/**
	 * Gets the unannounced credit and resets it to <tt>0</tt> atomically.
	 *
	 * @return Credit which was not announced to the sender yet.
	 */
	public int getAndResetUnannouncedCredit() {
		return unannouncedCredit.getAndSet(0);
	}

	/**
	 * Gets the current number of received buffers which have not been processed yet.
	 *
	 * @return Buffers queued for processing.
	 */
	public int getNumberOfQueuedBuffers() {
		synchronized (receivedBuffers) {
			return receivedBuffers.size();
		}
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return Math.max(0, receivedBuffers.size());
	}

	public int unsynchronizedGetExclusiveBuffersUsed() {
		return Math.max(0, initialCredit - bufferManager.getNumberOfAvailableExclusiveBuffers());
	}

	public int unsynchronizedGetFloatingBuffersAvailable() {
		return Math.max(0, bufferManager.getNumberOfAvailableFloatingBuffers());
	}

	public InputChannelID getInputChannelId() {
		return id;
	}

	public int getInitialCredit() {
		return initialCredit;
	}

	public BufferProvider getBufferProvider() throws IOException {
		if (isReleased.get()) {
			return null;
		}

		return inputGate.getBufferProvider();
	}

	/**
	 * Requests buffer from input channel directly for receiving network data.
	 * It should always return an available buffer in credit-based mode unless
	 * the channel has been released.
	 *
	 * @return The available buffer.
	 */
	@Nullable
	public Buffer requestBuffer() {
		return bufferManager.requestBuffer();
	}

	/**
	 * Receives the backlog from the producer's buffer response. If the number of available
	 * buffers is less than backlog + initialCredit, it will request floating buffers from
	 * the buffer manager, and then notify unannounced credits to the producer.
	 *
	 * @param backlog The number of unsent buffers in the producer's sub partition.
	 */
	void onSenderBacklog(int backlog) throws IOException {
		int numRequestedBuffers = bufferManager.requestFloatingBuffers(backlog + initialCredit);
		mayNotifyCreditAvailable(numRequestedBuffers);
	}

	public void onBuffer(Buffer buffer, int sequenceNumber, int backlog) throws IOException {
		boolean recycleBuffer = true;

		try {
			if (expectedSequenceNumber != sequenceNumber) {
				onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
				return;
			}

			final boolean wasEmpty;
			final CheckpointBarrier notifyReceivedBarrier;
			final Buffer notifyReceivedBuffer;
			final BufferReceivedListener listener = inputGate.getBufferReceivedListener();
			synchronized (receivedBuffers) {
				// Similar to notifyBufferAvailable(), make sure that we never add a buffer
				// after releaseAllResources() released all buffers from receivedBuffers
				// (see above for details).
				if (isReleased.get()) {
					return;
				}

				wasEmpty = receivedBuffers.isEmpty();
				receivedBuffers.add(buffer);

				if (listener != null && buffer.isBuffer() && receivedCheckpointId < lastRequestedCheckpointId) {
					notifyReceivedBuffer = buffer.retainBuffer();
				} else {
					notifyReceivedBuffer = null;
				}
				notifyReceivedBarrier = listener != null ? parseCheckpointBarrierOrNull(buffer) : null;
			}
			recycleBuffer = false;

			++expectedSequenceNumber;

			if (wasEmpty) {
				notifyChannelNonEmpty();
			}

			if (backlog >= 0) {
				onSenderBacklog(backlog);
			}

			if (notifyReceivedBarrier != null) {
				receivedCheckpointId = notifyReceivedBarrier.getId();
				listener.notifyBarrierReceived(notifyReceivedBarrier, channelInfo);
			} else if (notifyReceivedBuffer != null) {
				listener.notifyBufferReceived(notifyReceivedBuffer, channelInfo);
			}
		} finally {
			if (recycleBuffer) {
				buffer.recycleBuffer();
			}
		}
	}

	public void onEmptyBuffer(int sequenceNumber, int backlog) throws IOException {
		boolean success = false;

		synchronized (receivedBuffers) {
			if (!isReleased.get()) {
				if (expectedSequenceNumber == sequenceNumber) {
					expectedSequenceNumber++;
					success = true;
				} else {
					onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
				}
			}
		}

		if (success && backlog >= 0) {
			onSenderBacklog(backlog);
		}
	}

	public void onFailedPartitionRequest() {
		inputGate.triggerPartitionStateCheck(partitionId);
	}

	public void onError(Throwable cause) {
		setError(cause);
	}

	private static class BufferReorderingException extends IOException {

		private static final long serialVersionUID = -888282210356266816L;

		private final int expectedSequenceNumber;

		private final int actualSequenceNumber;

		BufferReorderingException(int expectedSequenceNumber, int actualSequenceNumber) {
			this.expectedSequenceNumber = expectedSequenceNumber;
			this.actualSequenceNumber = actualSequenceNumber;
		}

		@Override
		public String getMessage() {
			return String.format("Buffer re-ordering: expected buffer with sequence number %d, but received %d.",
				expectedSequenceNumber, actualSequenceNumber);
		}
	}
}
