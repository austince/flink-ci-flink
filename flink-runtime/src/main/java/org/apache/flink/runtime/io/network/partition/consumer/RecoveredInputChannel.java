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
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader.ReadResult;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;

/**
 * An input channel reads recovered state from previous unaligned checkpoint snapshots
 * via {@link ChannelStateReader}.
 */
public abstract class RecoveredInputChannel extends InputChannel {

	final ArrayDeque<Buffer> receivedBuffers = new ArrayDeque<>();

	RecoveredInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			int initialBackoff,
			int maxBackoff,
			InputChannelMetrics metrics) {
		super(inputGate, channelIndex, partitionId, initialBackoff, maxBackoff, metrics.getNumBytesInRemoteCounter(), metrics.getNumBuffersInRemoteCounter());
	}

	protected void readRecoveredState(ChannelStateReader reader) throws IOException, InterruptedException {
		ReadResult result = ReadResult.HAS_MORE_DATA;
		while (result == ReadResult.HAS_MORE_DATA) {
			Buffer buffer = getBufferManager().requestBufferBlocking();
			result = internalReaderRecoveredState(reader, buffer);
		}
	}

	private ReadResult internalReaderRecoveredState(ChannelStateReader reader, Buffer buffer) throws IOException {
		ReadResult result;
		try {
			result = reader.readInputData(channelInfo, buffer);
		} catch (Throwable t) {
			buffer.recycleBuffer();
			throw t;
		}
		if (buffer.readableBytes() > 0) {
			onRecoveredStateBuffer(buffer);
		} else {
			buffer.recycleBuffer();
		}
		return result;
	}

	@VisibleForTesting
	void onRecoveredStateBuffer(Buffer buffer) {
		boolean recycleBuffer = true;
		try {
			final boolean wasEmpty;
			synchronized (receivedBuffers) {
				// Similar to notifyBufferAvailable(), make sure that we never add a buffer
				// after releaseAllResources() released all buffers from receivedBuffers.
				if (isReleased()) {
					return;
				}

				wasEmpty = receivedBuffers.isEmpty();
				receivedBuffers.add(buffer);
				recycleBuffer = false;
			}

			if (wasEmpty) {
				notifyChannelNonEmpty();
			}
		} finally {
			if (recycleBuffer) {
				buffer.recycleBuffer();
			}
		}
	}

	@Nullable
	BufferAndAvailability getNextRecoveredStateBuffer() {
		final Buffer next;
		final boolean moreAvailable;

		synchronized (receivedBuffers) {
			next = receivedBuffers.poll();
			if (next == null) {
				return null;
			} else {
				moreAvailable = !receivedBuffers.isEmpty();
				return new BufferAndAvailability(next, moreAvailable, 0);
			}
		}
	}

	void releaseAllResources() throws IOException {
		ArrayDeque<Buffer> releasedBuffers;
		synchronized (receivedBuffers) {
			releasedBuffers = receivedBuffers;
		}
		getBufferManager().releaseAllBuffers(releasedBuffers);
	}

	@VisibleForTesting
	protected int getNumberOfQueuedBuffers() {
		synchronized (receivedBuffers) {
			return receivedBuffers.size();
		}
	}

	abstract BufferManager getBufferManager();
}
