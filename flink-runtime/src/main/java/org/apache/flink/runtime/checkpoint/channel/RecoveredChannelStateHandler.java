/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState.PartitionMapping;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState.VirtualChannelMapping;
import org.apache.flink.runtime.io.network.api.VirtualChannelSelector;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultSubpartition;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;

interface RecoveredChannelStateHandler<Info, Context> extends AutoCloseable {
	class BufferWithContext<Context> {
		final ChannelStateByteBuffer buffer;
		final Context context;

		BufferWithContext(ChannelStateByteBuffer buffer, Context context) {
			this.buffer = buffer;
			this.context = context;
		}
	}

	BufferWithContext<Context> getBuffer(Info info) throws IOException, InterruptedException;

	void recover(Info info, int oldSubtaskIndex, Context context) throws IOException;
}

class InputChannelRecoveredStateHandler implements RecoveredChannelStateHandler<InputChannelInfo, Buffer> {
	private final InputGate[] inputGates;

	private final VirtualChannelMapping channelMapping;

	private final Map<InputChannelInfo, List<RecoveredInputChannel>> rescaledChannels = new HashMap<>();

	InputChannelRecoveredStateHandler(IndexedInputGate[] inputGates, VirtualChannelMapping channelMapping) {
		this.inputGates = inputGates;
		this.channelMapping = channelMapping;
	}

	@Override
	public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo) throws IOException, InterruptedException {
		RecoveredInputChannel channel = getMappedChannels(channelInfo).get(0);
		Buffer buffer = channel.requestBufferBlocking();
		return new BufferWithContext<>(wrap(buffer), buffer);
	}

	@Override
	public void recover(InputChannelInfo channelInfo, int oldSubtaskIndex, Buffer buffer) throws IOException {
		if (buffer.readableBytes() > 0) {
			for (final RecoveredInputChannel channel : getMappedChannels(channelInfo)) {
				channel.onRecoveredStateBuffer(EventSerializer.toBuffer(new VirtualChannelSelector(
					oldSubtaskIndex,
					channelInfo.getInputChannelIdx()), false));
				channel.onRecoveredStateBuffer(buffer.retainBuffer());
			}
		}
		buffer.recycleBuffer();
	}

	@Override
	public void close() throws IOException {
		// note that we need to finish all RecoveredInputChannels, not just those with state
		for (final InputGate inputGate : inputGates) {
			inputGate.finishReadRecoveredState();
		}
	}

	private RecoveredInputChannel getChannel(int gateIndex, int subPartitionIndex) {
		final InputChannel inputChannel = inputGates[gateIndex].getChannel(subPartitionIndex);
		if (!(inputChannel instanceof RecoveredInputChannel)) {
			throw new IllegalStateException("Cannot restore state to a non-recovered input channel: " + inputChannel);
		}
		return (RecoveredInputChannel) inputChannel;
	}

	private List<RecoveredInputChannel> getMappedChannels(InputChannelInfo channelInfo) {
		return rescaledChannels.computeIfAbsent(channelInfo, this::calculateMapping);
	}

	private List<RecoveredInputChannel> calculateMapping(InputChannelInfo info) {
		final PartitionMapping partitionMapping = channelMapping.getPartitionMapping(info.getGateIdx());
		return Arrays.stream(partitionMapping.getNewChannelIndexes(info.getInputChannelIdx()))
			.mapToObj(newChannelIndex -> getChannel(info.getGateIdx(), newChannelIndex))
			.collect(Collectors.toList());
	}
}

class ResultSubpartitionRecoveredStateHandler implements
		RecoveredChannelStateHandler<ResultSubpartitionInfo, Tuple2<BufferBuilder, BufferConsumer>> {

	private final ResultPartitionWriter[] writers;

	private final VirtualChannelMapping channelMapping;

	private final Map<ResultSubpartitionInfo, List<CheckpointedResultSubpartition>> rescaledChannels = new HashMap<>();

	ResultSubpartitionRecoveredStateHandler(ResultPartitionWriter[] writers, VirtualChannelMapping channelMapping) {
		this.writers = writers;
		this.channelMapping = channelMapping;
	}

	@Override
	public BufferWithContext<Tuple2<BufferBuilder, BufferConsumer>> getBuffer(ResultSubpartitionInfo subpartitionInfo) throws IOException, InterruptedException {
		final List<CheckpointedResultSubpartition> channels = getMappedChannels(subpartitionInfo);
		BufferBuilder bufferBuilder = channels.get(0).requestBufferBuilderBlocking();
		return new BufferWithContext<>(wrap(bufferBuilder), Tuple2.of(bufferBuilder, bufferBuilder.createBufferConsumer()));
	}

	@Override
	public void recover(
			ResultSubpartitionInfo subpartitionInfo,
			int oldSubtaskIndex,
			Tuple2<BufferBuilder, BufferConsumer> bufferBuilderAndConsumer) throws IOException {
		bufferBuilderAndConsumer.f0.finish();
		if (bufferBuilderAndConsumer.f1.isDataAvailable()) {
			final List<CheckpointedResultSubpartition> channels = getMappedChannels(subpartitionInfo);
			for (final CheckpointedResultSubpartition channel : channels) {
				channel.add(EventSerializer.toBufferConsumer(new VirtualChannelSelector(
					oldSubtaskIndex,
					subpartitionInfo.getSubPartitionIdx()), false), Integer.MIN_VALUE);
				boolean added = channel.add(bufferBuilderAndConsumer.f1.copy(), Integer.MIN_VALUE);
				if (!added) {
					throw new IOException("Buffer consumer couldn't be added to ResultSubpartition");
				}
			}
		}
		bufferBuilderAndConsumer.f1.close();
	}

	private CheckpointedResultSubpartition getSubpartition(int partitionIndex, int subPartitionIdx) {
		ResultPartitionWriter writer = writers[partitionIndex];
		if (!(writer instanceof CheckpointedResultPartition)) {
			throw new IllegalStateException("Cannot restore state to a non-checkpointable partition type: " + writer);
		}
		return ((CheckpointedResultPartition) writer).getCheckpointedSubpartition(subPartitionIdx);
	}

	private List<CheckpointedResultSubpartition> getMappedChannels(ResultSubpartitionInfo subpartitionInfo) {
		return rescaledChannels.computeIfAbsent(subpartitionInfo, this::calculateMapping);
	}

	private List<CheckpointedResultSubpartition> calculateMapping(ResultSubpartitionInfo info) {
		final PartitionMapping partitionMapping = channelMapping.getPartitionMapping(info.getPartitionIdx());
		return Arrays.stream(partitionMapping.getNewChannelIndexes(info.getSubPartitionIdx()))
			.mapToObj(newIndexes -> getSubpartition(info.getPartitionIdx(), newIndexes))
			.collect(Collectors.toList());
	}

	@Override
	public void close() {
	}
}
