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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState.VirtualChannelMapping;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.VirtualChannelSelector;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamTaskNetworkInput} implementation that demultiplexes virtual channels.
 *
 * <p>The demultiplexing works in two dimensions for the following cases.
 * <ul>
 *     <li> Subtasks of the current operator have been collapsed in a round-robin fashion.
 *     <li> The connected output operator has been rescaled (up and down!) and there is an overlap of channels (mostly
 * relevant to keyed exchanges).
 * </ul>
 * In both cases, records from multiple old channels are received over one new physical channel, which need to
 * demultiplex the record to correctly restore spanning records (similar to how StreamTaskNetworkInput works).
 *
 * <p>Note that when both cases occur at the same time (downscaling of several operators), there is the cross product of
 * channels. So if two subtasks are collapsed and two channels overlap from the output side, there is a total of 4
 * virtual channels.
 */
@Internal
public final class RescalingStreamTaskNetworkInput<T> implements RecoverableStreamTaskInput<T> {

	private final CheckpointedInputGate checkpointedInputGate;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final Demultiplexer[] channelDemultiplexers;

	/** Valve that controls how watermarks and stream statuses are forwarded. */
	private final StatusWatermarkValve statusWatermarkValve;

	private final int inputIndex;

	private final Map<InputChannelInfo, Integer> channelIndexes;

	private final TypeSerializer<?> inputSerializer;
	private final IOManager ioManager;

	@Nullable
	private Demultiplexer currentChannelDemultiplexer = null;
	private int lastChannel;

	private RescalingStreamTaskNetworkInput(
			CheckpointedInputGate checkpointedInputGate,
			TypeSerializer<?> inputSerializer,
			IOManager ioManager,
			StatusWatermarkValve statusWatermarkValve,
			int inputIndex,
			VirtualChannelMapping virtualChannelMapping,
			Function<Integer, StreamPartitioner<?>> inputPartitionerRetriever,
			int subtaskIndex) {
		this.checkpointedInputGate = checkpointedInputGate;
		this.inputSerializer = inputSerializer;
		this.ioManager = ioManager;
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(
			new StreamElementSerializer<>(inputSerializer));

		this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
		this.inputIndex = inputIndex;
		this.channelIndexes = getChannelIndexes(checkpointedInputGate);

		Map<Integer, StreamPartitioner<?>> partitionerCache = new HashMap<>();
		final DemultiplexParameters parameters = new DemultiplexParameters(
			inputSerializer,
			ioManager,
			virtualChannelMapping,
			gateIndex -> partitionerCache.computeIfAbsent(gateIndex, inputPartitionerRetriever),
			channelIndexes.size(),
			subtaskIndex);
		this.channelDemultiplexers = this.checkpointedInputGate.getChannelInfos().stream()
			.map(channelInfo -> SubtaskDemultiplexer.forChannel(channelInfo, parameters))
			.toArray(SubtaskDemultiplexer[]::new);
	}

	@Override
	public StreamTaskInput<T> finishRecovery() {
		return new StreamTaskNetworkInput<>(
			checkpointedInputGate,
			inputSerializer,
			ioManager,
			statusWatermarkValve,
			inputIndex);
	}

	private static Map<InputChannelInfo, Integer> getChannelIndexes(CheckpointedInputGate checkpointedInputGate) {
		int index = 0;
		List<InputChannelInfo> channelInfos = checkpointedInputGate.getChannelInfos();
		Map<InputChannelInfo, Integer> channelIndexes = new HashMap<>(channelInfos.size());
		for (InputChannelInfo channelInfo : channelInfos) {
			channelIndexes.put(channelInfo, index++);
		}
		return channelIndexes;
	}

	public static <T> StreamTaskInput<T> of(
			CheckpointedInputGate checkpointedInputGate,
			TypeSerializer<?> inputSerializer,
			IOManager ioManager,
			StatusWatermarkValve statusWatermarkValve,
			int inputIndex,
			VirtualChannelMapping virtualChannelMapping,
			Function<Integer, StreamPartitioner<?>> inputPartitionerRetriever,
			int subtaskIndex) {
		return virtualChannelMapping.equals(VirtualChannelMapping.NO_MAPPING) ?
			new StreamTaskNetworkInput<>(
				checkpointedInputGate,
				inputSerializer,
				ioManager,
				statusWatermarkValve,
				inputIndex) :
			new RescalingStreamTaskNetworkInput<>(
				checkpointedInputGate,
				inputSerializer,
				ioManager,
				statusWatermarkValve,
				inputIndex,
				virtualChannelMapping,
				inputPartitionerRetriever,
				subtaskIndex);
	}

	@Override
	public InputStatus emitNext(DataOutput<T> output) throws Exception {

		while (true) {
			// get the stream element from the deserializer
			if (currentChannelDemultiplexer != null) {
				DeserializationResult result = currentChannelDemultiplexer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentChannelDemultiplexer = null;
				}

				if (result.isFullRecord()) {
					processElement(deserializationDelegate.getInstance(), output);
					return InputStatus.MORE_AVAILABLE;
				}
			}

			Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
			if (bufferOrEvent.isPresent()) {
				// return to the mailbox after receiving a checkpoint barrier to avoid processing of
				// data after the barrier before checkpoint is performed for unaligned checkpoint mode
				if (bufferOrEvent.get().isBuffer()) {
					processBuffer(bufferOrEvent.get());
				} else {
					processEvent(bufferOrEvent.get());
					return InputStatus.MORE_AVAILABLE;
				}
			} else {
				if (checkpointedInputGate.isFinished()) {
					checkState(checkpointedInputGate.getAvailableFuture().isDone(), "Finished BarrierHandler should be available");
					return InputStatus.END_OF_INPUT;
				}
				return InputStatus.NOTHING_AVAILABLE;
			}
		}
	}

	private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
		if (recordOrMark.isRecord()){
			output.emitRecord(recordOrMark.asRecord());
		} else if (recordOrMark.isWatermark()) {
			statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), lastChannel, output);
		} else if (recordOrMark.isLatencyMarker()) {
			output.emitLatencyMarker(recordOrMark.asLatencyMarker());
		} else if (recordOrMark.isStreamStatus()) {
			statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), lastChannel, output);
		} else {
			throw new UnsupportedOperationException("Unknown type of StreamElement");
		}
	}

	private void processEvent(BufferOrEvent bufferOrEvent) {
		// Event received
		final AbstractEvent event = bufferOrEvent.getEvent();
		if (event instanceof VirtualChannelSelector) {
			int channel = channelIndexes.get(bufferOrEvent.getChannelInfo());
			checkState(channel != StreamTaskInput.UNSPECIFIED);
			this.channelDemultiplexers[channel].select((VirtualChannelSelector) event);
		} else if (event.getClass() == EndOfPartitionEvent.class) {
			// release the record deserializer immediately,
			// which is very valuable in case of bounded stream
			releaseDeserializer(channelIndexes.get(bufferOrEvent.getChannelInfo()));
		}
	}

	private void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
		lastChannel = channelIndexes.get(bufferOrEvent.getChannelInfo());
		checkState(lastChannel != StreamTaskInput.UNSPECIFIED);
		currentChannelDemultiplexer = this.channelDemultiplexers[lastChannel];
		checkState(
			currentChannelDemultiplexer != null,
			"currentRecordDeserializer has already been released");

		currentChannelDemultiplexer.setNextBuffer(bufferOrEvent.getBuffer());
	}

	@Override
	public int getInputIndex() {
		return inputIndex;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		if (currentChannelDemultiplexer != null) {
			return AVAILABLE;
		}
		return checkpointedInputGate.getAvailableFuture();
	}

	@Override
	public CompletableFuture<Void> prepareSnapshot(ChannelStateWriter channelStateWriter, long checkpointId) {
		throw new UnsupportedOperationException("Checkpointing during recovery is not supported (yet)");
	}

	@Override
	public void close() throws IOException {
		// release the deserializers . this part should not ever fail
		for (int channelIndex = 0; channelIndex < channelDemultiplexers.length; channelIndex++) {
			releaseDeserializer(channelIndex);
		}

		// cleanup the resources of the checkpointed input gate
		checkpointedInputGate.close();
	}

	private void releaseDeserializer(int channelIndex) {
		Demultiplexer demultiplexer = channelDemultiplexers[channelIndex];
		if (demultiplexer != null) {
			demultiplexer.close();

			channelDemultiplexers[channelIndex] = null;
		}
	}

	/**
	 * Parameter structure to pass all relevant information to the factory methods of @{@link Demultiplexer}.
	 */
	private static class DemultiplexParameters {
		final IOManager ioManager;
		final VirtualChannelMapping channelMapping;
		final Function<Integer, StreamPartitioner<?>> gatePartitionerRetriever;
		final SerializationDelegate<StreamRecord> delegate;
		final int numberOfChannels;
		final int subtaskIndex;

		@SuppressWarnings("unchecked")
		public DemultiplexParameters(
				TypeSerializer<?> inputSerializer,
				IOManager ioManager,
				VirtualChannelMapping channelMapping,
				Function<Integer, StreamPartitioner<?>> gatePartitionerRetriever,
				int numberOfChannels,
				int subtaskIndex) {
			delegate = new SerializationDelegate<>((TypeSerializer<StreamRecord>) inputSerializer);
			this.ioManager = ioManager;
			this.channelMapping = channelMapping;
			this.gatePartitionerRetriever = gatePartitionerRetriever;
			this.numberOfChannels = numberOfChannels;
			this.subtaskIndex = subtaskIndex;
		}
	}

	/**
	 * {@link RecordDeserializer}-like interface for recovery. To avoid additional virtual method calls on the
	 * non-recovery hotpath, this interface is not extending RecordDeserializer.
	 */
	private interface Demultiplexer extends AutoCloseable {
		DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> deserializationDelegate) throws IOException;

		void setNextBuffer(Buffer buffer) throws IOException;

		void select(VirtualChannelSelector event);

		@Override
		void close();
	}

	/**
	 * Demultiplexes buffers on subtask-level.
	 *
	 * <p>Example: If the current task has been downscaled from 2 to 1. Then the only new subtask needs to handle data
	 * originating from old subtasks 0 and 1. In this case, {@link #channelDemultiplexers} contains
	 * {@code 0->ChannelDemultiplexer0, 1->ChannelDemultiplexer1}.
	 *
	 * <p>Since this the outer demultiplexing layer, it is also responsible for summarizing watermark and stream
	 * statuses of the (nested) virtual channels.
	 */
	private static class SubtaskDemultiplexer implements Demultiplexer {
		private final Map<Integer, ChannelDemultiplexer> channelDemultiplexers;

		/** Keep track of the last emitted watermark for all (nested) virtual channels. */
		private final Map<VirtualChannelSelector, Watermark> lastWatermarks;

		/** Keep track of the last emitted stream status for all (nested) virtual channels. */
		private final Map<VirtualChannelSelector, StreamStatus> streamStatuses;

		private VirtualChannelSelector currentSelector;

		private ChannelDemultiplexer selectedChannelDemultiplexer;

		public SubtaskDemultiplexer(Map<Integer, ChannelDemultiplexer> channelDemultiplexers) {
			this.channelDemultiplexers = channelDemultiplexers;
			final Map.Entry<Integer, ChannelDemultiplexer> defaultSelection =
				Iterables.get(channelDemultiplexers.entrySet(), 0);
			selectedChannelDemultiplexer = defaultSelection.getValue();
			currentSelector = new VirtualChannelSelector(defaultSelection.getKey(),
				selectedChannelDemultiplexer.selectedIndex);

			// initialize watermarks and streamStatuses for all nested virtual channels
			int totalVirtualChannels =
				channelDemultiplexers.size() * selectedChannelDemultiplexer.recordDeserializers.size();
			this.lastWatermarks = new HashMap<>(totalVirtualChannels, 1f);
			this.streamStatuses = new HashMap<>(totalVirtualChannels, 1f);
			for (final Map.Entry<Integer, ChannelDemultiplexer> taskEntry : channelDemultiplexers.entrySet()) {
				for (final Integer channelIndex : taskEntry.getValue().recordDeserializers.keySet()) {
					VirtualChannelSelector selector = new VirtualChannelSelector(taskEntry.getKey(), channelIndex);
					lastWatermarks.put(selector, Watermark.UNINITIALIZED);
					streamStatuses.put(selector, StreamStatus.ACTIVE);
				}
			}
		}

		public void select(VirtualChannelSelector selector) {
			currentSelector = selector;
			selectedChannelDemultiplexer = channelDemultiplexers.get(selector.getTaskIndex());
			selectedChannelDemultiplexer.select(selector);
		}

		@Override
		public void setNextBuffer(Buffer buffer) throws IOException {
			selectedChannelDemultiplexer.setNextBuffer(buffer);
		}

		@Override
		public DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> deserializationDelegate) throws IOException {
			do {
				DeserializationResult result = selectedChannelDemultiplexer.getNextRecord(deserializationDelegate);

				// special handling of watermarks and stream status
				if (result.isFullRecord()) {
					final StreamElement element = deserializationDelegate.getInstance();
					if (element.isWatermark()) {
						// basically, do not emit a watermark if not all virtual channel are past it
						lastWatermarks.put(currentSelector, element.asWatermark());
						final Watermark minWatermark = lastWatermarks.values().stream()
							.min(Comparator.comparing(Watermark::getTimestamp))
							.orElseThrow(() -> new IllegalStateException("Should always have a min watermark"));
						// at least one virtual channel has no watermark, so don't emit any watermark yet
						if (minWatermark.equals(Watermark.UNINITIALIZED)) {
							continue;
						}
						deserializationDelegate.setInstance(minWatermark);
					} else if (element.isStreamStatus()) {
						streamStatuses.put(currentSelector, element.asStreamStatus());
						// summarize statuses across all virtual channels
						// duplicate statuses are filtered in StatusWatermarkValve
						if (streamStatuses.values().stream().anyMatch(s -> s.equals(StreamStatus.ACTIVE))) {
							deserializationDelegate.setInstance(StreamStatus.ACTIVE);
						}
					}
				}

				return result;
				// loop is only re-executed for suppressed watermark
			} while (true);
		}

		public void close() {
			channelDemultiplexers.values().forEach(Demultiplexer::close);
		}

		static SubtaskDemultiplexer forChannel(InputChannelInfo channelInfo, DemultiplexParameters parameters) {
			final int[] oldTaskInstances = parameters.channelMapping.getOldTaskInstances(parameters.subtaskIndex);
			Map<Integer, ChannelDemultiplexer> channelDemultiplexers = Arrays.stream(oldTaskInstances).boxed()
				.collect(Collectors.toMap(
					Function.identity(),
					unused -> ChannelDemultiplexer.forChannel(channelInfo, parameters)
				));
			return new SubtaskDemultiplexer(channelDemultiplexers);
		}
	}

	/**
	 * Demultiplexes buffers on channel-level.
	 *
	 * <p>Example: If the upstream task has been downscaled from 2 to 1. Then, old channels 0 and 1 are both
	 * processed over new channel 0. So this channel demultiplexer has two {@link #recordDeserializers} associated
	 * with the respective old channels.
	 *
	 * <p>For all non-unique mappings of new channels to old channels (see
	 * {@link org.apache.flink.runtime.io.network.api.writer.ChannelStateRescaler} for more details), a filter
	 * verifies if the restored record should be indeed processed by this subtask or if it should be filtered out and
	 * be processed at a different subtask.
	 */
	private static class ChannelDemultiplexer implements Demultiplexer {
		private final Map<Integer, RecordDeserializer<DeserializationDelegate<StreamElement>>> recordDeserializers;

		private static final Predicate<StreamRecord> NO_FILTER = record -> true;

		private final Map<Integer, Predicate<StreamRecord>> filters;

		@Nullable
		private RecordDeserializer<DeserializationDelegate<StreamElement>> selectedDeserializer;

		private int selectedIndex;

		ChannelDemultiplexer(
				Map<Integer, Predicate<StreamRecord>> oldChannelsWithFilters,
				DemultiplexParameters parameters,
				int numVirtualChannels) {
			this.filters = oldChannelsWithFilters;
			recordDeserializers = new HashMap<>(oldChannelsWithFilters.size(), 1f);
			for (final Integer oldChannel : oldChannelsWithFilters.keySet()) {
				recordDeserializers.put(oldChannel,
					new SpillingAdaptiveSpanningRecordDeserializer<>(parameters.ioManager.getSpillingDirectoriesPaths(),
						SpillingAdaptiveSpanningRecordDeserializer.DEFAULT_THRESHOLD_FOR_SPILLING / numVirtualChannels,
						SpillingAdaptiveSpanningRecordDeserializer.DEFAULT_FILE_BUFFER_SIZE / numVirtualChannels));
			}
		}

		@Override
		public DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> deserializationDelegate) throws IOException {
			do {
				final DeserializationResult result = selectedDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					selectedDeserializer.getCurrentBuffer().recycleBuffer();
				}
				if (result.isFullRecord()) {
					final StreamElement element = deserializationDelegate.getInstance();
					if (element.isRecord() && !filters.get(selectedIndex).test(element.asRecord())) {
						continue;
					}
				}

				return result;
				// loop is re-executed for filtered full records.
			} while (true);
		}

		public void select(VirtualChannelSelector selector) {
			selectedIndex = selector.getChannelIndex();
			selectedDeserializer = recordDeserializers.get(selectedIndex);
			if (selectedDeserializer == null) {
				throw new IllegalStateException(
					"Cannot select " + selector + "; known channels are " + recordDeserializers.keySet());
			}
		}

		@Override
		public void setNextBuffer(Buffer buffer) throws IOException {
			selectedDeserializer.setNextBuffer(buffer);
		}

		public void close() {
			for (RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer :
				recordDeserializers.values()) {
				// recycle buffers and clear the deserializer.
				Buffer buffer = deserializer.getCurrentBuffer();
				if (buffer != null && !buffer.isRecycled()) {
					buffer.recycleBuffer();
				}
				deserializer.clear();
			}
		}

		static ChannelDemultiplexer forChannel(InputChannelInfo channelInfo, DemultiplexParameters parameters) {
			final VirtualChannelMapping mapping = parameters.channelMapping;
			final int numTasks = mapping.getOldTaskInstances(parameters.subtaskIndex).length;
			final OperatorSubtaskState.PartitionMapping partitionMapping =
				mapping.getPartitionMapping(channelInfo.getGateIdx());
			final int[] oldChannels = partitionMapping.getOldChannelIndexes(channelInfo.getInputChannelIdx());

			final Map<Integer, Predicate<StreamRecord>> oldChannelsWithFilters =
				Arrays.stream(oldChannels).boxed()
					.collect(Collectors.toMap(
						Function.identity(),
						oldChannel -> getFilterForChannel(channelInfo, parameters, partitionMapping, oldChannel)));

			return new ChannelDemultiplexer(oldChannelsWithFilters, parameters, numTasks * oldChannels.length);
		}

		private static Predicate<StreamRecord> getFilterForChannel(
				InputChannelInfo channelInfo,
				DemultiplexParameters parameters,
				OperatorSubtaskState.PartitionMapping partitionMapping,
				Integer oldChannel) {
			return partitionMapping.getNewChannelIndexes(oldChannel).length <= 1 ?
				NO_FILTER :
				createFilter(channelInfo, parameters);
		}

		private static Predicate<StreamRecord> createFilter(
				InputChannelInfo channelInfo,
				DemultiplexParameters parameters) {
			final StreamPartitioner partitioner = parameters.gatePartitionerRetriever.apply(channelInfo.getGateIdx());
			final int inputChannelIdx = channelInfo.getInputChannelIdx();
			final SerializationDelegate<StreamRecord> delegate = parameters.delegate;
			partitioner.setup(parameters.numberOfChannels);
			if (partitioner instanceof ConfigurableStreamPartitioner) {
				((ConfigurableStreamPartitioner) partitioner).configure(KeyGroupRangeAssignment.UPPER_BOUND_MAX_PARALLELISM);
			}
			return streamRecord -> {
				delegate.setInstance(streamRecord);
				return partitioner.selectChannel(delegate) == inputChannelIdx;
			};
		}
	}
}
