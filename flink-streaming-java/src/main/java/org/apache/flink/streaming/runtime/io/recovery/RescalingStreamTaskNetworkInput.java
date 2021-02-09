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

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.VirtualChannelSelector;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.io.RecoverableStreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY;

/**
 * A {@link StreamTaskNetworkInput} implementation that demultiplexes virtual channels.
 *
 * <p>The demultiplexing works in two dimensions for the following cases. *
 *
 * <ul>
 *   <li>Subtasks of the current operator have been collapsed in a round-robin fashion.
 *   <li>The connected output operator has been rescaled (up and down!) and there is an overlap of
 *       channels (mostly relevant to keyed exchanges).
 * </ul>
 *
 * <p>In both cases, records from multiple old channels are received over one new physical channel,
 * which need to demultiplex the record to correctly restore spanning records (similar to how
 * StreamTaskNetworkInput works).
 *
 * <p>Note that when both cases occur at the same time (downscaling of several operators), there is
 * the cross product of channels. So if two subtasks are collapsed and two channels overlap from the
 * output side, there is a total of 4 virtual channels.
 */
@Internal
public final class RescalingStreamTaskNetworkInput<T>
        extends AbstractStreamTaskNetworkInput<T, DemultiplexingRecordDeserializer<T>>
        implements RecoverableStreamTaskInput<T> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RescalingStreamTaskNetworkInput.class);
    private final IOManager ioManager;

    private RescalingStreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            IOManager ioManager,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            InflightDataRescalingDescriptor inflightDataRescalingDescriptor,
            Function<Integer, StreamPartitioner<?>> gatePartitioners,
            TaskInfo taskInfo) {
        super(
                checkpointedInputGate,
                inputSerializer,
                statusWatermarkValve,
                inputIndex,
                getRecordDeserializers(
                        checkpointedInputGate,
                        inputSerializer,
                        ioManager,
                        inflightDataRescalingDescriptor,
                        gatePartitioners,
                        taskInfo));
        this.ioManager = ioManager;

        LOG.info(
                "Created demultiplexer for input {} from {}",
                inputIndex,
                inflightDataRescalingDescriptor);
    }

    private static <T>
            Map<InputChannelInfo, DemultiplexingRecordDeserializer<T>> getRecordDeserializers(
                    CheckpointedInputGate checkpointedInputGate,
                    TypeSerializer<T> inputSerializer,
                    IOManager ioManager,
                    InflightDataRescalingDescriptor rescalingDescriptor,
                    Function<Integer, StreamPartitioner<?>> gatePartitioners,
                    TaskInfo taskInfo) {

        RecordFilterFactory<T> recordFilterFactory =
                new RecordFilterFactory<>(
                        taskInfo.getIndexOfThisSubtask(),
                        inputSerializer,
                        taskInfo.getNumberOfParallelSubtasks(),
                        gatePartitioners,
                        taskInfo.getMaxNumberOfParallelSubtasks());
        final DeserializerFactory deserializerFactory = new DeserializerFactory(ioManager);
        Map<InputChannelInfo, DemultiplexingRecordDeserializer<T>> deserializers =
                Maps.newHashMapWithExpectedSize(checkpointedInputGate.getChannelInfos().size());
        for (InputChannelInfo channelInfo : checkpointedInputGate.getChannelInfos()) {
            deserializers.put(
                    channelInfo,
                    DemultiplexingRecordDeserializer.create(
                            channelInfo,
                            rescalingDescriptor,
                            deserializerFactory,
                            recordFilterFactory));
        }
        return deserializers;
    }

    @Override
    public StreamTaskInput<T> finishRecovery() throws IOException {
        close();
        return new StreamTaskNetworkInput<>(
                checkpointedInputGate,
                inputSerializer,
                ioManager,
                statusWatermarkValve,
                inputIndex);
    }

    /**
     * Factory method for {@link StreamTaskNetworkInput} or {@link RescalingStreamTaskNetworkInput}
     * depending on {@link InflightDataRescalingDescriptor}.
     */
    public static <T> StreamTaskInput<T> create(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            IOManager ioManager,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            InflightDataRescalingDescriptor rescalingDescriptorinflightDataRescalingDescriptor,
            Function<Integer, StreamPartitioner<?>> gatePartitioners,
            TaskInfo taskInfo) {
        return rescalingDescriptorinflightDataRescalingDescriptor.equals(
                        InflightDataRescalingDescriptor.NO_RESCALE)
                ? new StreamTaskNetworkInput<>(
                        checkpointedInputGate,
                        inputSerializer,
                        ioManager,
                        statusWatermarkValve,
                        inputIndex)
                : new RescalingStreamTaskNetworkInput<>(
                        checkpointedInputGate,
                        inputSerializer,
                        ioManager,
                        statusWatermarkValve,
                        inputIndex,
                        rescalingDescriptorinflightDataRescalingDescriptor,
                        gatePartitioners,
                        taskInfo);
    }

    protected DemultiplexingRecordDeserializer<T> getActiveSerializer(
            InputChannelInfo channelInfo) {
        final DemultiplexingRecordDeserializer<T> deserialier =
                recordDeserializers.get(channelInfo);
        if (!deserialier.hasMappings()) {
            throw new IllegalStateException(
                    "Channel " + channelInfo + " should not receive data during recovery.");
        }
        return deserialier;
    }

    protected InputStatus processEvent(BufferOrEvent bufferOrEvent) {
        // Event received
        final AbstractEvent event = bufferOrEvent.getEvent();
        if (event instanceof VirtualChannelSelector) {
            getActiveSerializer(bufferOrEvent.getChannelInfo())
                    .select((VirtualChannelSelector) event);
            return InputStatus.MORE_AVAILABLE;
        }
        return super.processEvent(bufferOrEvent);
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
    }

    static class RecordFilterFactory<T>
            implements Function<InputChannelInfo, Predicate<StreamRecord<T>>> {
        private final Map<Integer, StreamPartitioner<T>> partitionerCache = new HashMap<>(1);
        private final Function<Integer, StreamPartitioner<?>> gatePartitioners;
        private final TypeSerializer<T> inputSerializer;
        private final int numberOfChannels;
        private int subtaskIndex;
        private int maxParallelism;

        public RecordFilterFactory(
                int subtaskIndex,
                TypeSerializer<T> inputSerializer,
                int numberOfChannels,
                Function<Integer, StreamPartitioner<?>> gatePartitioners,
                int maxParallelism) {
            this.gatePartitioners = gatePartitioners;
            this.inputSerializer = inputSerializer;
            this.numberOfChannels = numberOfChannels;
            this.subtaskIndex = subtaskIndex;
            this.maxParallelism = maxParallelism;
        }

        @Override
        public Predicate<StreamRecord<T>> apply(InputChannelInfo channelInfo) {
            return new RecordFilter<>(
                    partitionerCache.computeIfAbsent(
                            channelInfo.getGateIdx(), this::createPartitioner),
                    inputSerializer,
                    subtaskIndex);
        }

        private StreamPartitioner<T> createPartitioner(Integer index) {
            StreamPartitioner<T> partitioner = (StreamPartitioner<T>) gatePartitioners.apply(index);
            partitioner.setup(numberOfChannels);
            if (partitioner instanceof ConfigurableStreamPartitioner) {
                ((ConfigurableStreamPartitioner) partitioner).configure(maxParallelism);
            }
            return partitioner;
        }
    }

    static class DeserializerFactory
            implements Function<
                    Integer, RecordDeserializer<DeserializationDelegate<StreamElement>>> {
        private final IOManager ioManager;

        public DeserializerFactory(IOManager ioManager) {
            this.ioManager = ioManager;
        }

        @Override
        public RecordDeserializer<DeserializationDelegate<StreamElement>> apply(
                Integer totalChannels) {
            return new SpillingAdaptiveSpanningRecordDeserializer<>(
                    ioManager.getSpillingDirectoriesPaths(),
                    SpillingAdaptiveSpanningRecordDeserializer.DEFAULT_THRESHOLD_FOR_SPILLING
                            / totalChannels,
                    SpillingAdaptiveSpanningRecordDeserializer.DEFAULT_FILE_BUFFER_SIZE
                            / totalChannels);
        }
    }
}
