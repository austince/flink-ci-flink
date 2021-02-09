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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.VirtualChannelSelector;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Demultiplexes buffers on subtask-level.
 *
 * <p>Example: If the current task has been downscaled from 2 to 1. Then the only new subtask needs
 * to handle data originating from old subtasks 0 and 1.
 *
 * <p>It is also responsible for summarizing watermark and stream statuses of the virtual channels.
 */
class DemultiplexingRecordDeserializer<T>
        implements RecordDeserializer<DeserializationDelegate<StreamElement>> {
    public static final DemultiplexingRecordDeserializer UNMAPPED =
            new DemultiplexingRecordDeserializer(Collections.emptyMap());
    private final Map<VirtualChannelSelector, VirtualChannel<T>> channels;

    private VirtualChannel<T> currentVirtualChannel;

    static class VirtualChannel<T> {
        final RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer;
        final Predicate<StreamRecord<T>> recordFilter;
        Watermark lastWatermark = Watermark.UNINITIALIZED;
        StreamStatus streamStatus = StreamStatus.ACTIVE;

        VirtualChannel(
                RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer,
                Predicate<StreamRecord<T>> recordFilter) {
            this.deserializer = deserializer;
            this.recordFilter = recordFilter;
        }
    }

    public DemultiplexingRecordDeserializer(
            Map<VirtualChannelSelector, VirtualChannel<T>> channels) {
        this.channels = checkNotNull(channels);
    }

    public void select(VirtualChannelSelector selector) {
        currentVirtualChannel = channels.get(selector);
        if (currentVirtualChannel == null) {
            throw new IllegalStateException(
                    "Cannot select " + selector + "; known channels are " + channels.keySet());
        }
    }

    public boolean hasMappings() {
        return !channels.isEmpty();
    }

    @VisibleForTesting
    Collection<VirtualChannelSelector> getVirtualChannelSelectors() {
        return channels.keySet();
    }

    @Override
    public void setNextBuffer(Buffer buffer) throws IOException {
        currentVirtualChannel.deserializer.setNextBuffer(buffer);
    }

    @Override
    public CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException {
        throw new IllegalStateException("Cannot checkpoint while recovering");
    }

    @Override
    public DeserializationResult getNextRecord(DeserializationDelegate<StreamElement> delegate)
            throws IOException {
        DeserializationResult result;
        do {
            result = currentVirtualChannel.deserializer.getNextRecord(delegate);

            if (result.isFullRecord()) {
                final StreamElement element = delegate.getInstance();
                // test if record belongs to this subtask if it comes from ambiguous channel
                if (element.isRecord()
                        && currentVirtualChannel.recordFilter.test(element.asRecord())) {
                    return result;
                } else if (element.isWatermark()) {
                    // basically, do not emit a watermark if not all virtual channel are past it
                    currentVirtualChannel.lastWatermark = element.asWatermark();
                    final Watermark minWatermark =
                            channels.values().stream()
                                    .map(virtualChannel -> virtualChannel.lastWatermark)
                                    .min(Comparator.comparing(Watermark::getTimestamp))
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "Should always have a watermark"));
                    // at least one virtual channel has no watermark, don't emit any watermark yet
                    if (minWatermark.equals(Watermark.UNINITIALIZED)) {
                        continue;
                    }
                    delegate.setInstance(minWatermark);
                    return result;
                } else if (element.isStreamStatus()) {
                    currentVirtualChannel.streamStatus = element.asStreamStatus();
                    // summarize statuses across all virtual channels
                    // duplicate statuses are filtered in StatusWatermarkValve
                    if (channels.values().stream().anyMatch(d -> d.streamStatus.isActive())) {
                        delegate.setInstance(StreamStatus.ACTIVE);
                    }
                    return result;
                }
            }

            // loop is only re-executed for filtered full records and suppressed watermark
        } while (!result.isBufferConsumed());
        return DeserializationResult.PARTIAL_RECORD;
    }

    public void clear() {
        channels.values().forEach(d -> d.deserializer.clear());
    }

    static <T> DemultiplexingRecordDeserializer<T> create(
            InputChannelInfo channelInfo,
            InflightDataRescalingDescriptor rescalingDescriptor,
            Function<Integer, RecordDeserializer<DeserializationDelegate<StreamElement>>>
                    deserializerFactory,
            Function<InputChannelInfo, Predicate<StreamRecord<T>>> recordFilterFactory) {
        int[] oldSubtaskIndexes = rescalingDescriptor.getOldSubtaskIndexes();
        if (oldSubtaskIndexes.length == 0) {
            return UNMAPPED;
        }
        final int[] oldChannelIndexes =
                rescalingDescriptor
                        .getChannelMapping(channelInfo.getGateIdx())
                        .getOldChannelIndexes(channelInfo.getInputChannelIdx());
        if (oldChannelIndexes.length == 0) {
            return UNMAPPED;
        }
        int totalChannels = oldSubtaskIndexes.length * oldChannelIndexes.length;
        Map<VirtualChannelSelector, VirtualChannel<T>> virtualChannels =
                Maps.newHashMapWithExpectedSize(totalChannels);
        for (int subtask : oldSubtaskIndexes) {
            for (int channel : oldChannelIndexes) {
                VirtualChannelSelector selector = new VirtualChannelSelector(subtask, channel);
                virtualChannels.put(
                        selector,
                        new VirtualChannel<>(
                                deserializerFactory.apply(totalChannels),
                                rescalingDescriptor.isAmbiguous(subtask)
                                        ? recordFilterFactory.apply(channelInfo)
                                        : RecordFilter.all()));
            }
        }

        return new DemultiplexingRecordDeserializer(virtualChannels);
    }

    @Override
    public String toString() {
        return "DemultiplexingRecordDeserializer{" + "channels=" + channels.keySet() + '}';
    }
}
