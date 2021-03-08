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

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;

import java.util.Collection;

/**
 * This class wraps aggregating state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> Type of the value added to the state
 * @param <ACC> The type of the accumulator (intermediate aggregate state).
 * @param <OUT> Type of the value extracted from the state
 */
public class LatencyTrackingAggregatingState<K, N, IN, ACC, OUT>
        extends AbstractLatencyTrackState<
                K,
                N,
                ACC,
                InternalAggregatingState<K, N, IN, ACC, OUT>,
                LatencyTrackingAggregatingState.LatencyTrackingAggregatingStateMetrics>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    LatencyTrackingAggregatingState(
            String stateName,
            InternalAggregatingState<K, N, IN, ACC, OUT> original,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        super(
                original,
                new LatencyTrackingAggregatingStateMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getSlidingWindow()));
    }

    @Override
    public OUT get() throws Exception {
        if (latencyTrackingStateMetric.checkGetCounter()) {
            return trackLatencyWithException(
                    () -> original.get(), latencyTrackingStateMetric::updateGetLatency);
        } else {
            return original.get();
        }
    }

    @Override
    public void add(IN value) throws Exception {
        if (latencyTrackingStateMetric.checkAddCounter()) {
            trackLatencyWithException(
                    () -> original.add(value), latencyTrackingStateMetric::updateAddLatency);
        } else {
            original.add(value);
        }
    }

    @Override
    public ACC getInternal() throws Exception {
        return original.getInternal();
    }

    @Override
    public void updateInternal(ACC valueToStore) throws Exception {
        original.updateInternal(valueToStore);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (latencyTrackingStateMetric.checkMergeNamespacesCounter()) {
            trackLatencyWithException(
                    () -> original.mergeNamespaces(target, sources),
                    latencyTrackingStateMetric::updateMergeNamespacesLatency);
        } else {
            original.mergeNamespaces(target, sources);
        }
    }

    protected static class LatencyTrackingAggregatingStateMetrics
            extends AbstractLatencyTrackingStateMetric {
        static final String AGGREGATING_STATE_GET_LATENCY = "aggregatingStateGetLatency";
        static final String AGGREGATING_STATE_ADD_LATENCY = "aggregatingStateAddLatency";
        static final String AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY =
                "aggregatingStateMergeNamespacesLatency";

        LatencyTrackingAggregatingStateMetrics(
                String stateName, MetricGroup metricGroup, int sampleInterval, long slidingWindow) {
            super(stateName, metricGroup, sampleInterval, slidingWindow);
        }

        boolean checkGetCounter() {
            return checkCounter(AGGREGATING_STATE_GET_LATENCY);
        }

        boolean checkAddCounter() {
            return checkCounter(AGGREGATING_STATE_ADD_LATENCY);
        }

        boolean checkMergeNamespacesCounter() {
            return checkCounter(AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY);
        }

        void updateGetLatency(long duration) {
            updateHistogram(AGGREGATING_STATE_GET_LATENCY, duration);
        }

        void updateAddLatency(long duration) {
            updateHistogram(AGGREGATING_STATE_ADD_LATENCY, duration);
        }

        void updateMergeNamespacesLatency(long duration) {
            updateHistogram(AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY, duration);
        }
    }
}
