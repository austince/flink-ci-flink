/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.internal.InternalMapState;

import java.util.Iterator;
import java.util.Map;

/**
 * This class wraps map state with latency tracking logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <UK> Type of the user entry key of state
 * @param <UV> Type of the user entry value of state
 */
public class LatencyTrackingMapState<K, N, UK, UV>
        extends AbstractLatencyTrackState<
                K,
                N,
                Map<UK, UV>,
                InternalMapState<K, N, UK, UV>,
                LatencyTrackingMapState.LatencyTrackingMapStateMetrics>
        implements InternalMapState<K, N, UK, UV> {
    LatencyTrackingMapState(
            String stateName,
            InternalMapState<K, N, UK, UV> original,
            LatencyTrackingStateConfig latencyTrackingStateConfig) {
        super(
                original,
                new LatencyTrackingMapStateMetrics(
                        stateName,
                        latencyTrackingStateConfig.getMetricGroup(),
                        latencyTrackingStateConfig.getSampleInterval(),
                        latencyTrackingStateConfig.getSlidingWindow()));
    }

    @Override
    public UV get(UK key) throws Exception {
        if (latencyTrackingStateMetric.checkGetCounter()) {
            return trackLatencyWithException(
                    () -> original.get(key), latencyTrackingStateMetric::updateGetLatency);
        } else {
            return original.get(key);
        }
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        if (latencyTrackingStateMetric.checkPutCounter()) {
            trackLatencyWithException(
                    () -> original.put(key, value), latencyTrackingStateMetric::updatePutLatency);
        } else {
            original.put(key, value);
        }
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        if (latencyTrackingStateMetric.checkPuAllCounter()) {
            trackLatencyWithException(
                    () -> original.putAll(map), latencyTrackingStateMetric::updatePutAllLatency);
        } else {
            original.putAll(map);
        }
    }

    @Override
    public void remove(UK key) throws Exception {
        if (latencyTrackingStateMetric.checkRemoveCounter()) {
            trackLatencyWithException(
                    () -> original.remove(key), latencyTrackingStateMetric::updateRemoveLatency);
        } else {
            original.remove(key);
        }
    }

    @Override
    public boolean contains(UK key) throws Exception {
        if (latencyTrackingStateMetric.checkContainsCounter()) {
            return trackLatencyWithException(
                    () -> original.contains(key),
                    latencyTrackingStateMetric::updateContainsLatency);
        } else {
            return original.contains(key);
        }
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        if (latencyTrackingStateMetric.checkEntriesCounter()) {
            return trackLatencyWithException(
                    () -> original.entries(), latencyTrackingStateMetric::updateEntriesLatency);
        } else {
            return original.entries();
        }
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        if (latencyTrackingStateMetric.checkKeysCounter()) {
            return trackLatencyWithException(
                    () -> original.keys(), latencyTrackingStateMetric::updateKeysLatency);
        } else {
            return original.keys();
        }
    }

    @Override
    public Iterable<UV> values() throws Exception {
        if (latencyTrackingStateMetric.checkValuesCounter()) {
            return trackLatencyWithException(
                    () -> original.values(), latencyTrackingStateMetric::updateValuesLatency);
        } else {
            return original.values();
        }
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        if (latencyTrackingStateMetric.checkIteratorCounter()) {
            return trackLatencyWithException(
                    () -> original.iterator(), latencyTrackingStateMetric::updateIteratorLatency);
        } else {
            return original.iterator();
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        if (latencyTrackingStateMetric.checkIsEmptyCounter()) {
            return trackLatencyWithException(
                    () -> original.isEmpty(), latencyTrackingStateMetric::updateIsEmptyLatency);
        } else {
            return original.isEmpty();
        }
    }

    protected static class LatencyTrackingMapStateMetrics
            extends AbstractLatencyTrackingStateMetric {
        static final String MAP_STATE_GET_LATENCY = "mapStateGetLatency";
        static final String MAP_STATE_PUT_LATENCY = "mapStatePutLatency";
        static final String MAP_STATE_PUT_ALL_LATENCY = "mapStatePutAllLatency";
        static final String MAP_STATE_REMOVE_LATENCY = "mapStateRemoveLatency";
        static final String MAP_STATE_CONTAINS_LATENCY = "mapStateContainsAllLatency";
        static final String MAP_STATE_ENTRIES_LATENCY = "mapStateEntriesLatency";
        static final String MAP_STATE_KEYS_LATENCY = "mapStateKeysLatency";
        static final String MAP_STATE_VALUES_LATENCY = "mapStateValuesLatency";
        static final String MAP_STATE_ITERATOR_LATENCY = "mapStateIteratorLatency";
        static final String MAP_STATE_IS_EMPTY_LATENCY = "mapStateIsEmptyLatency";

        LatencyTrackingMapStateMetrics(
                String stateName, MetricGroup metricGroup, int sampleInterval, long slidingWindow) {
            super(stateName, metricGroup, sampleInterval, slidingWindow);
        }

        boolean checkGetCounter() {
            return checkCounter(MAP_STATE_GET_LATENCY);
        }

        boolean checkPutCounter() {
            return checkCounter(MAP_STATE_PUT_LATENCY);
        }

        boolean checkPuAllCounter() {
            return checkCounter(MAP_STATE_PUT_ALL_LATENCY);
        }

        boolean checkRemoveCounter() {
            return checkCounter(MAP_STATE_REMOVE_LATENCY);
        }

        boolean checkContainsCounter() {
            return checkCounter(MAP_STATE_CONTAINS_LATENCY);
        }

        boolean checkEntriesCounter() {
            return checkCounter(MAP_STATE_ENTRIES_LATENCY);
        }

        boolean checkKeysCounter() {
            return checkCounter(MAP_STATE_KEYS_LATENCY);
        }

        boolean checkValuesCounter() {
            return checkCounter(MAP_STATE_VALUES_LATENCY);
        }

        boolean checkIteratorCounter() {
            return checkCounter(MAP_STATE_ITERATOR_LATENCY);
        }

        boolean checkIsEmptyCounter() {
            return checkCounter(MAP_STATE_IS_EMPTY_LATENCY);
        }

        void updateGetLatency(long duration) {
            updateHistogram(MAP_STATE_GET_LATENCY, duration);
        }

        void updatePutLatency(long duration) {
            updateHistogram(MAP_STATE_PUT_LATENCY, duration);
        }

        void updatePutAllLatency(long duration) {
            updateHistogram(MAP_STATE_PUT_ALL_LATENCY, duration);
        }

        void updateRemoveLatency(long duration) {
            updateHistogram(MAP_STATE_REMOVE_LATENCY, duration);
        }

        void updateContainsLatency(long duration) {
            updateHistogram(MAP_STATE_CONTAINS_LATENCY, duration);
        }

        void updateEntriesLatency(long duration) {
            updateHistogram(MAP_STATE_ENTRIES_LATENCY, duration);
        }

        void updateKeysLatency(long duration) {
            updateHistogram(MAP_STATE_KEYS_LATENCY, duration);
        }

        void updateValuesLatency(long duration) {
            updateHistogram(MAP_STATE_VALUES_LATENCY, duration);
        }

        void updateIteratorLatency(long duration) {
            updateHistogram(MAP_STATE_ITERATOR_LATENCY, duration);
        }

        void updateIsEmptyLatency(long duration) {
            updateHistogram(MAP_STATE_IS_EMPTY_LATENCY, duration);
        }
    }
}
