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

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_CONTAINS_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_ENTRIES_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_GET_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_IS_EMPTY_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_ITERATOR_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_KEYS_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_PUT_ALL_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_PUT_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_REMOVE_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingMapState.LatencyTrackingMapStateMetrics.MAP_STATE_VALUES_LATENCY;
import static org.hamcrest.core.Is.is;

/** Tests for {@link LatencyTrackingMapState}. */
public class LatencyTrackingMapStateTest extends LatencyTrackingStateTestBase<Integer> {
    @Override
    @SuppressWarnings("unchecked")
    MapStateDescriptor<Integer, Double> getStateDescriptor() {
        return new MapStateDescriptor<>("map", Integer.class, Double.class);
    }

    @Override
    TypeSerializer<Integer> getKeySerializer() {
        return IntSerializer.INSTANCE;
    }

    @Override
    void setCurrentKey(AbstractKeyedStateBackend<Integer> keyedBackend) {
        keyedBackend.setCurrentKey(1);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testLatencyTrackingMapState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingMapState<Integer, VoidNamespace, Long, Double> latencyTrackingState =
                    (LatencyTrackingMapState)
                            createLatencyTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            AbstractLatencyTrackingStateMetric latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();
            Map<String, AbstractLatencyTrackingStateMetric.Counter> countersPerMetric =
                    latencyTrackingStateMetric.getCountersPerMetric();
            Assert.assertThat(countersPerMetric.isEmpty(), is(true));
            setCurrentKey(keyedBackend);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.put(random.nextLong(), random.nextDouble());
                Assert.assertEquals(
                        expectedResult, countersPerMetric.get(MAP_STATE_PUT_LATENCY).getCounter());
                latencyTrackingState.putAll(
                        Collections.singletonMap(random.nextLong(), random.nextDouble()));
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(MAP_STATE_PUT_ALL_LATENCY).getCounter());
                latencyTrackingState.get(random.nextLong());
                Assert.assertEquals(
                        expectedResult, countersPerMetric.get(MAP_STATE_GET_LATENCY).getCounter());
                latencyTrackingState.remove(random.nextLong());
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(MAP_STATE_REMOVE_LATENCY).getCounter());
                latencyTrackingState.contains(random.nextLong());
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(MAP_STATE_CONTAINS_LATENCY).getCounter());
                latencyTrackingState.isEmpty();
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(MAP_STATE_IS_EMPTY_LATENCY).getCounter());
                latencyTrackingState.entries();
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(MAP_STATE_ENTRIES_LATENCY).getCounter());
                latencyTrackingState.keys();
                Assert.assertEquals(
                        expectedResult, countersPerMetric.get(MAP_STATE_KEYS_LATENCY).getCounter());
                latencyTrackingState.values();
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(MAP_STATE_VALUES_LATENCY).getCounter());
                latencyTrackingState.iterator();
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(MAP_STATE_ITERATOR_LATENCY).getCounter());
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }
}
