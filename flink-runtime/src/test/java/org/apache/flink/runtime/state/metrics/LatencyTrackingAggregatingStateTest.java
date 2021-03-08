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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.runtime.state.metrics.LatencyTrackingAggregatingState.LatencyTrackingAggregatingStateMetrics.AGGREGATING_STATE_ADD_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingAggregatingState.LatencyTrackingAggregatingStateMetrics.AGGREGATING_STATE_GET_LATENCY;
import static org.apache.flink.runtime.state.metrics.LatencyTrackingAggregatingState.LatencyTrackingAggregatingStateMetrics.AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY;
import static org.hamcrest.core.Is.is;

/** Tests for {@link LatencyTrackingAggregatingState}. */
public class LatencyTrackingAggregatingStateTest extends LatencyTrackingStateTestBase<Integer> {
    @Override
    @SuppressWarnings("unchecked")
    AggregatingStateDescriptor<Long, Long, Long> getStateDescriptor() {
        return new AggregatingStateDescriptor<>(
                "aggregate",
                new AggregateFunction<Long, Long, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Long value, Long accumulator) {
                        return value + accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                },
                Long.class);
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
    public void testLatencyTrackingAggregatingState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            LatencyTrackingAggregatingState<Integer, VoidNamespace, Long, Long, Long>
                    latencyTrackingState =
                            (LatencyTrackingAggregatingState)
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
                latencyTrackingState.add(random.nextLong());
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(AGGREGATING_STATE_ADD_LATENCY).getCounter());
                latencyTrackingState.get();
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric.get(AGGREGATING_STATE_GET_LATENCY).getCounter());
                latencyTrackingState.mergeNamespaces(
                        VoidNamespace.INSTANCE, Collections.emptyList());
                Assert.assertEquals(
                        expectedResult,
                        countersPerMetric
                                .get(AGGREGATING_STATE_MERGE_NAMESPACES_LATENCY)
                                .getCounter());
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }
}
