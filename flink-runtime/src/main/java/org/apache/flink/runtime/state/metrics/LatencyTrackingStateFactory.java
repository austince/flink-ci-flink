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

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;

/** Factory to create {@link AbstractLatencyTrackState}. */
public class LatencyTrackingStateFactory {

    @SuppressWarnings("unchecked")
    public static <K, N, S extends State, V, UK, UV, IN, SV, OUT>
            InternalKvState<K, N, ?> trackLatencyIfEnabled(
                    InternalKvState<K, N, ?> kvState,
                    StateDescriptor<S, V> stateDescriptor,
                    LatencyTrackingStateConfig latencyTrackingStateConfig) {
        if (latencyTrackingStateConfig.isEnabled()) {
            if (stateDescriptor instanceof ValueStateDescriptor) {
                return new LatencyTrackingValueState<>(
                        stateDescriptor.getName(),
                        (InternalValueState<K, N, V>) kvState,
                        latencyTrackingStateConfig);
            } else if (stateDescriptor instanceof ListStateDescriptor) {
                return new LatencyTrackingListState<>(
                        stateDescriptor.getName(),
                        (InternalListState<K, N, V>) kvState,
                        latencyTrackingStateConfig);
            } else if (stateDescriptor instanceof MapStateDescriptor) {
                return new LatencyTrackingMapState<>(
                        stateDescriptor.getName(),
                        (InternalMapState<K, N, UK, UV>) kvState,
                        latencyTrackingStateConfig);
            } else if (stateDescriptor instanceof ReducingStateDescriptor) {
                return new LatencyTrackingReducingState<>(
                        stateDescriptor.getName(),
                        (InternalReducingState<K, N, V>) kvState,
                        latencyTrackingStateConfig);
            } else if (stateDescriptor instanceof AggregatingStateDescriptor) {
                return new LatencyTrackingAggregatingState<>(
                        stateDescriptor.getName(),
                        (InternalAggregatingState<K, N, IN, SV, OUT>) kvState,
                        latencyTrackingStateConfig);
            }
        }
        return kvState;
    }
}
