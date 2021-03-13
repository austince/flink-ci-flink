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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalReadOnlyKeyContext;
import org.apache.flink.runtime.state.internal.InternalReducingState;

import java.util.Collection;

/**
 * Delegated partitioned {@link ReducingState} that forwards changes to {@link StateChange} upon
 * {@link ReducingState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
class ChangelogReducingState<K, N, V>
        extends AbstractChangelogState<K, N, V, InternalReducingState<K, N, V>>
        implements InternalReducingState<K, N, V> {

    ChangelogReducingState(
            InternalReducingState<K, N, V> delegatedState,
            StateChangelogWriter<?> stateChangelogWriter,
            InternalReadOnlyKeyContext<K> keyContext) {
        super(delegatedState, stateChangelogWriter, keyContext);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        logStateMerge(target, sources);
        delegatedState.mergeNamespaces(target, sources);
    }

    @Override
    public V getInternal() throws Exception {
        return delegatedState.getInternal();
    }

    @Override
    public void updateInternal(V valueToStore) throws Exception {
        logStateUpdate(valueToStore);
        delegatedState.updateInternal(valueToStore);
    }

    @Override
    public V get() throws Exception {
        return delegatedState.get();
    }

    @Override
    public void add(V value) throws Exception {
        delegatedState.add(value);
        logStateUpdate(delegatedState.get());
    }

    @Override
    public void clear() {
        logStateClear();
        delegatedState.clear();
    }
}
