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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalReadOnlyKeyContext;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.Collection;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Delegated partitioned {@link ListState} that forwards changes to {@link StateChange} upon {@link
 * ListState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
class ChangelogListState<K, N, V>
        extends AbstractChangelogState<K, N, List<V>, InternalListState<K, N, V>>
        implements InternalListState<K, N, V> {

    ChangelogListState(
            InternalListState<K, N, V> delegatedState,
            StateChangelogWriter<?> stateChangelogWriter,
            InternalReadOnlyKeyContext<K> keyContext) {
        super(delegatedState, stateChangelogWriter, keyContext);
    }

    @Override
    public void update(List<V> values) throws Exception {
        logStateUpdate(values);
        delegatedState.update(values);
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        logStateAdd(values);
        delegatedState.addAll(values);
    }

    @Override
    public void updateInternal(List<V> valueToStore) throws Exception {
        logStateUpdate(valueToStore);
        delegatedState.updateInternal(valueToStore);
    }

    @Override
    public void add(V value) throws Exception {
        if (getValueSerializer() instanceof ListSerializer) {
            logStateChange(
                    StateChangeOperation.ADD,
                    w ->
                            ((ListSerializer<V>) getValueSerializer())
                                    .getElementSerializer()
                                    .serialize(value, w));
        } else {
            logStateAdd(singletonList(value));
        }
        delegatedState.add(value);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        logStateMerge(target, sources);
        delegatedState.mergeNamespaces(target, sources);
    }

    @Override
    public List<V> getInternal() throws Exception {
        // NOTE: Both heap and rocks return copied state. But in RocksDB, changes to the returned
        // value will not get into the snapshot.
        return delegatedState.getInternal();
    }

    @Override
    public Iterable<V> get() throws Exception {
        // NOTE: Both heap and rocks return copied state. But in RocksDB, changes to the returned
        // value will not get into the snapshot.
        return delegatedState.get();
    }

    @Override
    public void clear() {
        logStateClear();
        delegatedState.clear();
    }
}
