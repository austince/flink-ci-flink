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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalReadOnlyKeyContext;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.util.function.BiConsumerWithException;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Delegated partitioned {@link MapState} that forwards changes to {@link StateChange} upon {@link
 * MapState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
class ChangelogMapState<K, N, UK, UV>
        extends AbstractChangelogState<K, N, Map<UK, UV>, InternalMapState<K, N, UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    ChangelogMapState(
            InternalMapState<K, N, UK, UV> delegatedState,
            StateChangelogWriter<?> stateChangelogWriter,
            InternalReadOnlyKeyContext<K> keyContext) {
        super(delegatedState, stateChangelogWriter, keyContext);
    }

    @Override
    public UV get(UK key) throws Exception {
        return delegatedState.get(key);
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        if (getValueSerializer() instanceof MapSerializer) {
            logStateChange(
                    StateChangeOperation.ADD,
                    wrapper -> {
                        serializeKey(key, wrapper);
                        serializeValue(value, wrapper);
                    });
        } else {
            logStateAdd(Collections.singletonMap(key, value));
        }
        delegatedState.put(key, value);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        logStateAdd(map);
        delegatedState.putAll(map);
    }

    @Override
    public void remove(UK key) throws Exception {
        logStateChange(StateChangeOperation.REMOVE, w -> serializeKey(key, w));
        delegatedState.remove(key);
    }

    @Override
    public boolean contains(UK key) throws Exception {
        return delegatedState.contains(key);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        return loggingIterable(delegatedState.entries(), REMOVE_LISTENER);
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        return loggingIterable(delegatedState.keys(), this::serializeKey);
    }

    @Override
    public Iterable<UV> values() throws Exception {
        Iterator<Map.Entry<UK, UV>> iterator = entries().iterator();
        return () ->
                new Iterator<UV>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public UV next() {
                        return iterator.next().getValue();
                    }
                };
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return loggingIterator(delegatedState.iterator(), REMOVE_LISTENER);
    }

    @Override
    public boolean isEmpty() throws Exception {
        return delegatedState.isEmpty();
    }

    @Override
    public void clear() {
        logStateClear();
        delegatedState.clear();
    }

    private void serializeValue(
            UV value, org.apache.flink.core.memory.DataOutputViewStreamWrapper wrapper)
            throws IOException {
        getMapSerializer().getValueSerializer().serialize(value, wrapper);
    }

    private void serializeKey(
            UK key, org.apache.flink.core.memory.DataOutputViewStreamWrapper wrapper)
            throws IOException {
        getMapSerializer().getKeySerializer().serialize(key, wrapper);
    }

    private MapSerializer<UK, UV> getMapSerializer() {
        return (MapSerializer<UK, UV>) getValueSerializer();
    }

    private final BiConsumerWithException<
                    Map.Entry<UK, UV>, DataOutputViewStreamWrapper, IOException>
            REMOVE_LISTENER = (entry, wrapper) -> serializeKey(entry.getKey(), wrapper);
}
