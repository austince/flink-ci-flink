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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalReadOnlyKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Base class for changelog state wrappers of state objects.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <V> The type of values kept internally in state without changelog wrapper
 * @param <S> Type of originally wrapped state object
 */
abstract class AbstractChangelogState<K, N, V, S extends InternalKvState<K, N, V>>
        implements InternalKvState<K, N, V> {
    protected final S delegatedState;
    protected final StateChangelogWriter<?> stateChangelogWriter;
    private final InternalReadOnlyKeyContext<K> keyContext;
    private N currentNamespace;

    AbstractChangelogState(
            S state,
            StateChangelogWriter<?> stateChangelogWriter,
            InternalReadOnlyKeyContext<K> keyContext) {
        // todo: check that state is not a changelog state
        this.delegatedState = state;
        this.stateChangelogWriter = stateChangelogWriter;
        this.keyContext = keyContext;
    }

    public S getDelegatedState() {
        return delegatedState;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return delegatedState.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return delegatedState.getNamespaceSerializer();
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return delegatedState.getValueSerializer();
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        currentNamespace = namespace;
        delegatedState.setCurrentNamespace(namespace);
    }

    @Override
    public byte[] getSerializedValue(
            byte[] serializedKeyAndNamespace,
            TypeSerializer<K> safeKeySerializer,
            TypeSerializer<N> safeNamespaceSerializer,
            TypeSerializer<V> safeValueSerializer)
            throws Exception {
        return delegatedState.getSerializedValue(
                serializedKeyAndNamespace,
                safeKeySerializer,
                safeNamespaceSerializer,
                safeValueSerializer);
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(
            int recommendedMaxNumberOfReturnedRecords) {
        return delegatedState.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
    }

    protected void logStateUpdate(V newState) throws IOException {
        if (newState == null) {
            logStateClear();
        } else {
            logStateChange(
                    StateChangeOperation.SET, w -> getValueSerializer().serialize(newState, w));
        }
    }

    protected void logStateAdd(V addedState) throws IOException {
        logStateChange(
                StateChangeOperation.ADD_ALL, w -> getValueSerializer().serialize(addedState, w));
    }

    protected void logStateMerge(N target, Collection<N> sources) throws java.io.IOException {
        // do not simply store the new value for the target namespace
        // because old namespaces need to be removed
        logStateChange(
                StateChangeOperation.MERGE,
                w -> {
                    getNamespaceSerializer().serialize(target, w);
                    w.writeInt(sources.size());
                    for (N n : sources) {
                        getNamespaceSerializer().serialize(n, w);
                    }
                });
    }

    protected void logStateClear() {
        try {
            logStateChange(StateChangeOperation.CLEAR, w -> {});
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    protected void logStateChange(
            StateChangeOperation op,
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer)
            throws IOException {
        logStateChange(serialize(op.code, dataSerializer));
    }

    private void logStateChange(byte[] bytes) {
        stateChangelogWriter.append(keyContext.getCurrentKeyGroupIndex(), bytes);
    }

    private byte[] serialize(
            byte opCode, ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataWriter)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(out)) {
            getKeySerializer().serialize(keyContext.getCurrentKey(), wrapper);
            getNamespaceSerializer().serialize(currentNamespace, wrapper);
            wrapper.writeByte(opCode);
            dataWriter.accept(wrapper);
            return out.toByteArray();
        }
    }

    protected enum StateChangeOperation {
        /** Clear the whole state. */
        CLEAR((byte) 0),
        /** Set (the whole) state to some value. */
        SET((byte) 1),
        /** Add a single state element to the current state. */
        ADD((byte) 2),
        /** Remove a single state element from the current state. */
        REMOVE((byte) 3),
        /** Add a collection of elements to the current state. */
        ADD_ALL((byte) 4),
        /** Merge states (usually from different namespaces). */
        MERGE((byte) 5);
        private final byte code;

        StateChangeOperation(byte code) {
            this.code = code;
        }
    }

    protected <T> Iterator<T> loggingIterator(
            @Nullable Iterator<T> iterator,
            BiConsumerWithException<T, DataOutputViewStreamWrapper, IOException> removeListener) {
        if (iterator == null) {
            return null;
        }
        return new Iterator<T>() {

            @Nullable private T lastReturned;

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                lastReturned = iterator.next();
                return lastReturned;
            }

            @Override
            public void remove() {
                try {
                    logStateChange(
                            StateChangeOperation.REMOVE,
                            writer -> removeListener.accept(lastReturned, writer));
                } catch (IOException e) {
                    ExceptionUtils.rethrow(e);
                }
                iterator.remove();
            }
        };
    }

    protected <T> Iterable<T> loggingIterable(
            @Nullable Iterable<T> iterable,
            BiConsumerWithException<T, DataOutputViewStreamWrapper, IOException> removeListener) {
        if (iterable == null) {
            return null;
        }
        return () -> loggingIterator(iterable.iterator(), removeListener);
    }
}
