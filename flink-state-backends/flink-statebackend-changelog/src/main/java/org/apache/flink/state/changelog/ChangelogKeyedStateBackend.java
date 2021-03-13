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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.TestableKeyedStateBackend;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link KeyedStateBackend} that keeps state on the underlying delegated keyed state backend as
 * well as on the state change log.
 *
 * @param <K> The key by which state is keyed.
 */
@Internal
class ChangelogKeyedStateBackend<K>
        implements CheckpointableKeyedStateBackend<K>,
                CheckpointListener,
                TestableKeyedStateBackend {

    /** delegated keyedStateBackend. */
    private final AbstractKeyedStateBackend<K> keyedStateBackend;

    /**
     * This is the cache maintained by the DelegateKeyedStateBackend itself. It is not the same as
     * the underlying delegated keyedStateBackend. InternalKvState is a delegated state.
     */
    private final HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName;

    private final ExecutionConfig executionConfig;

    private final TtlTimeProvider ttlTimeProvider;

    private final StateChangelogWriter<?> stateChangelogWriter;

    /** last accessed partitioned state. */
    @SuppressWarnings("rawtypes")
    private InternalKvState lastState;

    /** For caching the last accessed partitioned state. */
    private String lastName;

    public ChangelogKeyedStateBackend(
            AbstractKeyedStateBackend<K> keyedStateBackend,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            StateChangelogWriter<?> stateChangelogWriter) {
        this.keyedStateBackend = keyedStateBackend;
        this.executionConfig = executionConfig;
        this.ttlTimeProvider = ttlTimeProvider;
        this.keyValueStatesByName = new HashMap<>();
        this.stateChangelogWriter = stateChangelogWriter;
    }

    // -------------------- CheckpointableKeyedStateBackend --------------------------------
    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyedStateBackend.getKeyGroupRange();
    }

    @Override
    public void close() throws IOException {
        keyedStateBackend.close();
    }

    @Override
    public void setCurrentKey(K newKey) {
        keyedStateBackend.setCurrentKey(newKey);
    }

    @Override
    public K getCurrentKey() {
        return keyedStateBackend.getCurrentKey();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keyedStateBackend.getKeySerializer();
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return keyedStateBackend.getKeys(state, namespace);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        return keyedStateBackend.getKeysAndNamespaces(state);
    }

    @Override
    public void dispose() {
        keyedStateBackend.dispose();
        lastName = null;
        lastState = null;
        keyValueStatesByName.clear();
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        keyedStateBackend.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return keyedStateBackend.deregisterKeySelectionListener(listener);
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, T> stateDescriptor,
            KeyedStateFunction<K, S> function)
            throws Exception {

        keyedStateBackend.applyToAllKeys(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                function,
                this::getPartitionedState);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {

        checkNotNull(namespace, "Namespace");

        if (lastName != null && lastName.equals(stateDescriptor.getName())) {
            lastState.setCurrentNamespace(namespace);
            return (S) lastState;
        }

        final InternalKvState<K, ?, ?> previous =
                keyValueStatesByName.get(stateDescriptor.getName());
        if (previous != null) {
            lastState = previous;
            lastState.setCurrentNamespace(namespace);
            lastName = stateDescriptor.getName();
            return (S) previous;
        }

        final S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        final InternalKvState<K, N, ?> kvState = (InternalKvState<K, N, ?>) state;

        lastName = stateDescriptor.getName();
        lastState = kvState;
        kvState.setCurrentNamespace(namespace);

        return state;
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        return keyedStateBackend.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return keyedStateBackend.create(stateName, byteOrderedElementSerializer);
    }

    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        return keyedStateBackend.numKeyValueStateEntries();
    }

    @Override
    public boolean isStateImmutableInStateBackend(CheckpointType checkpointOptions) {
        return keyedStateBackend.isStateImmutableInStateBackend(checkpointOptions);
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() throws Exception {
        return keyedStateBackend.savepoint();
    }

    // -------------------- CheckpointListener --------------------------------
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        keyedStateBackend.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        keyedStateBackend.notifyCheckpointAborted(checkpointId);
    }

    // -------- Methods not simply delegating to wrapped state backend ---------
    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        checkNotNull(namespaceSerializer, "Namespace serializer");
        checkNotNull(
                getKeySerializer(),
                "State key serializer has not been configured in the config. "
                        + "This operation cannot use partitioned state.");

        InternalKvState<K, ?, ?> kvState = keyValueStatesByName.get(stateDescriptor.getName());
        if (kvState == null) {
            if (!stateDescriptor.isSerializerInitialized()) {
                stateDescriptor.initializeSerializerUnlessSet(executionConfig);
            }
            kvState =
                    TtlStateFactory.createStateAndWrapWithTtlIfEnabled(
                            namespaceSerializer, stateDescriptor, this, ttlTimeProvider);
            keyValueStatesByName.put(stateDescriptor.getName(), kvState);
            keyedStateBackend.publishQueryableStateIfEnabled(stateDescriptor, kvState);
        }
        return (S) kvState;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes", "NullableProblems"})
    public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, SV> stateDesc,
            StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        InternalKvState internalState =
                keyedStateBackend.createInternalState(
                        namespaceSerializer, stateDesc, snapshotTransformFactory);

        if (stateDesc instanceof ValueStateDescriptor) {
            return (IS)
                    new ChangelogValueState<>(
                            (InternalValueState) internalState,
                            stateChangelogWriter,
                            keyedStateBackend);
        } else if (stateDesc instanceof ListStateDescriptor) {
            return (IS)
                    new ChangelogListState<>(
                            (InternalListState) internalState,
                            stateChangelogWriter,
                            keyedStateBackend);
        } else if (stateDesc instanceof ReducingStateDescriptor) {
            return (IS)
                    new ChangelogReducingState(
                            (InternalReducingState) internalState,
                            stateChangelogWriter,
                            keyedStateBackend);
        } else if (stateDesc instanceof AggregatingStateDescriptor) {
            return (IS)
                    new ChangelogAggregatingState<>(
                            (InternalAggregatingState) internalState,
                            stateChangelogWriter,
                            keyedStateBackend);
        } else if (stateDesc instanceof MapStateDescriptor) {
            return (IS)
                    new ChangelogMapState(
                            (InternalMapState) internalState,
                            stateChangelogWriter,
                            keyedStateBackend);
        } else {
            throw new FlinkRuntimeException(
                    String.format(
                            "State %s is not supported by %s",
                            stateDesc.getClass(), this.getClass()));
        }
    }
}
