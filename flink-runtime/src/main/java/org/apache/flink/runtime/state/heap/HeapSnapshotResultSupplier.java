/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult;

/**
 * Writes out HeapState snapshot.
 *
 * @param <K> key type.
 */
@Internal
public class HeapSnapshotResultSupplier<K>
        implements SnapshotStrategy.SnapshotResultSupplier<KeyedStateHandle> {
    protected final HeapSnapshotResources<K> syncPartResource;
    protected final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
            checkpointStreamSupplier;
    protected final KeyedBackendSerializationProxy<K> serializationProxy;
    protected final KeyGroupRange keyGroupRange;
    protected final StreamCompressionDecorator keyGroupCompressionDecorator;

    protected HeapSnapshotResultSupplier(
            HeapSnapshotResources<K> syncPartResource,
            SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                    checkpointStreamSupplier,
            KeyedBackendSerializationProxy<K> serializationProxy,
            KeyGroupRange keyGroupRange,
            StreamCompressionDecorator keyGroupCompressionDecorator) {
        this.syncPartResource = syncPartResource;
        this.checkpointStreamSupplier = checkpointStreamSupplier;
        this.serializationProxy = serializationProxy;
        this.keyGroupRange = keyGroupRange;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
    }

    @Override
    public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
            throws Exception {
        final Map<StateUID, Integer> stateNamesToId = syncPartResource.getStateNamesToId();
        final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
                syncPartResource.getCowStateStableSnapshots();
        final CheckpointStreamWithResultProvider streamWithResultProvider =
                checkpointStreamSupplier.get();

        snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

        final CheckpointStreamFactory.CheckpointStateOutputStream localStream =
                streamWithResultProvider.getCheckpointOutputStream();

        final DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(localStream);
        serializationProxy.write(outView);

        final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

        for (int keyGroupPos = 0;
                keyGroupPos < keyGroupRange.getNumberOfKeyGroups();
                ++keyGroupPos) {
            int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
            keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
            outView.writeInt(keyGroupId);

            for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
                    cowStateStableSnapshots.entrySet()) {
                StateSnapshot.StateKeyGroupWriter partitionedSnapshot =
                        stateSnapshot.getValue().getKeyGroupWriter();
                try (OutputStream kgCompressionOut =
                        keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
                    DataOutputViewStreamWrapper kgCompressionView =
                            new DataOutputViewStreamWrapper(kgCompressionOut);
                    writeHeader(
                            kgCompressionView,
                            stateNamesToId.get(stateSnapshot.getKey()),
                            stateSnapshot.getValue());
                    writeBody(keyGroupId, partitionedSnapshot, kgCompressionView);
                } // this will just close the outer compression stream
            }
        }

        if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
            KeyGroupRangeOffsets kgOffs =
                    new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
            SnapshotResult<StreamStateHandle> result =
                    streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
            return toKeyedStateHandleSnapshotResult(result, kgOffs, KeyGroupsStateHandle::new);
        } else {
            throw new IOException("Stream already unregistered.");
        }
    }

    protected void writeHeader(
            DataOutputViewStreamWrapper kgCompressionView, int stateId, StateSnapshot snapshot)
            throws IOException {
        kgCompressionView.writeShort(stateId);
    }

    protected void writeBody(
            int keyGroupId,
            StateSnapshot.StateKeyGroupWriter partitionedSnapshot,
            DataOutputViewStreamWrapper kgCompressionView)
            throws IOException {
        partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, keyGroupId);
    }
}
