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

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.changelog.StateChangelogHandleStreamImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.changelog.fs.StateChangeSet.Status.PENDING;
import static org.apache.flink.runtime.concurrent.FutureUtils.combineAll;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@NotThreadSafe
class FsStateChangelogWriter implements StateChangelogWriter<StateChangelogHandleStreamImpl> {
    private static final Logger LOG = LoggerFactory.getLogger(FsStateChangelogWriter.class);

    private final UUID logId;
    private final KeyGroupRange keyGroupRange;
    private final StateChangeStore store;
    private final NavigableMap<SequenceNumber, StateChangeSet> changeSets = new TreeMap<>();
    private List<StateChange> activeChangeSet = new ArrayList<>();
    private SequenceNumber lastAppendedSequenceNumber = SequenceNumber.of(0L);
    private boolean closed;

    FsStateChangelogWriter(UUID logId, KeyGroupRange keyGroupRange, StateChangeStore store) {
        this.logId = logId;
        this.keyGroupRange = keyGroupRange;
        this.store = store;
    }

    @Override
    public void append(int keyGroup, byte[] value) {
        LOG.trace("append to {}: keyGroup={} {} bytes", logId, keyGroup, value.length);
        checkState(!closed, "%s is closed", logId);
        activeChangeSet.add(new StateChange(keyGroup, value));
        // size threshold could be added to call persist when reached. considerations:
        // 0. can actually degrade performance by amplifying number of requests
        // 1. which range to persist?
        // 2. how to deal with retries/aborts?
    }

    @Override
    public SequenceNumber lastAppendedSequenceNumber() {
        rollover();
        LOG.trace("query {} sqn: {}", logId, lastAppendedSequenceNumber);
        return lastAppendedSequenceNumber;
    }

    @Override
    public CompletableFuture<StateChangelogHandleStreamImpl> persist(SequenceNumber from)
            throws IOException {
        LOG.debug("persist {} from {}", logId, from);
        checkNotNull(from);
        checkArgument(
                lastAppendedSequenceNumber.next().equals(from) || changeSets.containsKey(from),
                "sequence number %s to persist from not in range (%s:%s/%s)",
                from,
                changeSets.isEmpty() ? null : changeSets.firstKey(),
                changeSets.isEmpty() ? null : changeSets.lastKey(),
                lastAppendedSequenceNumber.next());

        rollover();
        Collected collected = collect(from);
        collected.toRetry.forEach(
                changeSet -> changeSets.put(changeSet.getSequenceNumber(), changeSet));
        LOG.debug("collected {}", collected);
        store.save(collected.toUpload);
        return asHandle(collected.toReturn);
    }

    private Collected collect(SequenceNumber from) {
        Collected result = new Collected();
        changeSets
                .tailMap(from, true)
                .values()
                .forEach(
                        changeSet -> {
                            if (changeSet.isConfirmed()) {
                                result.toReturn.add(changeSet);
                            } else if (changeSet.setScheduled()) {
                                result.toUpload.add(changeSet);
                            } else {
                                // we also re-upload any scheduled/uploading/uploaded changes
                                // even if they were not sent to the JM yet because this can happen
                                // in the meantime and then JM can decide to discard them
                                result.toRetry.add(changeSet.forRetry());
                            }
                        });
        result.toUpload.addAll(result.toRetry);
        result.toReturn.addAll(result.toUpload);
        return result;
    }

    @Override
    public void close() {
        LOG.debug("close {}", logId);
        checkState(!closed);
        closed = true;
        activeChangeSet.clear();
        changeSets.values().forEach(StateChangeSet::setCancelled);
        // todo in MVP or later: cleanup if transition succeeded and had non-shared state
        changeSets.clear();
        // the store is closed from the owning FsStateChangelogClient
    }

    @Override
    public void confirm(SequenceNumber from, SequenceNumber to) {
        LOG.debug("confirm {} from {} to {}", logId, from, to);
        changeSets.subMap(from, true, to, false).values().forEach(StateChangeSet::setConfirmed);
    }

    @Override
    public void reset(SequenceNumber from, SequenceNumber to) {
        LOG.debug("reset {} from {} to {}", logId, from, to);
        changeSets.subMap(from, to).forEach((key, value) -> value.setAborted());
        // todo in MVP or later: cleanup if change to aborted succeeded and had non-shared state
        // For now, relying on manual cleanup.
        // If a checkpoint that is aborted uses the changes uploaded for another checkpoint
        // which was completed on JM but not confirmed to this TM
        // then discarding those changes would invalidate that previously completed checkpoint.
        // Solution:
        // 1. pass last completed checkpoint id in barriers, trigger RPC, and abort RPC
        // 2. confirm for the id above
        // 3. make sure that at most 1 checkpoint in flight (CC config)
    }

    @Override
    public void truncate(SequenceNumber to) {
        LOG.debug("truncate {} to {}", logId, to);
        if (to.compareTo(lastAppendedSequenceNumber) > 0) {
            // can happen if client calls truncate(prevSqn.next())
            rollover();
        }
        NavigableMap<SequenceNumber, StateChangeSet> headMap = changeSets.headMap(to, false);
        headMap.values().forEach(StateChangeSet::setTruncated);
        headMap.clear();
    }

    private void rollover() {
        if (activeChangeSet.isEmpty()) {
            return;
        }
        lastAppendedSequenceNumber = lastAppendedSequenceNumber.next();
        changeSets.put(
                lastAppendedSequenceNumber,
                new StateChangeSet(logId, lastAppendedSequenceNumber, activeChangeSet, PENDING));
        activeChangeSet = new ArrayList<>();
    }

    private CompletableFuture<StateChangelogHandleStreamImpl> asHandle(
            Collection<StateChangeSet> snapshot) {
        return combineAll(
                        snapshot.stream()
                                .map(StateChangeSet::getStoreResult)
                                .collect(Collectors.toList()))
                .thenApply(uploaded -> buildHandle(snapshot, uploaded));
    }

    private StateChangelogHandleStreamImpl buildHandle(
            Collection<StateChangeSet> changes, Collection<StoreResult> results) {
        List<Tuple2<StreamStateHandle, Long>> sorted =
                results.stream()
                        // can't assume order across different handles because of retries and aborts
                        .sorted(Comparator.comparing(StoreResult::getSequenceNumber))
                        .map(up -> Tuple2.of(up.getStreamStateHandle(), up.getOffset()))
                        .collect(Collectors.toList());
        changes.forEach(StateChangeSet::setSentToJm);
        // todo in MVP: replace old handles with placeholders (but maybe in the backend)
        return new StateChangelogHandleStreamImpl(sorted, keyGroupRange);
    }

    @VisibleForTesting
    SequenceNumber lastAppendedSqnUnsafe() {
        return lastAppendedSequenceNumber;
    }

    private static class Collected {
        private final Collection<StateChangeSet> toUpload = new ArrayList<>();
        private final Collection<StateChangeSet> toRetry = new ArrayList<>();
        private final Collection<StateChangeSet> toReturn = new ArrayList<>();

        @Override
        public String toString() {
            return "toUpload="
                    + toUpload.size()
                    + ", toRetry="
                    + toRetry.size()
                    + ", toReturn="
                    + toReturn.size();
        }
    }
}
