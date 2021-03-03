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

import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Comparator.comparingInt;
import static java.util.EnumSet.noneOf;
import static java.util.EnumSet.of;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.CANCELLED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.CONFIRMED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.FAILED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.MATERIALIZED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.PENDING;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.SCHEDULED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.SENT_TO_JM;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.UPLOADED;
import static org.apache.flink.changelog.fs.StateChangeSet.Status.UPLOADING;
import static org.apache.flink.util.Preconditions.checkState;

class StateChangeSet {
    private final UUID logId;
    private final AtomicReference<Status> status;
    private final List<StateChange> changes;
    private final SequenceNumber sequenceNumber;
    private final CompletableFuture<StoreResult> storeResultFuture;

    public StateChangeSet(
            UUID logId, SequenceNumber sequenceNumber, List<StateChange> changes, Status status) {
        this.logId = logId;
        this.changes = changes;
        this.changes.sort(comparingInt(StateChange::getKeyGroup));
        this.sequenceNumber = sequenceNumber;
        this.storeResultFuture = new CompletableFuture<>();
        this.status = new AtomicReference<>(status);
    }

    public UUID getLogId() {
        return logId;
    }

    public SequenceNumber getSequenceNumber() {
        return sequenceNumber;
    }

    public CompletableFuture<StoreResult> getStoreResult() {
        return storeResultFuture;
    }

    public List<StateChange> getChanges() {
        return changes;
    }

    public long getSize() {
        long size = 0;
        for (StateChange change : changes) {
            size += change.getChange().length;
        }
        return size;
    }

    boolean setScheduled() {
        return setStatus(SCHEDULED);
    }

    boolean setUploadStarted() {
        return setStatus(UPLOADING);
    }

    void setUploaded(StoreResult storeResult) {
        setStatusOrFail(UPLOADED);
        storeResultFuture.complete(storeResult);
    }

    void setSentToJm() {
        setStatusOrFail(SENT_TO_JM);
    }

    void setConfirmed() {
        if (setStatus(CONFIRMED)) {
            changes.clear();
        }
    }

    void setAborted() {
        transition(of(SENT_TO_JM), CANCELLED); // will be retried by re-creating the object
    }

    void setTruncated() {
        if (setStatus(MATERIALIZED)) {
            changes.clear();
        }
    }

    void setCancelled() {
        if (setStatus(CANCELLED)) {
            storeResultFuture.completeExceptionally(new CancellationException());
        }
    }

    void setFailed(Throwable failure) {
        if (setStatus(FAILED)) {
            storeResultFuture.completeExceptionally(failure);
        }
    }

    private boolean setStatus(Status newStatus) {
        return transition(EnumSet.allOf(Status.class), newStatus);
    }

    private boolean transition(Set<Status> expectedOldStatuses, Status newStatus) {
        Status oldStatus = status.get();
        return expectedOldStatuses.contains(oldStatus)
                && oldStatus.canTransitionTo(newStatus)
                && status.compareAndSet(oldStatus, newStatus);
    }

    private void setStatusOrFail(Status newStatus) {
        checkState(setStatus(newStatus), "can't transition from %s to %s", status.get(), newStatus);
    }

    public boolean isConfirmed() {
        return status.get() == CONFIRMED;
    }

    public StateChangeSet forRetry() {
        checkState(status.get().canBeRetried());
        return new StateChangeSet(logId, sequenceNumber, changes, SCHEDULED);
    }

    public Status getStatus() {
        return status.get();
    }

    enum Status {
        /**
         * Changes are in memory but not persisted yet (or at least without a guarantee). Will be
         * moved to SCHEDULED/UPLOADING upon trigger checkpoint RPC, checkpoint barrier or reaching
         * some threshold.
         */
        PENDING,
        /**
         * Changes are scheduled for upload. From now on, any materialization event is ignored for
         * this object to prevent any potential waiting checkpoints from becoming invalid.
         */
        SCHEDULED,
        /**
         * Changes are being uploaded. From now on, any materialization event is ignored for this
         * object to prevent any potential waiting checkpoints from becoming invalid. In case of
         * failure, changes can be moved either to FAILED or PENDING depending on the
         * implementation.
         */
        UPLOADING,
        /** Changes were uploaded bot not sent to the JM yet. */
        UPLOADED,
        /**
         * Changes were uploaded AND sent to the JM. In case if JM aborts the checkpoint, they
         * should be cleaned up and moved to PENDING because we are unsure whether they were
         * discarded and whether they will be used by further checkpoints..
         */
        SENT_TO_JM,
        /**
         * JM confirmed the checkpoint with the changes included. No re-upload will happen anymore.
         */
        CONFIRMED {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        },
        /**
         * State with these changes included was materialized. No re-upload will happen anymore.
         * Note that transition from SCHEDULED/UPLOADING/UPLOADED is not possible; however, the
         * reference CAN be removed from the client, so that future calls won't use this change set.
         */
        MATERIALIZED {
            @Override
            public boolean canBeRetried() {
                return false;
            }
        },
        /** Changes upload failed permanently. */
        FAILED,
        /** Cancelled e.g. due to shutdown. */
        CANCELLED;
        private static final Map<Status, Set<Status>> transitionsTo = new HashMap<>();

        static {
            // non-terminal states
            // any return to a previous state (e.g. for retry/abort) should be done by copying an
            // object and setting target state and replacing an old one in a registry
            transitionsTo.put(PENDING, of(SCHEDULED, MATERIALIZED, CANCELLED));
            transitionsTo.put(SCHEDULED, of(UPLOADING, FAILED, CANCELLED));
            transitionsTo.put(UPLOADING, of(UPLOADED));
            transitionsTo.put(UPLOADED, of(SENT_TO_JM, CANCELLED));
            transitionsTo.put(SENT_TO_JM, of(CONFIRMED, MATERIALIZED, CANCELLED));
            transitionsTo.put(CONFIRMED, of(MATERIALIZED, CANCELLED));
            // terminal states
            transitionsTo.put(MATERIALIZED, noneOf(Status.class));
            transitionsTo.put(FAILED, noneOf(Status.class));
            transitionsTo.put(CANCELLED, noneOf(Status.class));
        }

        public boolean canTransitionTo(Status newStatus) {
            return transitionsTo.get(this).contains(newStatus);
        }

        public boolean canBeRetried() {
            return true;
        }
    }

    @Override
    public String toString() {
        return "logId=" + logId + ", status=" + status + ", changes=" + changes;
    }
}
