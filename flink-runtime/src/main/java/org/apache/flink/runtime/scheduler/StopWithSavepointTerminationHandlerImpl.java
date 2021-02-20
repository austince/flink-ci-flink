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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.util.FlinkException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@code StopWithSavepointTerminationHandlerImpl} implements {@link
 * StopWithSavepointTerminationHandler}.
 *
 * <p>The operation only succeeds if both steps, the savepoint creation and the successful
 * termination of the job, succeed. If the former step fails, the operation fails exceptionally
 * without any further actions. If the latter one fails, a global fail-over is triggered before
 * failing the operation.
 */
public class StopWithSavepointTerminationHandlerImpl
        implements StopWithSavepointTerminationHandler {

    private final Logger log;

    private final SchedulerNG scheduler;
    private final CheckpointScheduling checkpointScheduling;
    private final JobID jobId;

    private final CompletableFuture<CompletedCheckpoint> result = new CompletableFuture<>();

    private State state = new WaitingForSavepoint();

    public <S extends SchedulerNG & CheckpointScheduling> StopWithSavepointTerminationHandlerImpl(
            JobID jobId, S schedulerWithCheckpointing, Logger log) {
        this(jobId, schedulerWithCheckpointing, schedulerWithCheckpointing, log);
    }

    @VisibleForTesting
    StopWithSavepointTerminationHandlerImpl(
            JobID jobId,
            SchedulerNG scheduler,
            CheckpointScheduling checkpointScheduling,
            Logger log) {
        this.jobId = checkNotNull(jobId);
        this.scheduler = checkNotNull(scheduler);
        this.checkpointScheduling = checkNotNull(checkpointScheduling);
        this.log = checkNotNull(log);
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> getCompletedSavepoint() {
        return result;
    }

    @Override
    public void handleSavepointCreation(CompletedCheckpoint completedCheckpoint) {
        final State oldState = state;
        state = state.onSavepointCreation(completedCheckpoint);

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on savepoint creation handling for job {}.",
                oldState,
                state,
                jobId);
    }

    @Override
    public void handleSavepointCreationFailure(Throwable throwable) {
        final State oldState = state;
        state = state.onSavepointCreationFailure(throwable);

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on savepoint creation failure handling for job {}.",
                oldState,
                state,
                jobId);
    }

    @Override
    public void handleExecutionsFinished() {
        final State oldState = state;
        state = state.onExecutionsFinished();

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on execution termination handling with all executions being finished for job {}.",
                oldState,
                state,
                jobId);
    }

    @Override
    public void handleAnyExecutionNotFinished(Set<ExecutionState> notFinishedExecutionStates) {
        final State oldState = state;
        state = state.onAnyExecutionNotFinished(notFinishedExecutionStates);

        log.warn(
                "Stop-with-savepoint transitioned from {} to {} on execution termination handling for job {} with some executions being in an not-finished state: {}",
                oldState,
                state,
                jobId,
                notFinishedExecutionStates);
    }

    /**
     * Handles the termination of the {@code StopWithSavepointTerminationHandler} exceptionally
     * after triggering a global job fail-over.
     *
     * @param completedSavepoint the completed savepoint that needs to be discarded.
     * @param unfinishedExecutionStates the unfinished states that caused the failure.
     */
    private void terminateExceptionallyWithGlobalFailover(
            CompletedCheckpoint completedSavepoint,
            Iterable<ExecutionState> unfinishedExecutionStates) {
        String errorMessage =
                String.format(
                        "Inconsistent execution state after stopping with savepoint. At least one execution is still in one of the following states: %s. A global fail-over is triggered to recover the job %s.",
                        StringUtils.join(unfinishedExecutionStates, ", "), jobId);
        FlinkException inconsistentFinalStateException = new FlinkException(errorMessage);

        scheduler.handleGlobalFailure(inconsistentFinalStateException);

        try {
            completedSavepoint.discard();
        } catch (Exception e) {
            log.warn(
                    "Error occurred while cleaning up completed savepoint due to stop-with-savepoint failure.",
                    e);
            inconsistentFinalStateException.addSuppressed(e);
        }

        terminateExceptionally(inconsistentFinalStateException);
    }

    /**
     * Handles the termination of the {@code StopWithSavepointTerminationHandler} exceptionally
     * without triggering a global job fail-over. It does restart the checkpoint scheduling.
     *
     * @param throwable the error that caused the exceptional termination.
     */
    private void terminateExceptionally(Throwable throwable) {
        checkpointScheduling.startCheckpointScheduler();
        result.completeExceptionally(throwable);
    }

    /**
     * Handles the successful termination of the {@code StopWithSavepointTerminationHandler}.
     *
     * @param completedSavepoint the completed savepoint
     */
    private void terminateSuccessfully(CompletedCheckpoint completedSavepoint) {
        result.complete(completedSavepoint);
    }

    private final class WaitingForSavepoint implements State {

        @Override
        public State onSavepointCreation(CompletedCheckpoint completedSavepoint) {
            return new SavepointCreated(completedSavepoint);
        }

        @Override
        public State onSavepointCreationFailure(Throwable throwable) {
            terminateExceptionally(throwable);
            return new FinalState();
        }
    }

    private final class SavepointCreated implements State {

        private final CompletedCheckpoint completedSavepoint;

        private SavepointCreated(CompletedCheckpoint completedSavepoint) {
            this.completedSavepoint = completedSavepoint;
        }

        @Override
        public State onExecutionsFinished() {
            terminateSuccessfully(completedSavepoint);
            return new FinalState();
        }

        @Override
        public State onAnyExecutionNotFinished(
                Iterable<ExecutionState> notFinishedExecutionStates) {
            terminateExceptionallyWithGlobalFailover(
                    completedSavepoint, notFinishedExecutionStates);
            return new FinalState();
        }
    }

    private final class FinalState implements State {

        @Override
        public State onExecutionsFinished() {
            return this;
        }

        @Override
        public State onAnyExecutionNotFinished(
                Iterable<ExecutionState> notFinishedExecutionStates) {
            return this;
        }
    }

    private interface State {

        default State onSavepointCreation(CompletedCheckpoint completedSavepoint) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onSavepointCreation.");
        }

        default State onSavepointCreationFailure(Throwable throwable) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onSavepointCreationFailure.");
        }

        default State onExecutionsFinished() {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onExecutionsFinished.");
        }

        default State onAnyExecutionNotFinished(
                Iterable<ExecutionState> notFinishedExecutionStates) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onAnyExecutionNotFinished.");
        }
    }
}
