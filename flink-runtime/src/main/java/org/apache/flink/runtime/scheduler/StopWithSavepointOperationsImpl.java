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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.util.FlinkException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** {@code StopWithSavepointOperationsImpl} implements {@link StopWithSavepointOperations}. */
public class StopWithSavepointOperationsImpl implements StopWithSavepointOperations {

    private final Logger log;

    private final SchedulerNG scheduler;
    private final CheckpointScheduling checkpointScheduling;
    private final JobID jobId;

    private final CompletableFuture<String> result = new CompletableFuture<>();

    private State state = new InitialState();

    public <S extends SchedulerNG & CheckpointScheduling> StopWithSavepointOperationsImpl(
            JobID jobId, S schedulerWithCheckpointing, Logger log) {
        this(jobId, schedulerWithCheckpointing, schedulerWithCheckpointing, log);
    }

    StopWithSavepointOperationsImpl(
            JobID jobId,
            SchedulerNG scheduler,
            CheckpointScheduling checkpointScheduling,
            Logger log) {
        this.jobId = jobId;
        this.scheduler = scheduler;
        this.checkpointScheduling = checkpointScheduling;
        this.log = log;
    }

    @Override
    public synchronized void handleSavepointCreation(CompletedCheckpoint completedCheckpoint) {
        final State oldState = state;
        state = state.onSavepointCreation(completedCheckpoint);

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on savepoint creation handling.",
                oldState,
                state);
    }

    @Override
    public synchronized void handleSavepointCreationFailure(Throwable throwable) {
        final State oldState = state;
        state = state.onSavepointCreationFailure(throwable);

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on savepoint creation failure handling.",
                oldState,
                state);
    }

    @Override
    public synchronized void handleExecutionsTermination(
            Collection<ExecutionState> executionStates) {
        final State oldState = state;
        state = state.onExecutionsTermination(executionStates);

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on execution termination handling.",
                oldState,
                state);
    }

    @Override
    public CompletableFuture<String> getResult() {
        return result;
    }

    /**
     * Handles the termination of the {@code StopWithSavepointOperations} exceptionally after
     * triggering a global job fail-over.
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
     * Handles the termination of the {@code StopWithSavepointOperations} exceptionally without
     * triggering a global job fail-over. It does restart the checkpoint scheduling.
     *
     * @param throwable the error that caused the exceptional termination.
     */
    private void terminateExceptionally(Throwable throwable) {
        checkpointScheduling.startCheckpointScheduler();
        result.completeExceptionally(throwable);
    }

    /**
     * Handles the successful termination of the {@code StopWithSavepointOperations}.
     *
     * @param path the path where the savepoint is stored.
     */
    private void terminateSuccessfully(String path) {
        result.complete(path);
    }

    private static Set<ExecutionState> extractUnfinishedStates(
            Collection<ExecutionState> executionStates) {
        return executionStates.stream()
                .filter(state -> state != ExecutionState.FINISHED)
                .collect(Collectors.toSet());
    }

    private final class InitialState extends State {

        @Override
        public State onSavepointCreation(CompletedCheckpoint completedSavepoint) {
            return new SavepointCreated(completedSavepoint);
        }

        @Override
        public State onSavepointCreationFailure(Throwable throwable) {
            terminateExceptionally(throwable);
            return new FinalState();
        }

        @Override
        public State onExecutionsTermination(Collection<ExecutionState> executionStates) {
            final Set<ExecutionState> unfinishedStates = extractUnfinishedStates(executionStates);

            if (unfinishedStates.isEmpty()) {
                return new ExecutionsFinished();
            }

            return new ExecutionsTerminatedExceptionally(unfinishedStates);
        }
    }

    private final class SavepointCreated extends State {

        private final CompletedCheckpoint completedSavepoint;

        private SavepointCreated(CompletedCheckpoint completedSavepoint) {
            this.completedSavepoint = completedSavepoint;
        }

        @Override
        public State onExecutionsTermination(Collection<ExecutionState> executionStates) {
            final Set<ExecutionState> unfinishedStates = extractUnfinishedStates(executionStates);

            if (unfinishedStates.isEmpty()) {
                terminateSuccessfully(completedSavepoint.getExternalPointer());
                return new FinalState();
            }

            terminateExceptionallyWithGlobalFailover(completedSavepoint, unfinishedStates);
            return new FinalState();
        }
    }

    private final class ExecutionsFinished extends State {

        @Override
        public State onSavepointCreation(CompletedCheckpoint completedSavepoint) {
            terminateSuccessfully(completedSavepoint.getExternalPointer());
            return new FinalState();
        }

        @Override
        public State onSavepointCreationFailure(Throwable throwable) {
            terminateExceptionally(throwable);
            return new FinalState();
        }
    }

    private final class ExecutionsTerminatedExceptionally extends State {

        private final Iterable<ExecutionState> unfinishedExecutionStates;

        private ExecutionsTerminatedExceptionally(
                Iterable<ExecutionState> unfinishedExecutionStates) {
            this.unfinishedExecutionStates = unfinishedExecutionStates;
        }

        @Override
        public State onSavepointCreation(CompletedCheckpoint completedSavepoint) {
            terminateExceptionallyWithGlobalFailover(
                    completedSavepoint, this.unfinishedExecutionStates);
            return new FinalState();
        }

        @Override
        public State onSavepointCreationFailure(Throwable throwable) {
            terminateExceptionally(throwable);
            return new FinalState();
        }

        @Override
        public State onExecutionsTermination(Collection<ExecutionState> executionStates) {
            throw new IllegalStateException(
                    "onExecutionsTermination was triggered in "
                            + ExecutionsTerminatedExceptionally.class.getSimpleName()
                            + " state.");
        }
    }

    private final class FinalState extends State {

        @Override
        public State onExecutionsTermination(Collection<ExecutionState> executionStates) {
            return this;
        }
    }

    private abstract class State {

        public State onSavepointCreation(CompletedCheckpoint completedSavepoint) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onSavepointCreation.");
        }

        public State onSavepointCreationFailure(Throwable throwable) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onSavepointCreationFailure.");
        }

        public State onExecutionsTermination(Collection<ExecutionState> executionStates) {
            throw new UnsupportedOperationException(
                    this.getClass().getSimpleName()
                            + " state does not support onExecutionsTermination.");
        }
    }
}
