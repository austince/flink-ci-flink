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
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** {@code StopWithSavepointContext} implements {@link StopWithSavepointOperations}. */
public class StopWithSavepointContext implements StopWithSavepointOperations {

    private final Logger log;

    private final SchedulerNG scheduler;
    private final CheckpointScheduling checkpointScheduling;
    private final JobID jobId;

    private final CompletableFuture<String> result = new CompletableFuture<>();

    private StopWithSavepointState state = StopWithSavepointState.InitialWait;
    @Nullable private CompletedCheckpoint completedSavepoint;
    @Nullable private Set<ExecutionState> unfinishedStates;

    public <S extends SchedulerNG & CheckpointScheduling> StopWithSavepointContext(
            JobID jobId, S schedulerWithCheckpointing, Logger log) {
        this(jobId, schedulerWithCheckpointing, schedulerWithCheckpointing, log);
    }

    StopWithSavepointContext(
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
        final StopWithSavepointState oldState = state;
        state = state.onSavepointCreation(this, completedCheckpoint);

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on savepoint creation handling.",
                oldState,
                state);
    }

    @Override
    public synchronized void handleSavepointCreationFailure(Throwable throwable) {
        final StopWithSavepointState oldState = state;
        state = state.onSavepointCreationFailure(this, throwable);

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on savepoint creation failure handling.",
                oldState,
                state);
    }

    @Override
    public synchronized void handleExecutionTermination(
            Collection<ExecutionState> executionStates) {
        final StopWithSavepointState oldState = state;
        state = state.onExecutionsTermination(this, executionStates);

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on execution termination handling.",
                oldState,
                state);
    }

    @Override
    public CompletableFuture<String> getResult() {
        return result;
    }

    private StopWithSavepointState terminateExceptionallyWithGlobalFailover(
            Iterable<ExecutionState> unfinishedExecutionStates) {
        String errorMessage =
                String.format(
                        "Inconsistent execution state after stopping with savepoint. At least one execution is still in one of the following states: %s. A global fail-over is triggered to recover the job %s.",
                        StringUtils.join(unfinishedExecutionStates, ", "), jobId);
        FlinkException inconsistentFinalStateException = new FlinkException(errorMessage);

        scheduler.handleGlobalFailure(inconsistentFinalStateException);
        return terminateExceptionally(inconsistentFinalStateException);
    }

    private StopWithSavepointState terminateExceptionally(Throwable throwable) {
        checkpointScheduling.startCheckpointScheduler();
        result.completeExceptionally(throwable);

        return StopWithSavepointState.Final;
    }

    private StopWithSavepointState terminateSuccessfully(String path) {
        result.complete(path);

        return StopWithSavepointState.Final;
    }

    private static Set<ExecutionState> extractUnfinishedStates(
            Collection<ExecutionState> executionStates) {
        return executionStates.stream()
                .filter(state -> state != ExecutionState.FINISHED)
                .collect(Collectors.toSet());
    }

    /**
     * {@code StopWithSavepointState} represents the different states during the stop-with-savepoint
     * operation.
     *
     * <p>The state transitions are implemented in the following way: InitialWait ->
     * [WaitForSavepointCreation|WaitForJobTermination] -> Final
     */
    private enum StopWithSavepointState {
        InitialWait {
            @Override
            public StopWithSavepointState onSavepointCreation(
                    StopWithSavepointContext context, CompletedCheckpoint completedSavepoint) {
                context.completedSavepoint = completedSavepoint;
                return WaitForJobTermination;
            }

            @Override
            public StopWithSavepointState onSavepointCreationFailure(
                    StopWithSavepointContext context, Throwable throwable) {
                return context.terminateExceptionally(throwable);
            }

            @Override
            public StopWithSavepointState onExecutionsTermination(
                    StopWithSavepointContext context, Collection<ExecutionState> executionStates) {
                context.unfinishedStates = extractUnfinishedStates(executionStates);
                return WaitForSavepointCreation;
            }
        },
        WaitForSavepointCreation {
            @Override
            public StopWithSavepointState onSavepointCreation(
                    StopWithSavepointContext context, CompletedCheckpoint completedSavepoint) {
                Preconditions.checkState(
                        context.unfinishedStates != null,
                        InitialWait + " should have preceded: No unfinishedStates is set.");

                return context.terminateSuccessfully(completedSavepoint.getExternalPointer());
            }

            @Override
            public StopWithSavepointState onSavepointCreationFailure(
                    StopWithSavepointContext context, Throwable throwable) {
                Preconditions.checkState(
                        context.unfinishedStates != null,
                        InitialWait + " should have preceded: No unfinishedStates is set.");

                return context.terminateExceptionally(throwable);
            }

            @Override
            public StopWithSavepointState onExecutionsTermination(
                    StopWithSavepointContext context, Collection<ExecutionState> executionStates) {
                Preconditions.checkState(
                        context.unfinishedStates != null,
                        InitialWait + " should have preceded: No unfinishedStates is set.");

                // we still have to wait for the SavepointCreation to terminate
                return WaitForSavepointCreation;
            }
        },
        WaitForJobTermination {
            @Override
            public StopWithSavepointState onSavepointCreation(
                    StopWithSavepointContext context, CompletedCheckpoint completedSavepoint) {
                throw new IllegalStateException(
                        "The savepoint should have been completed already while waiting for the job termination.");
            }

            @Override
            public StopWithSavepointState onSavepointCreationFailure(
                    StopWithSavepointContext context, Throwable throwable) {
                throw new IllegalStateException(
                        "The savepoint should have been completed already while waiting for the job termination.");
            }

            @Override
            public StopWithSavepointState onExecutionsTermination(
                    StopWithSavepointContext context, Collection<ExecutionState> executionStates) {
                Preconditions.checkState(
                        context.completedSavepoint != null,
                        InitialWait
                                + " should have preceded: No completed savepoint was colelcted.");

                Collection<ExecutionState> unfinishedExecutionStates =
                        extractUnfinishedStates(executionStates);
                if (!unfinishedExecutionStates.isEmpty()) {
                    return context.terminateExceptionallyWithGlobalFailover(
                            unfinishedExecutionStates);
                }

                return context.terminateSuccessfully(
                        context.completedSavepoint.getExternalPointer());
            }
        },
        Final {
            @Override
            public StopWithSavepointState onSavepointCreation(
                    StopWithSavepointContext context, CompletedCheckpoint completedSavepoint) {
                throw new IllegalStateException(
                        "onSavepointCreation was called on "
                                + Final
                                + " state. The operation will be ignored.");
            }

            @Override
            public StopWithSavepointState onSavepointCreationFailure(
                    StopWithSavepointContext context, Throwable throwable) {
                throw new IllegalStateException(
                        "onSavepointCreationFailure was called on "
                                + Final
                                + " state. The operation will be ignored.");
            }

            @Override
            public StopWithSavepointState onExecutionsTermination(
                    StopWithSavepointContext context, Collection<ExecutionState> executionStates) {
                context.log.debug(
                        "onExecutionsTermination was called on "
                                + Final
                                + " state. The operation will be ignored.");
                return Final;
            }
        };

        public abstract StopWithSavepointState onSavepointCreation(
                StopWithSavepointContext context, CompletedCheckpoint completedSavepoint);

        public abstract StopWithSavepointState onSavepointCreationFailure(
                StopWithSavepointContext context, Throwable throwable);

        public abstract StopWithSavepointState onExecutionsTermination(
                StopWithSavepointContext context, Collection<ExecutionState> executionStates);
    }
}
