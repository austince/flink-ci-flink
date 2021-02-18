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
    @Nullable private String path;
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
    public synchronized void handleSavepointCreation(
            @Nullable String path, @Nullable Throwable throwable) {
        final StopWithSavepointState oldState = state;
        state = state.onSavepointCreation(this, path, throwable);

        log.debug(
                "Stop-with-savepoint transitioned from {} to {} on savepoint creation handling.",
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
            protected StopWithSavepointState handleSavepointCreation(
                    StopWithSavepointContext context, String path) {
                context.path = path;
                return WaitForJobTermination;
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
            public StopWithSavepointState handleSavepointCreation(
                    StopWithSavepointContext context, String path) {
                Preconditions.checkState(
                        context.unfinishedStates != null,
                        InitialWait + " should have preceded: No unfinishedStates is set.");

                if (!context.unfinishedStates.isEmpty()) {
                    return context.terminateExceptionallyWithGlobalFailover(
                            context.unfinishedStates);
                }

                return context.terminateSuccessfully(path);
            }
        },
        WaitForJobTermination {
            @Override
            public StopWithSavepointState onExecutionsTermination(
                    StopWithSavepointContext context, Collection<ExecutionState> executionStates) {
                Preconditions.checkState(
                        context.path != null,
                        InitialWait + " should have preceded: No path is set.");

                Collection<ExecutionState> unfinishedExecutionStates =
                        extractUnfinishedStates(executionStates);
                if (!unfinishedExecutionStates.isEmpty()) {
                    return context.terminateExceptionallyWithGlobalFailover(
                            unfinishedExecutionStates);
                }

                return context.terminateSuccessfully(context.path);
            }
        },
        Final;

        public StopWithSavepointState onSavepointCreation(
                StopWithSavepointContext context,
                @Nullable String path,
                @Nullable Throwable throwable) {
            if (throwable != null) {
                return context.terminateExceptionally(throwable);
            }

            return handleSavepointCreation(context, path);
        }

        protected StopWithSavepointState handleSavepointCreation(
                StopWithSavepointContext context, String path) {
            throw new IllegalStateException(
                    "No onSavepointCreation should have been called in " + this.name() + " state.");
        }

        public StopWithSavepointState onExecutionsTermination(
                StopWithSavepointContext context, Collection<ExecutionState> executionStates) {
            throw new IllegalStateException(
                    "No onExecutionsTermination should have been called in "
                            + this.name()
                            + " state.");
        }
    }
}
