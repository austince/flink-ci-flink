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
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.util.FlinkException;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** {@code StopWithSavepointContext} implements {@link StopWithSavepointOperations}. */
public class StopWithSavepointContext implements StopWithSavepointOperations {

    private final SchedulerBase scheduler;
    private final CheckpointCoordinator checkpointCoordinator;
    private final JobID jobId;

    private final CompletableFuture<String> result = new CompletableFuture<>();

    private StopWithSavepointState state = StopWithSavepointState.WaitForSavepointCreation;
    private String path;

    public StopWithSavepointContext(JobID jobId, SchedulerBase scheduler) {
        this.jobId = jobId;
        this.scheduler = scheduler;
        this.checkpointCoordinator = scheduler.getCheckpointCoordinator();
    }

    @Override
    public void handleSavepointCreation(String path, Throwable throwable) {
        state = state.onSavepointCreation(this, path, throwable);
    }

    @Override
    public void handleExecutionTermination(
            Collection<ExecutionState> executionStates, Throwable throwable) {
        state = state.onExecutionsTermination(this, executionStates, throwable);
    }

    @Override
    public CompletableFuture<String> getResult() {
        return result;
    }

    private StopWithSavepointState terminateExceptionally(Throwable throwable) {
        scheduler.startCheckpointScheduler(checkpointCoordinator);
        result.completeExceptionally(throwable);

        return StopWithSavepointState.Final;
    }

    private StopWithSavepointState terminateSuccessfully() {
        result.complete(path);

        return StopWithSavepointState.Final;
    }

    private static Set<ExecutionState> extractNonFinishedStates(
            Collection<ExecutionState> executionStates) {
        return executionStates.stream()
                .filter(state -> state != ExecutionState.FINISHED)
                .collect(Collectors.toSet());
    }

    /**
     * {@code StopWithSavepointState} represents the different states during the stop-with-savepoint
     * operation.
     */
    private enum StopWithSavepointState {
        WaitForSavepointCreation {
            @Override
            public StopWithSavepointState onSavepointCreation(
                    StopWithSavepointContext context, String path, Throwable throwable) {
                if (throwable != null) {
                    return context.terminateExceptionally(throwable);
                }
                context.path = path;

                return WaitForJobTermination;
            }
        },
        WaitForJobTermination {
            @Override
            public StopWithSavepointState onExecutionsTermination(
                    StopWithSavepointContext context,
                    Collection<ExecutionState> executionStates,
                    Throwable throwable) {
                if (extractNonFinishedStates(executionStates).isEmpty()) {
                    return context.terminateSuccessfully();
                }

                FlinkException inconsistentFinalStateException =
                        new FlinkException(
                                String.format(
                                        "Inconsistent execution state after stopping with savepoint. A global fail-over was triggered to recover the job %s.",
                                        context.jobId));
                context.scheduler.handleGlobalFailure(inconsistentFinalStateException);
                return context.terminateExceptionally(inconsistentFinalStateException);
            }
        },
        Final;

        public StopWithSavepointState onSavepointCreation(
                StopWithSavepointContext context, String path, Throwable throwable) {
            throw new IllegalStateException(
                    "No onSavepointCreation should have been called in " + this.name() + " state.");
        }

        public StopWithSavepointState onExecutionsTermination(
                StopWithSavepointContext context,
                Collection<ExecutionState> executionStates,
                Throwable throwable) {
            throw new IllegalStateException(
                    "No onExecutionsTermination should have been called in "
                            + this.name()
                            + " state.");
        }
    }
}
