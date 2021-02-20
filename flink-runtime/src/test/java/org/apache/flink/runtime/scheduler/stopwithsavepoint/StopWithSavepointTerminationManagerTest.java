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

package org.apache.flink.runtime.scheduler.stopwithsavepoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * {@code StopWithSavepointTerminationManagerTest} tests that {@link
 * StopWithSavepointTerminationManager} applies the correct order expected by {@link
 * StopWithSavepointTerminationHandler} regardless of the completion of the provided {@code
 * CompletableFutures}.
 */
public class StopWithSavepointTerminationManagerTest extends TestLogger {

    @Test
    public void testHappyPathInCorrectOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    completedSavepointFuture.complete(createCompletedSavepoint());
                    terminatedExecutionStatesFuture.complete(ExecutionState.FINISHED);
                });
    }

    @Test
    public void testHappyPathInInverseOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    terminatedExecutionStatesFuture.complete(ExecutionState.FINISHED);
                    completedSavepointFuture.complete(createCompletedSavepoint());
                });
    }

    @Test
    public void testSavepointFailureWithFinishedExecutionInCorrectOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    completedSavepointFuture.completeExceptionally(createSavepointFailure());
                    terminatedExecutionStatesFuture.complete(ExecutionState.FINISHED);
                });
    }

    @Test
    public void testSavepointFailureWithFinishedExecutionInInverseOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    terminatedExecutionStatesFuture.complete(ExecutionState.FINISHED);
                    completedSavepointFuture.completeExceptionally(createSavepointFailure());
                });
    }

    @Test
    public void testSavepointFailureWithFailedExecutionInCorrectOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    completedSavepointFuture.completeExceptionally(createSavepointFailure());
                    terminatedExecutionStatesFuture.complete(ExecutionState.FAILED);
                });
    }

    @Test
    public void testSavepointFailureWithFailedExecutionInInverseOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    terminatedExecutionStatesFuture.complete(ExecutionState.FAILED);
                    completedSavepointFuture.completeExceptionally(createSavepointFailure());
                });
    }

    @Test
    public void testTerminationFailureInCorrectOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    completedSavepointFuture.complete(createCompletedSavepoint());
                    terminatedExecutionStatesFuture.complete(ExecutionState.FAILED);
                });
    }

    @Test
    public void testTerminationFailureInInverseOrder() {
        assertCorrectOrderOfProcessing(
                (completedSavepointFuture, terminatedExecutionStatesFuture) -> {
                    terminatedExecutionStatesFuture.complete(ExecutionState.FAILED);
                    completedSavepointFuture.complete(createCompletedSavepoint());
                });
    }

    private void assertCorrectOrderOfProcessing(
            BiConsumer<CompletableFuture<CompletedCheckpoint>, CompletableFuture<ExecutionState>>
                    completion) {
        final CompletableFuture<CompletedCheckpoint> completedSavepointFuture =
                new CompletableFuture<>();
        final CompletableFuture<ExecutionState> terminatedExecutionStateFuture =
                new CompletableFuture<>();

        final TestingStopWithSavepointTerminationHandler stopWithSavepointTerminationHandler =
                new TestingStopWithSavepointTerminationHandler();
        new StopWithSavepointTerminationManager(stopWithSavepointTerminationHandler)
                .stopWithSavepoint(
                        completedSavepointFuture,
                        terminatedExecutionStateFuture.thenApply(Collections::singleton),
                        ComponentMainThreadExecutorServiceAdapter.forMainThread());
        completion.accept(completedSavepointFuture, terminatedExecutionStateFuture);

        stopWithSavepointTerminationHandler.assertSavepointCompletionTriggeredOnce();
        stopWithSavepointTerminationHandler.assertTerminationCompletionTriggeredOnce();
    }

    private static CompletedCheckpoint createCompletedSavepoint() {
        return new CompletedCheckpoint(
                new JobID(),
                0,
                0L,
                0L,
                new HashMap<>(),
                null,
                CheckpointProperties.SYNC_SAVEPOINT,
                new TestCompletedCheckpointStorageLocation(
                        new EmptyStreamStateHandle(), "savepoint-path"));
    }

    private static Throwable createSavepointFailure() {
        return new Exception("Expected exception during savepoint creation.");
    }

    private static class TestingStopWithSavepointTerminationHandler
            implements StopWithSavepointTerminationHandler {

        private int savepointCompletedCallCount = 0;
        private int terminationCompletedCallCount = 0;

        @Override
        public CompletableFuture<String> getSavepointPath() {
            return FutureUtils.completedExceptionally(
                    new Exception("The result is not relevant in this test."));
        }

        @Override
        public void handleSavepointCreation(
                CompletedCheckpoint completedSavepoint, Throwable throwable) {
            savepointCompletedCallCount++;
        }

        @Override
        public void handleExecutionsTermination(
                Collection<ExecutionState> terminatedExecutionStates) {
            terminationCompletedCallCount++;
        }

        private void assertSavepointCompletionTriggeredOnce() {
            assertThat(savepointCompletedCallCount, is(1));
        }

        public void assertTerminationCompletionTriggeredOnce() {
            assertThat(terminationCompletedCallCount, is(1));
        }
    }
}
