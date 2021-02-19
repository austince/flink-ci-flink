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
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TestingCheckpointScheduling;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.QuadConsumer;
import org.apache.flink.util.function.TriConsumer;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@code StopWithSavepointTerminationHandlerImplTest} tests the stop-with-savepoint functionality
 * of {@link SchedulerBase#stopWithSavepoint(String, boolean)}.
 */
public class StopWithSavepointTerminationHandlerImplTest extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final JobID JOB_ID = new JobID();

    private final TestingCheckpointScheduling checkpointScheduling =
            new TestingCheckpointScheduling(false);

    private StopWithSavepointTerminationHandlerImpl createTestInstance(
            Consumer<Throwable> handleGlobalFailureConsumer) {
        // checkpointing should be always stopped before initiating stop-with-savepoint
        checkpointScheduling.stopCheckpointScheduler();

        final SchedulerNG scheduler =
                TestingSchedulerNG.newBuilder()
                        .setHandleGlobalFailureConsumer(handleGlobalFailureConsumer)
                        .build();
        return new StopWithSavepointTerminationHandlerImpl(
                JOB_ID, scheduler, checkpointScheduling, log);
    }

    @Test
    public void testHappyPathWithSavepointCreationBeforeSuccessfulTermination() throws Exception {
        assertHappyPath(
                (completedSavepoint,
                        completedSavepointFuture,
                        terminatedExecutionStates,
                        executionsTerminatedFuture) -> {
                    completedSavepointFuture.complete(completedSavepoint);
                    executionsTerminatedFuture.complete(terminatedExecutionStates);
                });
    }

    @Test
    public void testHappyPathWithSavepointCreationAfterSuccessfulTermination() throws Exception {
        assertHappyPath(
                (completedSavepoint,
                        completedSavepointFuture,
                        terminatedExecutionStates,
                        executionsTerminatedFuture) -> {
                    executionsTerminatedFuture.complete(terminatedExecutionStates);
                    completedSavepointFuture.complete(completedSavepoint);
                });
    }

    @Test
    public void testSavepointCreationFailureBeforeSuccessfulTermination() {
        assertSavepointCreationFailure(
                (expectedException, completedSavepointFuture, executionsTerminatedFuture) -> {
                    completedSavepointFuture.completeExceptionally(expectedException);
                    executionsTerminatedFuture.complete(
                            Collections.singletonList(ExecutionState.FINISHED));
                });
    }

    @Test
    public void testSavepointCreationFailureAfterSuccessfulTermination() {
        assertSavepointCreationFailure(
                (expectedException, completedSavepointFuture, executionsTerminatedFuture) -> {
                    executionsTerminatedFuture.complete(
                            Collections.singletonList(ExecutionState.FINISHED));
                    completedSavepointFuture.completeExceptionally(expectedException);
                });
    }

    @Test
    public void testSavepointCreationFailureBeforeTaskFailure() {
        assertSavepointCreationFailure(
                (expectedException, completedSavepointFuture, executionsTerminatedFuture) -> {
                    completedSavepointFuture.completeExceptionally(expectedException);
                    executionsTerminatedFuture.complete(
                            Collections.singletonList(ExecutionState.FAILED));
                });
    }

    @Test
    public void testSavepointCreationFailureAfterTaskFailure() {
        assertSavepointCreationFailure(
                (expectedException, completedSavepointFuture, executionsTerminatedFuture) -> {
                    executionsTerminatedFuture.complete(
                            Collections.singletonList(ExecutionState.FAILED));
                    completedSavepointFuture.completeExceptionally(expectedException);
                });
    }

    @Test
    public void testNoTerminationHandlingAfterSavepointCompletion() {
        assertNoTerminationHandling(
                (completedSavepoint,
                        completedSavepointFuture,
                        terminatedExecutionStates,
                        executionsTerminatedFuture) -> {
                    completedSavepointFuture.complete(completedSavepoint);
                    executionsTerminatedFuture.complete(terminatedExecutionStates);
                });
    }

    @Test
    public void testNoTerminationHandlingBeforeSavepointCompletion() {
        assertNoTerminationHandling(
                (completedSavepoint,
                        completedSavepointFuture,
                        terminatedExecutionStates,
                        executionsTerminatedFuture) -> {
                    executionsTerminatedFuture.complete(terminatedExecutionStates);
                    completedSavepointFuture.complete(completedSavepoint);
                });
    }

    private void assertHappyPath(
            final QuadConsumer<
                            CompletedCheckpoint,
                            CompletableFuture<CompletedCheckpoint>,
                            Collection<ExecutionState>,
                            CompletableFuture<Collection<ExecutionState>>>
                    completion)
            throws ExecutionException, InterruptedException {
        final StopWithSavepointTerminationHandlerImpl testInstance =
                createTestInstance(
                        throwable -> fail("No global fail-over should have been triggered."));

        final CompletableFuture<CompletedCheckpoint> completedSavepointFuture =
                new CompletableFuture<>();
        final CompletableFuture<Collection<ExecutionState>> executionsTerminatedFuture =
                new CompletableFuture<>();

        final CompletableFuture<String> result =
                testInstance.handlesStopWithSavepointTermination(
                        completedSavepointFuture,
                        executionsTerminatedFuture,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread());

        final String savepointPath = "savepoint-path";
        completion.accept(
                createCompletedSavepoint(savepointPath),
                completedSavepointFuture,
                Collections.singleton(ExecutionState.FINISHED),
                executionsTerminatedFuture);

        assertThat(result.get(), is(savepointPath));

        // the happy path won't restart the checkpoint scheduling
        assertFalse("Checkpoint scheduling should be disabled.", checkpointScheduling.isEnabled());
    }

    private void assertSavepointCreationFailure(
            final TriConsumer<
                            Throwable,
                            CompletableFuture<CompletedCheckpoint>,
                            CompletableFuture<Collection<ExecutionState>>>
                    completion) {
        final StopWithSavepointTerminationHandlerImpl testInstance =
                createTestInstance(throwable -> fail("No global failover should be triggered."));

        final CompletableFuture<CompletedCheckpoint> completedSavepointFuture =
                new CompletableFuture<>();
        final CompletableFuture<Collection<ExecutionState>> executionsTerminatedFuture =
                new CompletableFuture<>();

        final CompletableFuture<String> result =
                testInstance.handlesStopWithSavepointTermination(
                        completedSavepointFuture,
                        executionsTerminatedFuture,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread());

        final String expectedErrorMessage = "Expected exception during savepoint creation.";
        completion.accept(
                new Exception(expectedErrorMessage),
                completedSavepointFuture,
                executionsTerminatedFuture);

        try {
            result.get();
            fail("An ExecutionException is expected.");
        } catch (Throwable e) {
            final Optional<Throwable> actualException =
                    ExceptionUtils.findThrowableWithMessage(e, expectedErrorMessage);
            assertTrue(
                    "An exception with the expected error message should have been thrown.",
                    actualException.isPresent());
        }

        // the checkpoint scheduling should be enabled in case of failure
        assertTrue("Checkpoint scheduling should be enabled.", checkpointScheduling.isEnabled());
    }

    private void assertNoTerminationHandling(
            final QuadConsumer<
                            CompletedCheckpoint,
                            CompletableFuture<CompletedCheckpoint>,
                            Collection<ExecutionState>,
                            CompletableFuture<Collection<ExecutionState>>>
                    completion) {
        final ExecutionState expectedNonFinishedState = ExecutionState.FAILED;
        final String expectedErrorMessage =
                String.format(
                        "Inconsistent execution state after stopping with savepoint. At least one execution is still in one of the following states: %s. A global fail-over is triggered to recover the job %s.",
                        expectedNonFinishedState, JOB_ID);

        final EmptyStreamStateHandle streamStateHandle = new EmptyStreamStateHandle();
        final CompletedCheckpoint completedSavepoint =
                createCompletedSavepoint(streamStateHandle, "savepoint-folder");

        // we have to verify that the handle pointing to the savepoint's metadata is not disposed,
        // yet
        assertFalse(
                "The completed savepoint must not be disposed, yet.",
                streamStateHandle.isDisposed());

        final StopWithSavepointTerminationHandlerImpl testInstance =
                createTestInstance(
                        throwable ->
                                assertThat(
                                        throwable,
                                        FlinkMatchers.containsMessage(expectedErrorMessage)));

        final CompletableFuture<CompletedCheckpoint> completedSavepointFuture =
                new CompletableFuture<>();
        final CompletableFuture<Collection<ExecutionState>> executionsTerminatedFuture =
                new CompletableFuture<>();

        final CompletableFuture<String> result =
                testInstance.handlesStopWithSavepointTermination(
                        completedSavepointFuture,
                        executionsTerminatedFuture,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread());

        completion.accept(
                completedSavepoint,
                completedSavepointFuture,
                Collections.singletonList(expectedNonFinishedState),
                executionsTerminatedFuture);

        try {
            result.get();
            fail("An ExecutionException is expected.");
        } catch (Throwable e) {
            final Optional<FlinkException> actualFlinkException =
                    ExceptionUtils.findThrowable(e, FlinkException.class);
            assertTrue(
                    "A FlinkException should have been thrown.", actualFlinkException.isPresent());
            assertThat(
                    actualFlinkException.get(),
                    FlinkMatchers.containsMessage(expectedErrorMessage));
        }

        // the checkpoint scheduling should be enabled in case of failure
        assertTrue("Checkpoint scheduling should be enabled.", checkpointScheduling.isEnabled());

        assertTrue("The savepoint should be cleaned up.", streamStateHandle.isDisposed());
    }

    private static CompletedCheckpoint createCompletedSavepoint(String path) {
        return createCompletedSavepoint(new EmptyStreamStateHandle(), path);
    }

    private static CompletedCheckpoint createCompletedSavepoint(
            StreamStateHandle metadataHandle, String path) {
        return new CompletedCheckpoint(
                JOB_ID,
                0,
                0L,
                0L,
                new HashMap<>(),
                null,
                new CheckpointProperties(
                        true, CheckpointType.SYNC_SAVEPOINT, false, false, false, false, false),
                new TestCompletedCheckpointStorageLocation(metadataHandle, path));
    }
}
