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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriConsumer;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@code StopWithSavepointOperationsImplTest} tests the stop-with-savepoint functionality of {@link
 * SchedulerBase#stopWithSavepoint(String, boolean)}.
 */
public class StopWithSavepointOperationsImplTest extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final JobID JOB_ID = new JobID();

    private final TestingCheckpointScheduling checkpointScheduling =
            new TestingCheckpointScheduling(false);

    private StopWithSavepointOperationsImpl createTestInstance(
            Consumer<Throwable> handleGlobalFailureConsumer) {
        // checkpointing should be always stopped before initiating stop-with-savepoint
        checkpointScheduling.stopCheckpointScheduler();

        final SchedulerNG scheduler =
                TestingSchedulerNG.newBuilder()
                        .setHandleGlobalFailureConsumer(handleGlobalFailureConsumer)
                        .build();
        return new StopWithSavepointOperationsImpl(JOB_ID, scheduler, checkpointScheduling, log);
    }

    @Test
    public void testHappyPathWithSavepointCreationBeforeSuccessfulTermination() throws Exception {
        assertHappyPath(
                (testInstance, savepointPath) -> {
                    testInstance.handleSavepointCreation(createCompletedSavepoint(savepointPath));
                    testInstance.handleExecutionTermination(
                            Collections.singletonList(ExecutionState.FINISHED));
                });
    }

    @Test
    public void testHappyPathWithSavepointCreationAfterSuccessfulTermination() throws Exception {
        assertHappyPath(
                (testInstance, savepointPath) -> {
                    testInstance.handleExecutionTermination(
                            Collections.singletonList(ExecutionState.FINISHED));
                    testInstance.handleSavepointCreation(createCompletedSavepoint(savepointPath));
                });
    }

    @Test
    public void testSavepointCreationFailureBeforeSuccessfulTermination() {
        assertSavepointCreationFailure(
                (testInstance, expectedFailure) -> {
                    testInstance.handleSavepointCreationFailure(expectedFailure);
                    testInstance.handleExecutionTermination(
                            Collections.singletonList(ExecutionState.FINISHED));
                });
    }

    @Test
    public void testSavepointCreationFailureAfterSuccessfulTermination() {
        assertSavepointCreationFailure(
                (testInstance, expectedFailure) -> {
                    testInstance.handleExecutionTermination(
                            Collections.singletonList(ExecutionState.FINISHED));
                    testInstance.handleSavepointCreationFailure(expectedFailure);
                });
    }

    @Test
    public void testSavepointCreationFailureBeforeTaskFailure() {
        assertSavepointCreationFailure(
                (testInstance, expectedFailure) -> {
                    testInstance.handleSavepointCreationFailure(expectedFailure);
                    testInstance.handleExecutionTermination(
                            Collections.singletonList(ExecutionState.FAILED));
                });
    }

    @Test
    public void testSavepointCreationFailureAfterTaskFailure() {
        assertSavepointCreationFailure(
                (testInstance, expectedFailure) -> {
                    testInstance.handleExecutionTermination(
                            Collections.singletonList(ExecutionState.FAILED));
                    testInstance.handleSavepointCreationFailure(expectedFailure);
                });
    }

    @Test
    public void testNoTerminationHandlingAfterSavepointCompletion() {
        assertNoTerminationHandling(
                (testInstance, completedSavepoint, expectedUnfinishedExecutionState) -> {
                    testInstance.handleSavepointCreation(completedSavepoint);
                    testInstance.handleExecutionTermination(
                            // the task failed and was restarted
                            Collections.singletonList(expectedUnfinishedExecutionState));
                });
    }

    @Test
    public void testNoTerminationHandlingBeforeSavepointCompletion() {
        assertNoTerminationHandling(
                (testInstance, completedSavepoint, expectedUnfinishedExecutionState) -> {
                    testInstance.handleExecutionTermination(
                            // the task failed and was restarted
                            Collections.singletonList(expectedUnfinishedExecutionState));
                    testInstance.handleSavepointCreation(completedSavepoint);
                });
    }

    private void assertHappyPath(
            BiConsumer<StopWithSavepointOperationsImpl, String> stopWithSavepointCompletion)
            throws Exception {
        final StopWithSavepointOperationsImpl testInstance =
                createTestInstance(
                        throwable -> fail("No global failover should have been triggered."));

        final String savepointPath = "savepoint-path";

        // trigger the completion of the stop-with-savepoint operations
        stopWithSavepointCompletion.accept(testInstance, savepointPath);

        assertThat(testInstance.getResult().get(), is(savepointPath));

        // the happy path won't restart the CheckpointCoordinator
        assertFalse("Checkpoint scheduling should be disabled.", checkpointScheduling.isEnabled());
    }

    private void assertSavepointCreationFailure(
            BiConsumer<StopWithSavepointOperationsImpl, Throwable> stopWithSavepointCompletion) {
        final Exception exception = new Exception("Expected exception during savepoint creation.");

        final StopWithSavepointOperationsImpl testInstance =
                createTestInstance(throwable -> fail("No global failover should be triggered."));
        stopWithSavepointCompletion.accept(testInstance, exception);

        try {
            testInstance.getResult().get();
            fail("An ExecutionException is expected.");
        } catch (Throwable e) {
            final Optional<Throwable> actualException =
                    ExceptionUtils.findThrowableWithMessage(e, exception.getMessage());
            assertTrue(
                    "An exception with the expected error message should have been thrown.",
                    actualException.isPresent());
        }

        // the checkpoint scheduling should be enabled in case of failure
        assertTrue("Checkpoint scheduling should be enabled.", checkpointScheduling.isEnabled());
    }

    private void assertNoTerminationHandling(
            TriConsumer<StopWithSavepointOperationsImpl, CompletedCheckpoint, ExecutionState>
                    stopWithSavepointCompletion) {
        final ExecutionState expectedNonFinishedState = ExecutionState.FAILED;
        final String expectedErrorMessage =
                String.format(
                        "Inconsistent execution state after stopping with savepoint. At least one execution is still in one of the following states: %s. A global fail-over is triggered to recover the job %s.",
                        expectedNonFinishedState, JOB_ID);

        final EmptyStreamStateHandle streamStateHandle = new EmptyStreamStateHandle();
        final CompletedCheckpoint completedSavepoint =
                createCompletedSavepoint(streamStateHandle, "savepoint-folder");

        // we have to verify that the handle points to the savepoint's metadata is not disposed, yet
        assertFalse(
                "The completed savepoint must not be disposed, yet.",
                streamStateHandle.isDisposed());

        final StopWithSavepointOperationsImpl testInstance =
                createTestInstance(
                        throwable ->
                                assertThat(
                                        throwable,
                                        FlinkMatchers.containsMessage(expectedErrorMessage)));
        stopWithSavepointCompletion.accept(
                testInstance, completedSavepoint, expectedNonFinishedState);

        try {
            testInstance.getResult().get();
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
