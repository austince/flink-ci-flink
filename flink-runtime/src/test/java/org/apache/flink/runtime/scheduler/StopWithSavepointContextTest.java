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
import org.apache.flink.runtime.checkpoint.TestingCheckpointScheduling;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@code StopWithSavepointContextTest} tests the stop-with-savepoint functionality of {@link
 * SchedulerBase#stopWithSavepoint(String, boolean)}.
 */
public class StopWithSavepointContextTest extends TestLogger {

    private final JobID jobId = new JobID();
    private final TestingCheckpointScheduling checkpointScheduling =
            new TestingCheckpointScheduling(false);

    private StopWithSavepointContext createTestInstance(
            Consumer<Throwable> handleGlobalFailureConsumer) {
        // checkpointing should be always stopped before initiating stop-with-savepoint
        checkpointScheduling.stopCheckpointScheduler();

        final SchedulerNG scheduler =
                TestingSchedulerNG.newBuilder()
                        .setHandleGlobalFailureConsumer(handleGlobalFailureConsumer)
                        .build();
        return new StopWithSavepointContext(jobId, scheduler, checkpointScheduling, log);
    }

    @Test
    public void testHappyPathWithSavepointCreationBeforeSuccessfulTermination() throws Exception {
        assertHappyPath(
                (testInstance, savepointPath) -> {
                    testInstance.handleSavepointCreation(savepointPath, null);
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
                    testInstance.handleSavepointCreation(savepointPath, null);
                });
    }

    @Test
    public void testSavepointCreationFailureBeforeSuccessfulTermination() {
        assertSavepointCreationFailure(
                (testInstance, expectedFailure) -> {
                    testInstance.handleSavepointCreation(null, expectedFailure);
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
                    testInstance.handleSavepointCreation(null, expectedFailure);
                });
    }

    @Test
    public void testSavepointCreationFailureBeforeTaskFailure() {
        assertSavepointCreationFailure(
                (testInstance, expectedFailure) -> {
                    testInstance.handleSavepointCreation(null, expectedFailure);
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
                    testInstance.handleSavepointCreation(null, expectedFailure);
                });
    }

    @Test
    public void testNoTerminationHandlingAfterSavepointCompletion() {
        assertNoTerminationHandling(
                (testInstance, expectedUnfinishedExecutionState) -> {
                    testInstance.handleSavepointCreation("savepoint-path", null);
                    testInstance.handleExecutionTermination(
                            // the task failed and was restarted
                            Collections.singletonList(expectedUnfinishedExecutionState));
                });
    }

    @Test
    public void testNoTerminationHandlingBeforeSavepointCompletion() {
        assertNoTerminationHandling(
                (testInstance, expectedUnfinishedExecutionState) -> {
                    testInstance.handleExecutionTermination(
                            // the task failed and was restarted
                            Collections.singletonList(expectedUnfinishedExecutionState));
                    testInstance.handleSavepointCreation("savepoint-path", null);
                });
    }

    private void assertHappyPath(
            BiConsumer<StopWithSavepointContext, String> stopWithSavepointCompletion)
            throws Exception {
        final StopWithSavepointContext testInstance =
                createTestInstance(
                        (throwable) -> fail("No global failover should have been triggered."));

        final String savepointPath = "savepoint-path";

        // trigger the completion of the stop-with-savepoint operations
        stopWithSavepointCompletion.accept(testInstance, savepointPath);

        assertThat(testInstance.getResult().get(), is(savepointPath));

        // the happy path won't restart the CheckpointCoordinator
        assertFalse(checkpointScheduling.isEnabled());
    }

    private void assertSavepointCreationFailure(
            BiConsumer<StopWithSavepointContext, Throwable> stopWithSavepointCompletion) {
        final Exception exception = new Exception("Expected exception during savepoint creation.");

        final StopWithSavepointContext testInstance =
                createTestInstance(throwable -> fail("No global failover should be triggered."));
        stopWithSavepointCompletion.accept(testInstance, exception);

        try {
            testInstance.getResult().get();
            fail("An ExecutionException is expected.");
        } catch (Throwable e) {
            final Optional<Throwable> actualException =
                    ExceptionUtils.findThrowableWithMessage(e, exception.getMessage());
            assertTrue(actualException.isPresent());
        }

        // the checkpoint scheduling should be enabled in case of failure
        assertTrue(checkpointScheduling.isEnabled());
    }

    private void assertNoTerminationHandling(
            BiConsumer<StopWithSavepointContext, ExecutionState> stopWithSavepointCompletion) {
        final ExecutionState expectedNonFinishedState = ExecutionState.FAILED;
        final String expectedErrorMessage =
                String.format(
                        "Inconsistent execution state after stopping with savepoint. At least one execution is still in one of the following states: %s. A global fail-over is triggered to recover the job %s.",
                        expectedNonFinishedState, jobId);

        final StopWithSavepointContext testInstance =
                createTestInstance(
                        throwable ->
                                assertThat(
                                        throwable,
                                        FlinkMatchers.containsMessage(expectedErrorMessage)));
        stopWithSavepointCompletion.accept(testInstance, expectedNonFinishedState);

        try {
            testInstance.getResult().get();
            fail("An ExecutionException is expected.");
        } catch (Throwable e) {
            final Optional<FlinkException> actualFlinkException =
                    ExceptionUtils.findThrowable(e, FlinkException.class);
            assertTrue(actualFlinkException.isPresent());
            assertThat(
                    actualFlinkException.get(),
                    FlinkMatchers.containsMessage(expectedErrorMessage));
        }

        // the checkpoint scheduling should be enabled in case of failure
        assertTrue(checkpointScheduling.isEnabled());
    }
}
