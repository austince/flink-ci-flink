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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    private JobGraph jobGraph;
    private DefaultScheduler scheduler;

    private StopWithSavepointOperations testInstance;

    @Before
    public void setup() throws Exception {
        jobGraph = new JobGraph();

        final JobVertex jobVertex = new JobVertex("vertex #0");
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobGraph.addVertex(jobVertex);

        // checkpointInterval has to be set to a value lower than Long.MAX_VALUE to enable
        // periodic checkpointing - only then can we enable/disable the CheckpointCoordinator
        SchedulerTestingUtils.enablePeriodicCheckpointing(jobGraph, Long.MAX_VALUE - 1);
        scheduler =
                SchedulerTestingUtils.createSchedulerBuilder(
                                jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .setFutureExecutor(new DirectScheduledExecutorService())
                        .build();
        scheduler.startScheduling();

        // the checkpoint scheduler is stopped before triggering the stop-with-savepoint
        disableCheckpointScheduler();

        testInstance = new StopWithSavepointContext(jobGraph.getJobID(), scheduler, this.log);
    }

    @Test
    public void testHappyPathWithSavepointCreationBeforeTermination() throws Exception {
        assertHappyPath(
                (savepointPath) -> {
                    testInstance.handleSavepointCreation(savepointPath, null);
                    testInstance.handleExecutionTermination(
                            Collections.singletonList(ExecutionState.FINISHED));
                });
    }

    @Test
    public void testHappyPathWithSavepointCreationAfterTermination() throws Exception {
        assertHappyPath(
                (savepointPath) -> {
                    testInstance.handleExecutionTermination(
                            Collections.singletonList(ExecutionState.FINISHED));
                    testInstance.handleSavepointCreation(savepointPath, null);
                });
    }

    private void assertHappyPath(Consumer<String> stopWithSavepointCompletion) throws Exception {
        final String savepointPath = "savepoint-path";

        stopWithSavepointCompletion.accept(savepointPath);

        assertThat(testInstance.getResult().get(), is(savepointPath));

        // the happy path won't restart the CheckpointCoordinator
        assertCheckpointSchedulingDisabled();
    }

    @Test
    public void testSavepointCreationFailure() {
        final Exception exception = new Exception("Expected exception during savepoint creation.");
        testInstance.handleSavepointCreation(null, exception);

        try {
            testInstance.getResult().get();
            fail("An ExecutionException is expected.");
        } catch (Throwable e) {
            final Optional<Throwable> actualException =
                    ExceptionUtils.findThrowableWithMessage(e, exception.getMessage());
            assertTrue(actualException.isPresent());
        }

        assertCheckpointSchedulingEnabled();
    }

    @Test
    public void testNoTerminationHandlingAfterSavepointCompletion() throws Exception {
        assertNoTerminationHandling(
                () -> {
                    testInstance.handleSavepointCreation("savepoint-path", null);
                    testInstance.handleExecutionTermination(
                            // the task failed and was restarted
                            Collections.singletonList(ExecutionState.RUNNING));
                });
    }

    @Test
    public void testNoTerminationHandlingBeforeSavepointCompletion() throws Exception {
        assertNoTerminationHandling(
                () -> {
                    testInstance.handleExecutionTermination(
                            // the task failed and was restarted
                            Collections.singletonList(ExecutionState.RUNNING));
                    testInstance.handleSavepointCreation("savepoint-path", null);
                });
    }

    private void assertNoTerminationHandling(Runnable stopWithSavepointCompletion)
            throws Exception {
        final ManuallyTriggeredScheduledExecutor restartExecutor =
                new ManuallyTriggeredScheduledExecutor();
        scheduler =
                SchedulerTestingUtils.createSchedulerBuilder(
                                jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        // we're expecting a global fail-over and,
                        // therefore, have to enable restarting
                        .setRestartBackoffTimeStrategy(new TestRestartBackoffTimeStrategy(true, 0))
                        .setDelayExecutor(restartExecutor)
                        .setFutureExecutor(new DirectScheduledExecutorService())
                        .build();
        scheduler.startScheduling();

        testInstance = new StopWithSavepointContext(jobGraph.getJobID(), scheduler, log);

        disableCheckpointScheduler();

        stopWithSavepointCompletion.run();

        // the task gets cancelled before triggering the restart
        ExecutionAttemptID executionAttemptID =
                scheduler
                        .getExecutionGraph()
                        .getAllExecutionVertices()
                        .iterator()
                        .next()
                        .getCurrentExecutionAttempt()
                        .getAttemptId();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(), executionAttemptID, ExecutionState.CANCELED));

        restartExecutor.triggerScheduledTasks();

        try {
            testInstance.getResult().get();
            fail("An ExecutionException is expected.");
        } catch (Throwable e) {
            final Optional<FlinkException> expectedFlinkException =
                    ExceptionUtils.findThrowable(e, FlinkException.class);
            final String expectedMessage =
                    String.format(
                            "Inconsistent execution state after stopping with savepoint. At least one execution is still in one of the following states: RUNNING. A global fail-over is triggered to recover the job %s.",
                            jobGraph.getJobID());
            assertTrue(expectedFlinkException.isPresent());
            assertThat(
                    expectedFlinkException.get(), FlinkMatchers.containsMessage(expectedMessage));
        }

        // the global fail-over puts all tasks into DEPLOYING state again
        assertExecutionStates(scheduler, ExecutionState.DEPLOYING);

        // the CheckpointCoordinator should be enabled again
        assertCheckpointSchedulingEnabled();
    }

    private void disableCheckpointScheduler() {
        scheduler.getCheckpointCoordinator().stopCheckpointScheduler();
    }

    private void assertCheckpointSchedulingEnabled() {
        assertTrue(scheduler.getCheckpointCoordinator().isCurrentPeriodicTriggerAvailable());
    }

    private void assertCheckpointSchedulingDisabled() {
        assertFalse(scheduler.getCheckpointCoordinator().isCurrentPeriodicTriggerAvailable());
    }

    private static void assertExecutionStates(
            SchedulerBase scheduler, ExecutionState expectedExecutionState) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () ->
                        StreamSupport.stream(
                                        scheduler
                                                .getExecutionGraph()
                                                .getAllExecutionVertices()
                                                .spliterator(),
                                        false)
                                .map(ExecutionVertex::getCurrentExecutionAttempt)
                                .map(Execution::getState)
                                .collect(Collectors.toSet())
                                .equals(Sets.newHashSet(expectedExecutionState)),
                Deadline.fromNow(Duration.ofSeconds(1)));
    }
}
