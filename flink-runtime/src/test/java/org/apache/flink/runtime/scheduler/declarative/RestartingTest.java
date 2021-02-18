/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;
import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.declarative.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link Restarting} state of the declarative scheduler. */
public class RestartingTest extends TestLogger {

    @Test
    public void testExecutionGraphCancellationOnEnter() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            Restarting restarting = createRestartingState(ctx, mockExecutionGraph);

            restarting.onEnter();
            assertThat(mockExecutionGraph.getState(), is(JobStatus.CANCELLING));
        }
    }

    @Test
    public void testTransitionToWaitingForResourcesWhenCancellationComplete() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            ctx.setExpectWaitingForResources();
            restarting.onGloballyTerminalState(JobStatus.CANCELED);
        }
    }

    @Test
    public void testCancel() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            ctx.setExpectCancelling(assertNonNull());
            restarting.cancel();
        }
    }

    @Test
    public void testSuspend() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED)));
            final Throwable cause = new RuntimeException("suspend");
            restarting.suspend(cause);
        }
    }

    @Test
    public void testGlobalFailuresAreIgnored() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            restarting.handleGlobalFailure(new RuntimeException());
            ctx.assertNoStateTransition();
        }
    }

    public Restarting createRestartingState(
            MockRestartingContext ctx, ExecutionGraph executionGraph) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new OperatorCoordinatorHandler(
                        executionGraph,
                        (throwable) -> {
                            throw new RuntimeException("Error in test", throwable);
                        });
        executionGraph.transitionToRunning();
        return new Restarting(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                Duration.ZERO);
    }

    public Restarting createRestartingState(MockRestartingContext ctx)
            throws JobException, JobExecutionException {
        ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder().build();
        return createRestartingState(ctx, executionGraph);
    }

    private static class MockRestartingContext extends MockStateWithExecutionGraphContext
            implements Restarting.Context {

        private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
                new StateValidator<>("Cancelling");

        private final StateValidator<Void> waitingForResourcesStateValidator =
                new StateValidator<>("WaitingForResources");

        public void setExpectCancelling(Consumer<ExecutingTest.CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        public void setExpectWaitingForResources() {
            waitingForResourcesStateValidator.expectInput((none) -> {});
        }

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
            cancellingStateValidator.validateInput(
                    new ExecutingTest.CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
            hadStateTransition = true;
        }

        @Override
        public void goToWaitingForResources() {
            waitingForResourcesStateValidator.validateInput(null);
            hadStateTransition = true;
        }

        @Override
        public void runIfState(State expectedState, Runnable action, Duration delay) {
            if (!hadStateTransition) {
                action.run();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            cancellingStateValidator.close();
            waitingForResourcesStateValidator.close();
        }
    }
}
