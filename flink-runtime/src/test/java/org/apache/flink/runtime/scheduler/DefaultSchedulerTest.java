/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.hooks.TestMasterHook;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartAllFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestSchedulingStrategy;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.acknowledgePendingCheckpoint;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.enableCheckpointing;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.getCheckpointCoordinator;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link DefaultScheduler}. */
public class DefaultSchedulerTest extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final int TIMEOUT_MS = 1000;

    private static final JobID TEST_JOB_ID = new JobID();

    private ManuallyTriggeredScheduledExecutor taskRestartExecutor =
            new ManuallyTriggeredScheduledExecutor();

    private ExecutorService executor;

    private ScheduledExecutorService scheduledExecutorService;

    private Configuration configuration;

    private TestRestartBackoffTimeStrategy testRestartBackoffTimeStrategy;

    private TestExecutionVertexOperationsDecorator testExecutionVertexOperations;

    private ExecutionVertexVersioner executionVertexVersioner;

    private TestExecutionSlotAllocatorFactory executionSlotAllocatorFactory;

    private TestExecutionSlotAllocator testExecutionSlotAllocator;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadExecutor();
        scheduledExecutorService = new DirectScheduledExecutorService();

        configuration = new Configuration();

        testRestartBackoffTimeStrategy = new TestRestartBackoffTimeStrategy(true, 0);

        testExecutionVertexOperations =
                new TestExecutionVertexOperationsDecorator(new DefaultExecutionVertexOperations());

        executionVertexVersioner = new ExecutionVertexVersioner();

        executionSlotAllocatorFactory = new TestExecutionSlotAllocatorFactory();
        testExecutionSlotAllocator = executionSlotAllocatorFactory.getTestExecutionSlotAllocator();
    }

    @After
    public void tearDown() throws Exception {
        if (scheduledExecutorService != null) {
            ExecutorUtils.gracefulShutdown(
                    TIMEOUT_MS, TimeUnit.MILLISECONDS, scheduledExecutorService);
        }

        if (executor != null) {
            ExecutorUtils.gracefulShutdown(TIMEOUT_MS, TimeUnit.MILLISECONDS, executor);
        }
    }

    @Test
    public void startScheduling() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        createSchedulerAndStartScheduling(jobGraph);

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionVertexOperations.getDeployedVertices();

        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId));
    }

    @Test
    public void deployTasksOnlyWhenAllSlotRequestsAreFulfilled() throws Exception {
        final JobGraph jobGraph = singleJobVertexJobGraph(4);
        final JobVertexID onlyJobVertexId = getOnlyJobVertex(jobGraph).getID();

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        schedulingStrategyFactory);
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
        scheduler.startScheduling();

        final List<ExecutionVertexID> verticesToSchedule =
                Arrays.asList(
                        new ExecutionVertexID(onlyJobVertexId, 0),
                        new ExecutionVertexID(onlyJobVertexId, 1),
                        new ExecutionVertexID(onlyJobVertexId, 2),
                        new ExecutionVertexID(onlyJobVertexId, 3));
        schedulingStrategy.schedule(verticesToSchedule);

        assertThat(testExecutionVertexOperations.getDeployedVertices(), hasSize(0));

        testExecutionSlotAllocator.completePendingRequest(verticesToSchedule.get(0));
        assertThat(testExecutionVertexOperations.getDeployedVertices(), hasSize(0));

        testExecutionSlotAllocator.completePendingRequests();
        assertThat(testExecutionVertexOperations.getDeployedVertices(), hasSize(4));
    }

    @Test
    public void scheduledVertexOrderFromSchedulingStrategyIsRespected() throws Exception {
        final JobGraph jobGraph = singleJobVertexJobGraph(10);
        final JobVertexID onlyJobVertexId = getOnlyJobVertex(jobGraph).getID();

        final List<ExecutionVertexID> desiredScheduleOrder =
                Arrays.asList(
                        new ExecutionVertexID(onlyJobVertexId, 4),
                        new ExecutionVertexID(onlyJobVertexId, 0),
                        new ExecutionVertexID(onlyJobVertexId, 3),
                        new ExecutionVertexID(onlyJobVertexId, 1),
                        new ExecutionVertexID(onlyJobVertexId, 2));

        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        createScheduler(
                jobGraph,
                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                schedulingStrategyFactory);
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();

        schedulingStrategy.schedule(desiredScheduleOrder);

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionVertexOperations.getDeployedVertices();

        assertEquals(desiredScheduleOrder, deployedExecutionVertices);
    }

    @Test
    public void restartAfterDeploymentFails() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        testExecutionVertexOperations.enableFailDeploy();

        createSchedulerAndStartScheduling(jobGraph);

        testExecutionVertexOperations.disableFailDeploy();
        taskRestartExecutor.triggerScheduledTasks();

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionVertexOperations.getDeployedVertices();

        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
    }

    @Test
    public void restartFailedTask() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex archivedExecutionVertex =
                Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                archivedExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

        taskRestartExecutor.triggerScheduledTasks();

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionVertexOperations.getDeployedVertices();
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
    }

    @Test
    public void updateTaskExecutionStateReturnsFalseIfExecutionDoesNotExist() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final TaskExecutionState taskExecutionState =
                new TaskExecutionState(
                        jobGraph.getJobID(), new ExecutionAttemptID(), ExecutionState.FAILED);

        assertFalse(scheduler.updateTaskExecutionState(taskExecutionState));
    }

    @Test
    public void failJobIfCannotRestart() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        testRestartBackoffTimeStrategy.setCanRestart(false);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

        taskRestartExecutor.triggerScheduledTasks();

        waitForTermination(scheduler);
        final JobStatus jobStatus = scheduler.requestJobStatus();
        assertThat(jobStatus, is(equalTo(JobStatus.FAILED)));
    }

    @Test
    public void failJobIfNotEnoughResources() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        testRestartBackoffTimeStrategy.setCanRestart(false);
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        testExecutionSlotAllocator.timeoutPendingRequests();

        waitForTermination(scheduler);
        final JobStatus jobStatus = scheduler.requestJobStatus();
        assertThat(jobStatus, is(equalTo(JobStatus.FAILED)));

        Throwable failureCause =
                scheduler
                        .requestJob()
                        .getFailureInfo()
                        .getException()
                        .deserializeError(DefaultSchedulerTest.class.getClassLoader());
        assertTrue(findThrowable(failureCause, NoResourceAvailableException.class).isPresent());
        assertTrue(
                findThrowableWithMessage(
                                failureCause,
                                "Could not allocate the required slot within slot request timeout.")
                        .isPresent());
        assertThat(jobStatus, is(equalTo(JobStatus.FAILED)));
    }

    @Test
    public void restartVerticesOnSlotAllocationTimeout() throws Exception {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        testRestartVerticesOnFailuresInScheduling(
                vid -> testExecutionSlotAllocator.timeoutPendingRequest(vid));
    }

    @Test
    public void restartVerticesOnAssignedSlotReleased() throws Exception {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        testRestartVerticesOnFailuresInScheduling(
                vid -> {
                    final LogicalSlot slot = testExecutionSlotAllocator.completePendingRequest(vid);
                    slot.releaseSlot(new Exception("Release slot for test"));
                });
    }

    private void testRestartVerticesOnFailuresInScheduling(
            Consumer<ExecutionVertexID> actionsToTriggerTaskFailure) throws Exception {
        final int parallelism = 2;
        final JobVertex v1 = createVertex("vertex1", parallelism);
        final JobVertex v2 = createVertex("vertex2", parallelism);
        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final JobGraph jobGraph = new JobGraph(v1, v2);

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        schedulingStrategyFactory,
                        new RestartPipelinedRegionFailoverStrategy.Factory());
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
        scheduler.startScheduling();

        final ExecutionVertexID vid11 = new ExecutionVertexID(v1.getID(), 0);
        final ExecutionVertexID vid12 = new ExecutionVertexID(v1.getID(), 1);
        final ExecutionVertexID vid21 = new ExecutionVertexID(v2.getID(), 0);
        final ExecutionVertexID vid22 = new ExecutionVertexID(v2.getID(), 1);
        schedulingStrategy.schedule(Arrays.asList(vid11, vid12, vid21, vid22));

        assertThat(testExecutionSlotAllocator.getPendingRequests().keySet(), hasSize(4));

        actionsToTriggerTaskFailure.accept(vid11);

        final Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler.requestJob().getAllExecutionVertices().iterator();
        final ArchivedExecutionVertex ev11 = vertexIterator.next();
        final ArchivedExecutionVertex ev12 = vertexIterator.next();
        final ArchivedExecutionVertex ev21 = vertexIterator.next();
        final ArchivedExecutionVertex ev22 = vertexIterator.next();

        // ev11 and ev21 needs to be restarted because it is pipelined region failover and
        // they are in the same region. ev12 and ev22 will not be affected
        assertThat(testExecutionSlotAllocator.getPendingRequests().keySet(), hasSize(2));
        assertThat(ev11.getExecutionState(), is(ExecutionState.FAILED));
        assertThat(ev21.getExecutionState(), is(ExecutionState.CANCELED));
        assertThat(ev12.getExecutionState(), is(ExecutionState.SCHEDULED));
        assertThat(ev22.getExecutionState(), is(ExecutionState.SCHEDULED));

        taskRestartExecutor.triggerScheduledTasks();
        assertThat(
                schedulingStrategy.getReceivedVerticesToRestart(),
                containsInAnyOrder(vid11, vid21));
    }

    @Test
    public void skipDeploymentIfVertexVersionOutdated() {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final JobGraph jobGraph = nonParallelSourceSinkJobGraph();
        final List<JobVertex> sortedJobVertices =
                jobGraph.getVerticesSortedTopologicallyFromSources();
        final ExecutionVertexID sourceExecutionVertexId =
                new ExecutionVertexID(sortedJobVertices.get(0).getID(), 0);
        final ExecutionVertexID sinkExecutionVertexId =
                new ExecutionVertexID(sortedJobVertices.get(1).getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        testExecutionSlotAllocator.completePendingRequest(sourceExecutionVertexId);

        final ArchivedExecutionVertex sourceExecutionVertex =
                scheduler.requestJob().getAllExecutionVertices().iterator().next();
        final ExecutionAttemptID attemptId =
                sourceExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));
        testRestartBackoffTimeStrategy.setCanRestart(false);

        testExecutionSlotAllocator.enableAutoCompletePendingRequests();
        taskRestartExecutor.triggerScheduledTasks();

        assertThat(
                testExecutionVertexOperations.getDeployedVertices(),
                containsInAnyOrder(sourceExecutionVertexId, sinkExecutionVertexId));
        assertThat(scheduler.requestJob().getState(), is(equalTo(JobStatus.RUNNING)));
    }

    @Test
    public void releaseSlotIfVertexVersionOutdated() {
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(getOnlyJobVertex(jobGraph).getID(), 0);

        createSchedulerAndStartScheduling(jobGraph);

        executionVertexVersioner.recordModification(onlyExecutionVertexId);
        testExecutionSlotAllocator.completePendingRequests();

        assertThat(testExecutionSlotAllocator.getReturnedSlots(), hasSize(1));
    }

    @Test
    public void vertexIsResetBeforeRestarted() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        schedulingStrategyFactory);
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
        final SchedulingTopology topology = schedulingStrategy.getSchedulingTopology();

        scheduler.startScheduling();

        final SchedulingExecutionVertex onlySchedulingVertex =
                Iterables.getOnlyElement(topology.getVertices());
        schedulingStrategy.schedule(Collections.singletonList(onlySchedulingVertex.getId()));

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));

        taskRestartExecutor.triggerScheduledTasks();

        assertThat(schedulingStrategy.getReceivedVerticesToRestart(), hasSize(1));
        assertThat(onlySchedulingVertex.getState(), is(equalTo(ExecutionState.CREATED)));
    }

    @Test
    public void scheduleOnlyIfVertexIsCreated() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        final TestSchedulingStrategy.Factory schedulingStrategyFactory =
                new TestSchedulingStrategy.Factory();
        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        schedulingStrategyFactory);
        final TestSchedulingStrategy schedulingStrategy =
                schedulingStrategyFactory.getLastCreatedSchedulingStrategy();
        final SchedulingTopology topology = schedulingStrategy.getSchedulingTopology();

        scheduler.startScheduling();

        final ExecutionVertexID onlySchedulingVertexId =
                Iterables.getOnlyElement(topology.getVertices()).getId();

        // Schedule the vertex to get it to a non-CREATED state
        schedulingStrategy.schedule(Collections.singletonList(onlySchedulingVertexId));

        // The scheduling of a non-CREATED vertex will result in IllegalStateException
        try {
            schedulingStrategy.schedule(Collections.singletonList(onlySchedulingVertexId));
            fail("IllegalStateException should happen");
        } catch (IllegalStateException e) {
            // expected exception
        }
    }

    @Test
    public void handleGlobalFailure() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        scheduler.handleGlobalFailure(new Exception("forced failure"));

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.CANCELED));

        taskRestartExecutor.triggerScheduledTasks();

        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionVertexOperations.getDeployedVertices();
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
    }

    @Test
    public void vertexIsNotAffectedByOutdatedDeployment() {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler.requestJob().getAllExecutionVertices().iterator();
        final ArchivedExecutionVertex v1 = vertexIterator.next();
        final ArchivedExecutionVertex v2 = vertexIterator.next();

        final SchedulingExecutionVertex sv1 =
                scheduler.getSchedulingTopology().getVertices().iterator().next();

        // fail v1 and let it recover to SCHEDULED
        // the initial deployment of v1 will be outdated
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(),
                        v1.getCurrentExecutionAttempt().getAttemptId(),
                        ExecutionState.FAILED));
        taskRestartExecutor.triggerScheduledTasks();

        // fail v2 to get all pending slot requests in the initial deployments to be done
        // this triggers the outdated deployment of v1
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(),
                        v2.getCurrentExecutionAttempt().getAttemptId(),
                        ExecutionState.FAILED));

        // v1 should not be affected
        assertThat(sv1.getState(), is(equalTo(ExecutionState.SCHEDULED)));
    }

    @Test
    public void abortPendingCheckpointsWhenRestartingTasks() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.RUNNING));

        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

        checkpointCoordinator.triggerCheckpoint(false);
        checkpointTriggeredLatch.await();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints(), is(equalTo(1)));

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));
        taskRestartExecutor.triggerScheduledTasks();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints(), is(equalTo(0)));
    }

    @Test
    public void testStopWithSavepointFailingAfterSavepointCreation() throws Exception {
        // initially, we don't allow any restarts since the first phase (savepoint creation)
        // succeeds without any failures
        testRestartBackoffTimeStrategy.setCanRestart(false);

        final JobGraph jobGraph = createTwoVertexJobGraphWithCheckpointingEnabled();

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        final CountDownLatch checkpointTriggeredLatch =
                getCheckpointTriggeredLatch(taskManagerGateway);

        // collect executions to which the checkpoint completion was confirmed
        final List<ExecutionAttemptID> executionAttemptIdsWithCompletedCheckpoint =
                new ArrayList<>();
        taskManagerGateway.setNotifyCheckpointCompleteConsumer(
                (executionAttemptId, jobId, actualCheckpointId, timestamp) ->
                        executionAttemptIdsWithCompletedCheckpoint.add(executionAttemptId));
        taskManagerGateway.setNotifyCheckpointAbortedConsumer(
                (ignored0, ignored1, ignored2, ignored3) -> {
                    throw new UnsupportedOperationException("notifyCheckpointAborted was called");
                });

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ExecutionAttemptID succeedingExecutionAttemptId =
                Iterables.get(scheduler.getExecutionGraph().getAllExecutionVertices(), 0)
                        .getCurrentExecutionAttempt()
                        .getAttemptId();
        final ExecutionAttemptID failingExecutionAttemptId =
                Iterables.getLast(scheduler.getExecutionGraph().getAllExecutionVertices())
                        .getCurrentExecutionAttempt()
                        .getAttemptId();

        // we have to make sure that the tasks are running before stop-with-savepoint is triggered
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(), failingExecutionAttemptId, ExecutionState.RUNNING));
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(), succeedingExecutionAttemptId, ExecutionState.RUNNING));

        final String savepointFolder = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        // trigger savepoint and wait for checkpoint to be retrieved by TaskManagerGateway
        final CompletableFuture<String> stopWithSavepointFuture =
                scheduler.stopWithSavepoint(savepointFolder, false);
        checkpointTriggeredLatch.await();

        acknowledgePendingCheckpoint(scheduler, 1);

        assertThat(
                "Both the executions where notified about the completed checkpoint.",
                executionAttemptIdsWithCompletedCheckpoint,
                containsInAnyOrder(failingExecutionAttemptId, succeedingExecutionAttemptId));

        // The savepoint creation succeeded a failure happens in the second phase when finishing
        // the tasks. That's why, the restarting policy is enabled.
        testRestartBackoffTimeStrategy.setCanRestart(true);

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(), failingExecutionAttemptId, ExecutionState.FAILED));
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(),
                        succeedingExecutionAttemptId,
                        ExecutionState.FINISHED));

        // the restarts due to local failure handling and global job fail-over are triggered
        assertThat(taskRestartExecutor.getNonPeriodicScheduledTask(), hasSize(2));
        taskRestartExecutor.triggerNonPeriodicScheduledTasks();

        try {
            stopWithSavepointFuture.get();
            fail("An exception is expected.");
        } catch (ExecutionException e) {
            Optional<FlinkException> flinkException =
                    ExceptionUtils.findThrowable(e, FlinkException.class);

            assertTrue(flinkException.isPresent());
            assertThat(
                    flinkException.get().getMessage(),
                    is(
                            String.format(
                                    "Inconsistent execution state after stopping with savepoint. A global fail-over was triggered to recover the job %s.",
                                    jobGraph.getJobID())));
        }

        assertThat(scheduler.getExecutionGraph().getState(), is(JobStatus.RUNNING));
    }

    @Test
    public void testStopWithSavepointFailingWithDeclinedCheckpoint() throws Exception {
        // we allow restarts right from the start since the failure is going to happen in the first
        // phase (savepoint creation) of stop-with-savepoint
        testRestartBackoffTimeStrategy.setCanRestart(true);

        final JobGraph jobGraph = createTwoVertexJobGraphWithCheckpointingEnabled();

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        final CountDownLatch checkpointTriggeredLatch =
                getCheckpointTriggeredLatch(taskManagerGateway);

        // collect executions to which the checkpoint completion was confirmed
        CountDownLatch checkpointAbortionConfirmedLatch = new CountDownLatch(2);
        final List<ExecutionAttemptID> executionAttemptIdsWithAbortedCheckpoint = new ArrayList<>();
        taskManagerGateway.setNotifyCheckpointAbortedConsumer(
                (executionAttemptId, jobId, actualCheckpointId, timestamp) -> {
                    executionAttemptIdsWithAbortedCheckpoint.add(executionAttemptId);
                    checkpointAbortionConfirmedLatch.countDown();
                });
        taskManagerGateway.setNotifyCheckpointCompleteConsumer(
                (ignored0, ignored1, ignored2, ignored3) -> {
                    throw new UnsupportedOperationException("notifyCheckpointCompleted was called");
                });

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ExecutionAttemptID succeedingExecutionAttemptId =
                Iterables.get(scheduler.getExecutionGraph().getAllExecutionVertices(), 0)
                        .getCurrentExecutionAttempt()
                        .getAttemptId();
        final ExecutionAttemptID failingExecutionAttemptId =
                Iterables.getLast(scheduler.getExecutionGraph().getAllExecutionVertices())
                        .getCurrentExecutionAttempt()
                        .getAttemptId();

        // we have to make sure that the tasks are running before stop-with-savepoint is triggered
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(), failingExecutionAttemptId, ExecutionState.RUNNING));
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(), succeedingExecutionAttemptId, ExecutionState.RUNNING));

        final CompletableFuture<String> stopWithSavepointFuture =
                scheduler.stopWithSavepoint("savepoint-path", false);
        checkpointTriggeredLatch.await();

        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

        final AcknowledgeCheckpoint acknowledgeCheckpoint =
                new AcknowledgeCheckpoint(jobGraph.getJobID(), succeedingExecutionAttemptId, 1);
        final DeclineCheckpoint declineCheckpoint =
                new DeclineCheckpoint(
                        jobGraph.getJobID(),
                        failingExecutionAttemptId,
                        1,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED));

        checkpointCoordinator.receiveAcknowledgeMessage(acknowledgeCheckpoint, "unknown location");
        checkpointCoordinator.receiveDeclineMessage(declineCheckpoint, "unknown location");

        // we need to wait for the confirmations to be collected since this is running in a separate
        // thread
        checkpointAbortionConfirmedLatch.await();

        assertThat(
                "Both of the executions where notified about the aborted checkpoint.",
                executionAttemptIdsWithAbortedCheckpoint,
                containsInAnyOrder(succeedingExecutionAttemptId, failingExecutionAttemptId));

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(),
                        succeedingExecutionAttemptId,
                        ExecutionState.FINISHED));
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobGraph.getJobID(), failingExecutionAttemptId, ExecutionState.FAILED));

        // the restart due to failed checkpoint handling triggering a global job fail-over
        assertThat(taskRestartExecutor.getNonPeriodicScheduledTask(), hasSize(1));
        taskRestartExecutor.triggerNonPeriodicScheduledTasks();

        try {
            stopWithSavepointFuture.get();
            fail("An exception is expected.");
        } catch (ExecutionException e) {
            Optional<CheckpointException> actualCheckpointException =
                    findThrowable(e, CheckpointException.class);
            assertTrue(actualCheckpointException.isPresent());
            assertThat(
                    actualCheckpointException.get().getCheckpointFailureReason(),
                    is(CheckpointFailureReason.CHECKPOINT_DECLINED));
        }

        assertThat(scheduler.getExecutionGraph().getState(), is(JobStatus.RUNNING));
    }

    @Test
    public void testStopWithSavepointFailingWithExpiredCheckpoint() throws Exception {
        // we allow restarts right from the start since the failure is going to happen in the first
        // phase (savepoint creation) of stop-with-savepoint
        testRestartBackoffTimeStrategy.setCanRestart(true);

        final JobGraph jobGraph = createTwoVertexJobGraph();
        // set checkpoint timeout to a low value to simulate checkpoint expiration
        enableCheckpointing(jobGraph, 10);

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        final CountDownLatch checkpointTriggeredLatch =
                getCheckpointTriggeredLatch(taskManagerGateway);

        // we have to set a listener that checks for the termination of the checkpoint handling
        OneShotLatch checkpointAbortionWasTriggered = new OneShotLatch();
        taskManagerGateway.setNotifyCheckpointAbortedConsumer(
                (executionAttemptId, jobId, actualCheckpointId, timestamp) ->
                        checkpointAbortionWasTriggered.trigger());

        // the failure handling has to happen in the same thread as the checkpoint coordination -
        // that's why we have to instantiate a separate ThreadExecutorService here
        final ScheduledExecutorService singleThreadExecutorService =
                Executors.newSingleThreadScheduledExecutor();
        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        singleThreadExecutorService);

        final DefaultScheduler scheduler =
                CompletableFuture.supplyAsync(
                                () ->
                                        createSchedulerAndStartScheduling(
                                                jobGraph, mainThreadExecutor),
                                mainThreadExecutor)
                        .get();

        final ExecutionAttemptID succeedingExecutionAttemptId =
                Iterables.get(scheduler.getExecutionGraph().getAllExecutionVertices(), 0)
                        .getCurrentExecutionAttempt()
                        .getAttemptId();
        final ExecutionAttemptID failingExecutionAttemptId =
                Iterables.getLast(scheduler.getExecutionGraph().getAllExecutionVertices())
                        .getCurrentExecutionAttempt()
                        .getAttemptId();

        final CompletableFuture<String> stopWithSavepointFuture =
                CompletableFuture.supplyAsync(
                                () -> {
                                    // we have to make sure that the tasks are running before
                                    // stop-with-savepoint is triggered
                                    scheduler.updateTaskExecutionState(
                                            new TaskExecutionState(
                                                    jobGraph.getJobID(),
                                                    failingExecutionAttemptId,
                                                    ExecutionState.RUNNING));
                                    scheduler.updateTaskExecutionState(
                                            new TaskExecutionState(
                                                    jobGraph.getJobID(),
                                                    succeedingExecutionAttemptId,
                                                    ExecutionState.RUNNING));

                                    return scheduler.stopWithSavepoint("savepoint-path", false);
                                },
                                mainThreadExecutor)
                        .get();

        checkpointTriggeredLatch.await();

        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

        final AcknowledgeCheckpoint acknowledgeCheckpoint =
                new AcknowledgeCheckpoint(jobGraph.getJobID(), succeedingExecutionAttemptId, 1);

        checkpointCoordinator.receiveAcknowledgeMessage(acknowledgeCheckpoint, "unknown location");

        // we need to wait for the expired checkpoint to be handled
        checkpointAbortionWasTriggered.await();

        CompletableFuture.runAsync(
                        () -> {
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            jobGraph.getJobID(),
                                            succeedingExecutionAttemptId,
                                            ExecutionState.FINISHED));
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            jobGraph.getJobID(),
                                            failingExecutionAttemptId,
                                            ExecutionState.FAILED));

                            // the restart due to failed checkpoint handling triggering a global job
                            // fail-over
                            assertThat(
                                    taskRestartExecutor.getNonPeriodicScheduledTask(), hasSize(1));
                            taskRestartExecutor.triggerNonPeriodicScheduledTasks();
                        },
                        mainThreadExecutor)
                .get();

        try {
            stopWithSavepointFuture.get();
            fail("An exception is expected.");
        } catch (ExecutionException e) {
            Optional<CheckpointException> actualCheckpointException =
                    findThrowable(e, CheckpointException.class);
            assertTrue(actualCheckpointException.isPresent());
            assertThat(
                    actualCheckpointException.get().getCheckpointFailureReason(),
                    is(CheckpointFailureReason.CHECKPOINT_EXPIRED));
        }

        // we have to wait for the main executor to be finished with restarting tasks
        singleThreadExecutorService.shutdown();
        singleThreadExecutorService.awaitTermination(1, TimeUnit.SECONDS);

        assertThat(scheduler.getExecutionGraph().getState(), is(JobStatus.RUNNING));
    }

    @Test
    public void testStopWithSavepoint() throws Exception {
        // we don't allow any restarts during the happy path
        testRestartBackoffTimeStrategy.setCanRestart(false);

        final JobGraph jobGraph = createTwoVertexJobGraphWithCheckpointingEnabled();

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        final CountDownLatch checkpointTriggeredLatch =
                getCheckpointTriggeredLatch(taskManagerGateway);

        // collect executions to which the checkpoint completion was confirmed
        final List<ExecutionAttemptID> executionAttemptIdsWithCompletedCheckpoint =
                new ArrayList<>();
        taskManagerGateway.setNotifyCheckpointCompleteConsumer(
                (executionAttemptId, jobId, actualCheckpointId, timestamp) ->
                        executionAttemptIdsWithCompletedCheckpoint.add(executionAttemptId));
        taskManagerGateway.setNotifyCheckpointAbortedConsumer(
                (ignored0, ignored1, ignored2, ignored3) -> {
                    throw new UnsupportedOperationException("notifyCheckpointAborted was called");
                });

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final Set<ExecutionAttemptID> executionAttemptIds =
                StreamSupport.stream(
                                scheduler
                                        .getExecutionGraph()
                                        .getAllExecutionVertices()
                                        .spliterator(),
                                false)
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .map(Execution::getAttemptId)
                        .collect(Collectors.toSet());

        // we have to make sure that the tasks are running before stop-with-savepoint is triggered
        executionAttemptIds.forEach(
                id ->
                        scheduler.updateTaskExecutionState(
                                new TaskExecutionState(
                                        jobGraph.getJobID(), id, ExecutionState.RUNNING)));

        final String savepointFolder = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        // trigger savepoint and wait for checkpoint to be retrieved by TaskManagerGateway
        final CompletableFuture<String> stopWithSavepointFuture =
                scheduler.stopWithSavepoint(savepointFolder, false);
        checkpointTriggeredLatch.await();

        acknowledgePendingCheckpoint(scheduler, 1);

        assertThat(
                "Both the executions where notified about the completed checkpoint.",
                executionAttemptIdsWithCompletedCheckpoint,
                containsInAnyOrder(executionAttemptIds.toArray()));

        executionAttemptIds.forEach(
                id ->
                        scheduler.updateTaskExecutionState(
                                new TaskExecutionState(
                                        jobGraph.getJobID(), id, ExecutionState.FINISHED)));

        assertThat(
                stopWithSavepointFuture.get(),
                startsWith(String.format("file:%s", savepointFolder)));

        assertThat(scheduler.getExecutionGraph().getState(), is(JobStatus.FINISHED));
    }

    private static JobGraph createTwoVertexJobGraph() {
        final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Testjob");

        final JobVertex jobVertex0 = new JobVertex("vertex #0");
        jobVertex0.setInvokableClass(NoOpInvokable.class);
        jobGraph.addVertex(jobVertex0);

        final JobVertex jobVertex1 = new JobVertex("vertex #1");
        jobVertex1.setInvokableClass(NoOpInvokable.class);
        jobGraph.addVertex(jobVertex1);

        return jobGraph;
    }

    private static JobGraph createTwoVertexJobGraphWithCheckpointingEnabled() {
        JobGraph jobGraph = createTwoVertexJobGraph();
        enableCheckpointing(jobGraph);

        return jobGraph;
    }

    @Test
    public void restoreStateWhenRestartingTasks() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.RUNNING));

        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

        // register a stateful master hook to help verify state restore
        final TestMasterHook masterHook = TestMasterHook.fromId("testHook");
        checkpointCoordinator.addMasterHook(masterHook);

        // complete one checkpoint for state restore
        checkpointCoordinator.triggerCheckpoint(false);
        checkpointTriggeredLatch.await();
        final long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();
        acknowledgePendingCheckpoint(scheduler, checkpointId);

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));
        taskRestartExecutor.triggerScheduledTasks();
        assertThat(masterHook.getRestoreCount(), is(equalTo(1)));
    }

    @Test
    public void failGlobalWhenRestoringStateFails() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        enableCheckpointing(jobGraph);

        final CountDownLatch checkpointTriggeredLatch = getCheckpointTriggeredLatch();

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.RUNNING));

        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);

        // register a master hook to fail state restore
        final TestMasterHook masterHook = TestMasterHook.fromId("testHook");
        masterHook.enableFailOnRestore();
        checkpointCoordinator.addMasterHook(masterHook);

        // complete one checkpoint for state restore
        checkpointCoordinator.triggerCheckpoint(false);
        checkpointTriggeredLatch.await();
        final long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();
        acknowledgePendingCheckpoint(scheduler, checkpointId);

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(jobGraph.getJobID(), attemptId, ExecutionState.FAILED));
        taskRestartExecutor.triggerScheduledTasks();
        final List<ExecutionVertexID> deployedExecutionVertices =
                testExecutionVertexOperations.getDeployedVertices();

        // the first task failover should be skipped on state restore failure
        final ExecutionVertexID executionVertexId = new ExecutionVertexID(onlyJobVertex.getID(), 0);
        assertThat(deployedExecutionVertices, contains(executionVertexId));

        // a global failure should be triggered on state restore failure
        masterHook.disableFailOnRestore();
        taskRestartExecutor.triggerScheduledTasks();
        assertThat(deployedExecutionVertices, contains(executionVertexId, executionVertexId));
    }

    @Test
    public void failJobWillIncrementVertexVersions() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.getExecutionVertexVersion(onlyExecutionVertexId);

        scheduler.failJob(new FlinkException("Test failure."));

        assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
    }

    @Test
    public void cancelJobWillIncrementVertexVersions() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.getExecutionVertexVersion(onlyExecutionVertexId);

        scheduler.cancel();

        assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
    }

    @Test
    public void suspendJobWillIncrementVertexVersions() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobVertex onlyJobVertex = getOnlyJobVertex(jobGraph);
        final ExecutionVertexID onlyExecutionVertexId =
                new ExecutionVertexID(onlyJobVertex.getID(), 0);

        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final ExecutionVertexVersion executionVertexVersion =
                executionVertexVersioner.getExecutionVertexVersion(onlyExecutionVertexId);

        scheduler.suspend(new Exception("forced suspend"));

        assertTrue(executionVertexVersioner.isModified(executionVertexVersion));
    }

    @Test
    public void jobStatusIsRestartingIfOneVertexIsWaitingForRestart() {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);
        final JobID jobId = jobGraph.getJobID();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler.requestJob().getAllExecutionVertices().iterator();
        final ExecutionAttemptID attemptId1 =
                vertexIterator.next().getCurrentExecutionAttempt().getAttemptId();
        final ExecutionAttemptID attemptId2 =
                vertexIterator.next().getCurrentExecutionAttempt().getAttemptId();

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobId,
                        attemptId1,
                        ExecutionState.FAILED,
                        new RuntimeException("expected")));
        final JobStatus jobStatusAfterFirstFailure = scheduler.requestJobStatus();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobId,
                        attemptId2,
                        ExecutionState.FAILED,
                        new RuntimeException("expected")));

        taskRestartExecutor.triggerNonPeriodicScheduledTask();
        final JobStatus jobStatusWithPendingRestarts = scheduler.requestJobStatus();
        taskRestartExecutor.triggerNonPeriodicScheduledTask();
        final JobStatus jobStatusAfterRestarts = scheduler.requestJobStatus();

        assertThat(jobStatusAfterFirstFailure, equalTo(JobStatus.RESTARTING));
        assertThat(jobStatusWithPendingRestarts, equalTo(JobStatus.RESTARTING));
        assertThat(jobStatusAfterRestarts, equalTo(JobStatus.RUNNING));
    }

    @Test
    public void cancelWhileRestartingShouldWaitForRunningTasks() {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);
        final JobID jobid = jobGraph.getJobID();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);
        final SchedulingTopology topology = scheduler.getSchedulingTopology();

        final Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler.requestJob().getAllExecutionVertices().iterator();
        final ExecutionAttemptID attemptId1 =
                vertexIterator.next().getCurrentExecutionAttempt().getAttemptId();
        final ExecutionAttemptID attemptId2 =
                vertexIterator.next().getCurrentExecutionAttempt().getAttemptId();
        final ExecutionVertexID executionVertex2 =
                scheduler.getExecutionVertexIdOrThrow(attemptId2);

        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobid,
                        attemptId1,
                        ExecutionState.FAILED,
                        new RuntimeException("expected")));
        scheduler.cancel();
        final ExecutionState vertex2StateAfterCancel =
                topology.getVertex(executionVertex2).getState();
        final JobStatus statusAfterCancelWhileRestarting = scheduler.requestJobStatus();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobid,
                        attemptId2,
                        ExecutionState.CANCELED,
                        new RuntimeException("expected")));

        assertThat(vertex2StateAfterCancel, is(equalTo(ExecutionState.CANCELING)));
        assertThat(statusAfterCancelWhileRestarting, is(equalTo(JobStatus.CANCELLING)));
        assertThat(scheduler.requestJobStatus(), is(equalTo(JobStatus.CANCELED)));
    }

    @Test
    public void failureInfoIsSetAfterTaskFailure() {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final JobID jobId = jobGraph.getJobID();
        final DefaultScheduler scheduler = createSchedulerAndStartScheduling(jobGraph);

        final ArchivedExecutionVertex onlyExecutionVertex =
                Iterables.getOnlyElement(scheduler.requestJob().getAllExecutionVertices());
        final ExecutionAttemptID attemptId =
                onlyExecutionVertex.getCurrentExecutionAttempt().getAttemptId();

        final String exceptionMessage = "expected exception";
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobId,
                        attemptId,
                        ExecutionState.FAILED,
                        new RuntimeException(exceptionMessage)));

        final ErrorInfo failureInfo = scheduler.requestJob().getFailureInfo();
        assertThat(failureInfo, is(notNullValue()));
        assertThat(failureInfo.getExceptionAsString(), containsString(exceptionMessage));
    }

    @Test
    public void allocationIsCanceledWhenVertexIsFailedOrCanceled() throws Exception {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);
        final JobID jobId = jobGraph.getJobID();
        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final DefaultScheduler scheduler =
                createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        new PipelinedRegionSchedulingStrategy.Factory(),
                        new RestartAllFailoverStrategy.Factory());
        scheduler.startScheduling();

        Iterator<ArchivedExecutionVertex> vertexIterator =
                scheduler.requestJob().getAllExecutionVertices().iterator();
        ArchivedExecutionVertex v1 = vertexIterator.next();

        assertThat(testExecutionSlotAllocator.getPendingRequests().keySet(), hasSize(2));

        final String exceptionMessage = "expected exception";
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        jobId,
                        v1.getCurrentExecutionAttempt().getAttemptId(),
                        ExecutionState.FAILED,
                        new RuntimeException(exceptionMessage)));

        vertexIterator = scheduler.requestJob().getAllExecutionVertices().iterator();
        v1 = vertexIterator.next();
        ArchivedExecutionVertex v2 = vertexIterator.next();
        assertThat(v1.getExecutionState(), is(ExecutionState.FAILED));
        assertThat(v2.getExecutionState(), is(ExecutionState.CANCELED));
        assertThat(testExecutionSlotAllocator.getPendingRequests().keySet(), hasSize(0));
    }

    private static JobVertex createVertex(String name, int parallelism) {
        final JobVertex v = new JobVertex(name);
        v.setParallelism(parallelism);
        v.setInvokableClass(AbstractInvokable.class);
        return v;
    }

    private void waitForTermination(final DefaultScheduler scheduler) throws Exception {
        scheduler.getTerminationFuture().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private static JobGraph singleNonParallelJobVertexJobGraph() {
        return singleJobVertexJobGraph(1);
    }

    private static JobGraph singleJobVertexJobGraph(final int parallelism) {
        final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Testjob");
        final JobVertex vertex = new JobVertex("source");
        vertex.setInvokableClass(NoOpInvokable.class);
        vertex.setParallelism(parallelism);
        jobGraph.addVertex(vertex);
        return jobGraph;
    }

    private static JobGraph nonParallelSourceSinkJobGraph() {
        final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Testjob");

        final JobVertex source = new JobVertex("source");
        source.setInvokableClass(NoOpInvokable.class);
        jobGraph.addVertex(source);

        final JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);
        jobGraph.addVertex(sink);

        sink.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        return jobGraph;
    }

    private static JobVertex getOnlyJobVertex(final JobGraph jobGraph) {
        final List<JobVertex> sortedVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
        Preconditions.checkState(sortedVertices.size() == 1);
        return sortedVertices.get(0);
    }

    private DefaultScheduler createSchedulerAndStartScheduling(final JobGraph jobGraph) {
        return createSchedulerAndStartScheduling(
                jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread());
    }

    private DefaultScheduler createSchedulerAndStartScheduling(
            final JobGraph jobGraph, ComponentMainThreadExecutor componentMainThreadExecutor) {
        final SchedulingStrategyFactory schedulingStrategyFactory =
                new PipelinedRegionSchedulingStrategy.Factory();

        try {
            final DefaultScheduler scheduler =
                    createScheduler(
                            jobGraph, componentMainThreadExecutor, schedulingStrategyFactory);
            scheduler.startScheduling();
            return scheduler;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DefaultScheduler createScheduler(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final SchedulingStrategyFactory schedulingStrategyFactory)
            throws Exception {
        return createScheduler(
                jobGraph,
                mainThreadExecutor,
                schedulingStrategyFactory,
                new RestartPipelinedRegionFailoverStrategy.Factory());
    }

    private DefaultScheduler createScheduler(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory)
            throws Exception {
        return SchedulerTestingUtils.newSchedulerBuilder(jobGraph, mainThreadExecutor)
                .setLogger(log)
                .setIoExecutor(executor)
                .setJobMasterConfiguration(configuration)
                .setFutureExecutor(scheduledExecutorService)
                .setDelayExecutor(taskRestartExecutor)
                .setSchedulingStrategyFactory(schedulingStrategyFactory)
                .setFailoverStrategyFactory(failoverStrategyFactory)
                .setRestartBackoffTimeStrategy(testRestartBackoffTimeStrategy)
                .setExecutionVertexOperations(testExecutionVertexOperations)
                .setExecutionVertexVersioner(executionVertexVersioner)
                .setExecutionSlotAllocatorFactory(executionSlotAllocatorFactory)
                .build();
    }

    /**
     * Since checkpoint is triggered asynchronously, we need to figure out when checkpoint is really
     * triggered. Note that this should be invoked before scheduler initialized.
     *
     * @param taskManagerGateway the {@link SimpleAckingTaskManagerGateway} used for the test.
     * @return the latch representing checkpoint is really triggered
     */
    private CountDownLatch getCheckpointTriggeredLatch(
            SimpleAckingTaskManagerGateway taskManagerGateway) {
        final CountDownLatch checkpointTriggeredLatch = new CountDownLatch(1);
        testExecutionSlotAllocator
                .getLogicalSlotBuilder()
                .setTaskManagerGateway(taskManagerGateway);
        taskManagerGateway.setCheckpointConsumer(
                (executionAttemptID,
                        jobId,
                        checkpointId,
                        timestamp,
                        checkpointOptions,
                        advanceToEndOfEventTime) -> checkpointTriggeredLatch.countDown());
        return checkpointTriggeredLatch;
    }

    private CountDownLatch getCheckpointTriggeredLatch() {
        return getCheckpointTriggeredLatch(new SimpleAckingTaskManagerGateway());
    }
}
