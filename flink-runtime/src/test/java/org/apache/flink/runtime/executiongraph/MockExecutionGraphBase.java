/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.types.Either;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Base class for mocking the ExecutionGraph. */
public class MockExecutionGraphBase implements ExecutionGraph {

    @Override
    public String getJsonPlan() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobID getJobID() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getJobName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobStatus getState() {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public ErrorInfo getFailureInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getStatusTimestamp(JobStatus status) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public CheckpointStatsSnapshot getCheckpointStatsSnapshot() {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public ArchivedExecutionConfig getArchivedExecutionConfig() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isStoppable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public StringifiedAccumulatorResult[] getAccumulatorResultsStringified() {
        return new StringifiedAccumulatorResult[0];
    }

    @Override
    public Map<String, SerializedValue<OptionalFailure<Object>>> getAccumulatorsSerialized() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isArchived() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<String> getStateBackendName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<String> getCheckpointStorageName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberOfExecutionJobVertices() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchedulingTopology getSchedulingTopology() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScheduleMode getScheduleMode() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public ComponentMainThreadExecutor getJobMasterMainThreadExecutor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void enableCheckpointing(
            CheckpointCoordinatorConfiguration chkConfig,
            List<MasterTriggerRestoreHook<?>> masterHooks,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore checkpointStore,
            StateBackend checkpointStateBackend,
            CheckpointStorage checkpointStorage,
            CheckpointStatsTracker statsTracker,
            CheckpointsCleaner checkpointsCleaner) {
        throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public CheckpointCoordinator getCheckpointCoordinator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public KvStateLocationRegistry getKvStateLocationRegistry() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJsonPlan(String jsonPlan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Either<SerializedValue<JobInformation>, PermanentBlobKey> getJobInformationOrBlobKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Configuration getJobConfiguration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClassLoader getUserClassLoader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Throwable getFailureCause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<ExecutionJobVertex> getVerticesTopologically() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<ExecutionVertex> getAllExecutionVertices() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecutionJobVertex getJobVertex(JobVertexID id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<JobVertexID, ExecutionJobVertex> getAllVertices() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberOfRestarts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTotalNumberOfVertices() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults() {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlobWriter getBlobWriter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Executor getFutureExecutor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, OptionalFailure<Accumulator<?, ?>>> aggregateUserAccumulators() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setInternalTaskFailuresListener(
            InternalFailuresListener internalTaskFailuresListener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void attachJobGraph(List<JobVertex> topologiallySorted) throws JobException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void transitionToRunning() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void suspend(Throwable suspensionCause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failGlobal(Throwable t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<JobStatus> getTerminationFuture() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobStatus waitUntilTerminal() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean transitionState(JobStatus current, JobStatus newState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void incrementRestarts() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initFailureCause(Throwable t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void vertexFinished() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void vertexUnFinished() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failJob(Throwable cause) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean updateState(TaskExecutionStateTransition state) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyPartitionDataAvailable(ResultPartitionID partitionId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<ExecutionAttemptID, Execution> getRegisteredExecutions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerJobStatusListener(JobStatusListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void assertRunningInJobMasterMainThread() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifySchedulerNgAboutInternalTaskFailure(
            ExecutionAttemptID attemptId,
            Throwable t,
            boolean cancelTask,
            boolean releasePartitions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShuffleMaster<?> getShuffleMaster() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobMasterPartitionTracker getPartitionTracker() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionReleaseStrategy getPartitionReleaseStrategy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExecutionDeploymentListener getExecutionDeploymentListener() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerExecution(Execution exec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deregisterExecution(Execution exec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyExecutionChange(Execution execution, ExecutionState newExecutionState) {
        throw new UnsupportedOperationException();
    }
}
