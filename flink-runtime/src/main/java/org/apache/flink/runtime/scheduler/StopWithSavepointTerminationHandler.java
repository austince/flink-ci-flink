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

import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.execution.ExecutionState;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * {@code StopWithSavepointTerminationHandler} handles the termination of the steps needed for the
 * stop-with-savepoint operation. This includes:
 *
 * <ol>
 *   <li>Creating a savepoint
 *   <li>Waiting for the executions of the underlying job to finish
 * </ol>
 */
public interface StopWithSavepointTerminationHandler {

    CompletableFuture<CompletedCheckpoint> getCompletedSavepoint();

    void handleSavepointCreation(CompletedCheckpoint completedCheckpoint);

    void handleSavepointCreationFailure(Throwable throwable);

    void handleExecutionsTermination(Collection<ExecutionState> executionStates);
}
