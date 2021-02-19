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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * {@code StopWithSavepointOperations} collects the steps of the stop-with-savepoint operation.
 * These steps include:
 *
 * <ol>
 *   <li>Creating a savepoint.
 *   <li>Stopping the job after the savepoint creation was successful.
 * </ol>
 */
public interface StopWithSavepointOperations {

    /**
     * Executes the stop-with-savepoint operation.
     *
     * @param completedSavepointFuture a {@code CompletableFuture} pointing the savepoint creation
     *     result.
     * @param terminatedExecutionsFuture a {@code CompletableFuture} pointing to the {@code
     *     Collection} of terminated states of the corresponding {@link
     *     org.apache.flink.runtime.executiongraph.ExecutionGraph}.
     * @param mainThreadExecutor the executor that should be used for executing the
     *     stop-with-savepoint operation.
     * @return a {@code CompletableFuture} pointing the path of finally created savepoint.
     */
    CompletableFuture<String> stopWithSavepoint(
            CompletableFuture<CompletedCheckpoint> completedSavepointFuture,
            CompletableFuture<Collection<ExecutionState>> terminatedExecutionsFuture,
            ComponentMainThreadExecutor mainThreadExecutor);
}
