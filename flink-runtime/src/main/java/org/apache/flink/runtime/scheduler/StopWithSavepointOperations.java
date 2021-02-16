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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * {@code StopWithSavepointOperations} collects the steps for creating a savepoint and waiting for
 * the job to stop.
 */
public interface StopWithSavepointOperations {

    /**
     * Handles the Savepoint creation termination.
     *
     * @param path the path to the newly created savepoint.
     * @param throwable the {@code Throwable} in case of failure.
     */
    void handleSavepointCreation(String path, Throwable throwable);

    /**
     * Handles the job termination.
     *
     * @param executionStates the states of the job's {@link Execution Executions}.
     */
    void handleExecutionTermination(Collection<ExecutionState> executionStates);

    /**
     * Returns the result of the stop-with-savepoint operation.
     *
     * @return a {@code CompletableFuture} containing the final path of the savepoint.
     */
    CompletableFuture<String> getResult();
}
