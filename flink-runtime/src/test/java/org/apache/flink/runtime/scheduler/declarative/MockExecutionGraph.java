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

import org.apache.flink.runtime.executiongraph.MockExecutionGraphBase;

import java.util.concurrent.CompletableFuture;

/**
 * Mocked ExecutionGraph with the following properties:
 *
 * <ul>
 *   <li>it stays in CANCELLING, when cancel() gets called
 *   <li>it stays in FAILING then failJob() gets called
 *   <li>it leaves above states when completeCancellation() gets called.
 * </ul>
 */
class MockExecutionGraph extends MockExecutionGraphBase {

    private final CompletableFuture<?> completeCancellationFuture = new CompletableFuture<>();
    private boolean isCancelling = false;
    private boolean isFailing = false;

    void completeCancellation() {
        completeCancellationFuture.complete(null);
    }

    public boolean isCancelling() {
        return isCancelling;
    }

    public boolean isFailing() {
        return isFailing;
    }

    // overwrites for the tests
    @Override
    public void cancel() {
        super.cancel();
        this.isCancelling = true;
    }

    @Override
    public void failJob(Throwable cause) {
        super.failJob(cause);
        this.isFailing = true;
    }
}
