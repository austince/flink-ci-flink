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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.stats.OperatorStatsTracker;
import org.apache.flink.runtime.webmonitor.stats.Stats;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Tracker of thread infos for {@link ExecutionJobVertex}.
 *
 * @param <T> Type of the derived statistics to return.
 */
public class ThreadInfoOperatorTracker<T extends Stats> implements OperatorStatsTracker<T> {

    /**
     * Create a new {@link Builder}.
     *
     * @param createStatsFn Function that converts a thread info sample into a derived statistic.
     *     Could be an identity function.
     * @param <T> Type of the derived statistics to return.
     * @return Builder.
     */
    public static <T extends Stats> Builder<T> newBuilder(
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            Function<OperatorThreadInfoStats, T> createStatsFn,
            ExecutorService executor) {
        return new Builder<>(resourceManagerGatewayRetriever, createStatsFn, executor);
    }

    /**
     * Builder for {@link ThreadInfoOperatorTracker}.
     *
     * @param <T> Type of the derived statistics to return.
     */
    public static class Builder<T extends Stats> {

        private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
        private final Function<OperatorThreadInfoStats, T> createStatsFn;
        private final ExecutorService executor;

        private ThreadInfoRequestCoordinator coordinator;
        private int cleanUpInterval;
        private int numSamples;
        private int statsRefreshInterval;
        private Time delayBetweenSamples;
        private int maxThreadInfoDepth;

        private Builder(
                GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
                Function<OperatorThreadInfoStats, T> createStatsFn,
                ExecutorService executor) {
            this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
            this.createStatsFn = createStatsFn;
            this.executor = executor;
        }

        /**
         * Sets {@code cleanUpInterval}.
         *
         * @param coordinator Coordinator for thread info stats request.
         * @return Builder.
         */
        public Builder<T> setCoordinator(ThreadInfoRequestCoordinator coordinator) {
            this.coordinator = coordinator;
            return this;
        }

        /**
         * Sets {@code cleanUpInterval}.
         *
         * @param cleanUpInterval Clean up interval for completed stats.
         * @return Builder.
         */
        public Builder<T> setCleanUpInterval(int cleanUpInterval) {
            this.cleanUpInterval = cleanUpInterval;
            return this;
        }

        /**
         * Sets {@code numSamples}.
         *
         * @param numSamples Number of thread info samples to collect for each subtask.
         * @return Builder.
         */
        public Builder<T> setNumSamples(int numSamples) {
            this.numSamples = numSamples;
            return this;
        }

        /**
         * Sets {@code statsRefreshInterval}.
         *
         * @param statsRefreshInterval Time interval after which the available thread info stats are
         *     deprecated and need to be refreshed.
         * @return Builder.
         */
        public Builder<T> setStatsRefreshInterval(int statsRefreshInterval) {
            this.statsRefreshInterval = statsRefreshInterval;
            return this;
        }

        /**
         * Sets {@code delayBetweenSamples}.
         *
         * @param delayBetweenSamples Delay between individual samples per task.
         * @return Builder.
         */
        public Builder<T> setDelayBetweenSamples(Time delayBetweenSamples) {
            this.delayBetweenSamples = delayBetweenSamples;
            return this;
        }

        /**
         * Sets {@code delayBetweenSamples}.
         *
         * @param maxThreadInfoDepth Limit for the depth of the stack traces included when sampling
         *     threads.
         * @return Builder.
         */
        public Builder<T> setMaxThreadInfoDepth(int maxThreadInfoDepth) {
            this.maxThreadInfoDepth = maxThreadInfoDepth;
            return this;
        }

        /**
         * Constructs a new {@link ThreadInfoOperatorTracker}.
         *
         * @return a new {@link ThreadInfoOperatorTracker} instance.
         */
        public ThreadInfoOperatorTracker<T> build() {
            return new ThreadInfoOperatorTracker<>(
                    coordinator,
                    resourceManagerGatewayRetriever,
                    createStatsFn,
                    executor,
                    cleanUpInterval,
                    numSamples,
                    statsRefreshInterval,
                    delayBetweenSamples,
                    maxThreadInfoDepth);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(ThreadInfoOperatorTracker.class);

    /** Lock guarding trigger operations. */
    private final Object lock = new Object();

    private final ThreadInfoRequestCoordinator coordinator;

    private final Function<OperatorThreadInfoStats, T> createStatsFn;

    private final ExecutorService executor;

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

    /**
     * Completed stats. Important: Job vertex IDs need to be scoped by job ID, because they are
     * potentially constant across runs messing up the cached data.
     */
    private final Cache<AccessExecutionJobVertex, T> operatorStatsCache;

    /**
     * Pending in progress stats. Important: Job vertex IDs need to be scoped by job ID, because
     * they are potentially constant across runs messing up the cached data.
     */
    private final Set<AccessExecutionJobVertex> pendingStats = new HashSet<>();

    private final int numSamples;

    private final int statsRefreshInterval;

    private final Time delayBetweenSamples;

    private final int maxThreadInfoDepth;

    // Used for testing purposes
    private final CompletableFuture<Void> resultAvailableFuture = new CompletableFuture<>();

    /** Flag indicating whether the stats tracker has been shut down. */
    private boolean shutDown;

    private ThreadInfoOperatorTracker(
            ThreadInfoRequestCoordinator coordinator,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            Function<OperatorThreadInfoStats, T> createStatsFn,
            ExecutorService executor,
            int cleanUpInterval,
            int numSamples,
            int statsRefreshInterval,
            Time delayBetweenSamples,
            int maxStackTraceDepth) {

        this.coordinator = checkNotNull(coordinator, "Thread info samples coordinator");
        this.resourceManagerGatewayRetriever =
                checkNotNull(resourceManagerGatewayRetriever, "Gateway retriever");
        this.createStatsFn = checkNotNull(createStatsFn, "Create stats function");
        this.executor = checkNotNull(executor, "Scheduled executor");

        checkArgument(cleanUpInterval >= 0, "Clean up interval");

        checkArgument(numSamples >= 1, "Number of samples");
        this.numSamples = numSamples;

        checkArgument(
                statsRefreshInterval >= 0,
                "Stats refresh interval must be greater than or equal to 0");
        this.statsRefreshInterval = statsRefreshInterval;

        this.delayBetweenSamples = checkNotNull(delayBetweenSamples, "Delay between samples");

        checkArgument(
                maxStackTraceDepth >= 0,
                "Max stack trace depth must be greater than or equal to 0");
        this.maxThreadInfoDepth = maxStackTraceDepth;

        this.operatorStatsCache =
                CacheBuilder.newBuilder()
                        .concurrencyLevel(1)
                        .expireAfterAccess(cleanUpInterval, TimeUnit.MILLISECONDS)
                        .build();
    }

    @Override
    public Optional<T> getOperatorStats(AccessExecutionJobVertex vertex) {
        synchronized (lock) {
            final T stats = operatorStatsCache.getIfPresent(vertex);
            if (stats == null
                    || System.currentTimeMillis() >= stats.getEndTime() + statsRefreshInterval) {
                triggerThreadInfoSampleInternal(vertex);
            }
            return Optional.ofNullable(stats);
        }
    }

    /**
     * Triggers a request for an operator to gather the thread info statistics. If there is a sample
     * in progress for the operator, the call is ignored.
     *
     * @param vertex Operator to get the stats for.
     */
    private void triggerThreadInfoSampleInternal(final AccessExecutionJobVertex vertex) {
        assert (Thread.holdsLock(lock));

        if (shutDown) {
            return;
        }

        if (!pendingStats.contains(vertex)) {
            pendingStats.add(vertex);

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Triggering thread info sample for tasks: "
                                + Arrays.toString(vertex.getTaskVertices()));
            }

            final AccessExecutionVertex[] executionVertices = vertex.getTaskVertices();
            final CompletableFuture<ResourceManagerGateway> gatewayFuture =
                    resourceManagerGatewayRetriever.getFuture();

            CompletableFuture<OperatorThreadInfoStats> sample =
                    gatewayFuture.thenCompose(
                            (ResourceManagerGateway resourceManagerGateway) ->
                                    coordinator.triggerThreadInfoRequest(
                                            matchExecutionsWithGateways(
                                                    executionVertices, resourceManagerGateway),
                                            numSamples,
                                            delayBetweenSamples,
                                            maxThreadInfoDepth));

            sample.handleAsync(new ThreadInfoSampleCompletionCallback(vertex), executor);
        }
    }

    private List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>>
            matchExecutionsWithGateways(
                    AccessExecutionVertex[] executionVertices,
                    ResourceManagerGateway resourceManagerGateway) {

        List<Tuple2<AccessExecutionVertex, CompletableFuture<TaskExecutorGateway>>>
                executionsWithGateways = new ArrayList<>();

        for (AccessExecutionVertex executionVertex : executionVertices) {
            TaskManagerLocation tmLocation = executionVertex.getCurrentAssignedResourceLocation();

            if (tmLocation != null) {
                CompletableFuture<TaskExecutorGateway> taskExecutorGatewayFuture =
                        resourceManagerGateway.requestTaskExecutorGateway(
                                tmLocation.getResourceID());

                executionsWithGateways.add(
                        new Tuple2<>(executionVertex, taskExecutorGatewayFuture));
            } else {
                LOG.warn("ExecutionVertex " + executionVertex + "is currently not assigned");
            }
        }

        return executionsWithGateways;
    }

    @Override
    public void cleanUpOperatorStatsCache() {
        operatorStatsCache.cleanUp();
    }

    @Override
    public void shutDown() {
        synchronized (lock) {
            if (!shutDown) {
                operatorStatsCache.invalidateAll();
                pendingStats.clear();

                shutDown = true;
            }
        }
    }

    @VisibleForTesting
    CompletableFuture<Void> getResultAvailableFuture() {
        return resultAvailableFuture;
    }

    /** Callback on completed thread info sample. */
    class ThreadInfoSampleCompletionCallback
            implements BiFunction<OperatorThreadInfoStats, Throwable, Void> {

        private final AccessExecutionJobVertex vertex;

        ThreadInfoSampleCompletionCallback(AccessExecutionJobVertex vertex) {
            this.vertex = vertex;
        }

        @Override
        public Void apply(OperatorThreadInfoStats threadInfoStats, Throwable throwable) {
            synchronized (lock) {
                try {
                    if (shutDown) {
                        return null;
                    }
                    if (threadInfoStats != null) {
                        resultAvailableFuture.complete(null);
                        operatorStatsCache.put(vertex, createStatsFn.apply(threadInfoStats));
                    } else {
                        LOG.debug("Failed to gather a thread info sample.", throwable);
                    }
                } catch (Throwable t) {
                    LOG.error("Error during stats completion.", t);
                } finally {
                    pendingStats.remove(vertex);
                }
                return null;
            }
        }
    }
}
