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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/** Utilities for reactive mode. */
public final class ReactiveModeUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ReactiveModeUtils.class);

    /**
     * Sets the parallelism of all vertices in the passed JobGraph to the highest possible max
     * parallelism, unless the user defined a maxParallelism.
     *
     * @param jobGraph The JobGraph to modify.
     */
    public static void configureJobGraphForReactiveMode(JobGraph jobGraph) {
        LOG.info("Modifying job parallelism for running in reactive mode.");
        for (JobVertex vertex : jobGraph.getVertices()) {
            if (vertex.getMaxParallelism() == JobVertex.MAX_PARALLELISM_DEFAULT) {
                vertex.setParallelism(Transformation.UPPER_BOUND_MAX_PARALLELISM);
                vertex.setMaxParallelism(Transformation.UPPER_BOUND_MAX_PARALLELISM);
            } else {
                vertex.setParallelism(vertex.getMaxParallelism());
            }
        }
    }

    public static void configureClusterForReactiveMode(Configuration configuration) {
        LOG.info("Modifying Cluster configuration for reactive mode");

        if (!configuration.contains(JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT)) {
            // Configure adaptive scheduler to schedule job even if desired resources are not
            // available (but sufficient resources)
            configuration.set(
                    JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT, Duration.ofMillis(0));
        }
        if (!configuration.contains(JobManagerOptions.RESOURCE_WAIT_TIMEOUT)) {
            // configure adaptive scheduler to wait forever for TaskManagers to register
            configuration.set(JobManagerOptions.RESOURCE_WAIT_TIMEOUT, Duration.ofMillis(-1));
        }
    }

    private ReactiveModeUtils() {}
}
