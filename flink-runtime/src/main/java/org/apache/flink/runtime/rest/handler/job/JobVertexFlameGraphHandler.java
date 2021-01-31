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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.FlameGraphTypeQueryParameter;
import org.apache.flink.runtime.rest.messages.JobVertexFlameGraphInfo;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.threadinfo.OperatorFlameGraph;
import org.apache.flink.runtime.webmonitor.threadinfo.OperatorFlameGraphFactory;
import org.apache.flink.runtime.webmonitor.threadinfo.OperatorThreadInfoStats;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoOperatorTracker;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

/** Request handler for the job vertex Flame Graph. */
public class JobVertexFlameGraphHandler
        extends AbstractJobVertexHandler<JobVertexFlameGraphInfo, JobVertexMessageParameters> {

    private final ThreadInfoOperatorTracker<OperatorThreadInfoStats> threadInfoOperatorTracker;

    private static JobVertexFlameGraphInfo createJobVertexFlameGraphInfo(
            OperatorFlameGraph flameGraph) {
        return new JobVertexFlameGraphInfo(flameGraph.getEndTime(), flameGraph.getRoot());
    }

    public JobVertexFlameGraphHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JobVertexFlameGraphInfo, JobVertexMessageParameters>
                    messageHeaders,
            ExecutionGraphCache executionGraphCache,
            Executor executor,
            ThreadInfoOperatorTracker<OperatorThreadInfoStats> threadInfoOperatorTracker) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                executionGraphCache,
                executor);
        this.threadInfoOperatorTracker = threadInfoOperatorTracker;
    }

    @Override
    protected JobVertexFlameGraphInfo handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
            @Nonnull AccessExecutionJobVertex jobVertex)
            throws RestHandlerException {

        final Optional<OperatorThreadInfoStats> threadInfoSample =
                threadInfoOperatorTracker.getOperatorStats(jobVertex);

        final List<FlameGraphTypeQueryParameter.Type> flameGraphTypeParameter =
                request.getQueryParameter(FlameGraphTypeQueryParameter.class);
        final FlameGraphTypeQueryParameter.Type flameGraphType;

        if (flameGraphTypeParameter.isEmpty()) {
            flameGraphType = FlameGraphTypeQueryParameter.Type.FULL;
        } else {
            flameGraphType = flameGraphTypeParameter.get(0);
        }

        final Optional<OperatorFlameGraph> operatorFlameGraph;

        switch (flameGraphType) {
            case FULL:
                operatorFlameGraph =
                        threadInfoSample.map(OperatorFlameGraphFactory::createFullFlameGraphFrom);
                break;
            case ON_CPU:
                operatorFlameGraph =
                        threadInfoSample.map(OperatorFlameGraphFactory::createOnCpuFlameGraph);
                break;
            case OFF_CPU:
                operatorFlameGraph =
                        threadInfoSample.map(OperatorFlameGraphFactory::createOffCpuFlameGraph);
                break;
            default:
                throw new RestHandlerException(
                        "Unknown Flame Graph type " + flameGraphType + '.',
                        HttpResponseStatus.BAD_REQUEST);
        }

        return operatorFlameGraph
                .map(JobVertexFlameGraphHandler::createJobVertexFlameGraphInfo)
                .orElse(JobVertexFlameGraphInfo.empty());
    }
}
