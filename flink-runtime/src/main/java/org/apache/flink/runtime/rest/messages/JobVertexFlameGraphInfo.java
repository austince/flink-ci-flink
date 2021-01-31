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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.handler.job.JobVertexFlameGraphHandler;
import org.apache.flink.runtime.webmonitor.threadinfo.OperatorFlameGraph;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/** Response type of the {@link JobVertexFlameGraphHandler}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobVertexFlameGraphInfo implements ResponseBody {

    public static JobVertexFlameGraphInfo empty() {
        return new JobVertexFlameGraphInfo(null, null);
    }

    private static final String FIELD_NAME_END_TIMESTAMP = "end-timestamp";
    private static final String FIELD_NAME_ROOT = "data";

    @JsonProperty(FIELD_NAME_END_TIMESTAMP)
    private final Long endTimestamp;

    @JsonProperty(FIELD_NAME_ROOT)
    private final OperatorFlameGraph.Node root;

    @JsonCreator
    public JobVertexFlameGraphInfo(
            @JsonProperty(FIELD_NAME_END_TIMESTAMP) Long endTimestamp,
            @JsonProperty(FIELD_NAME_ROOT) OperatorFlameGraph.Node root) {
        this.endTimestamp = endTimestamp;
        this.root = root;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
    }

    public OperatorFlameGraph.Node getData() {
        return root;
    }
}
