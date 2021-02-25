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

package org.apache.flink.client.deployment.application;

import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.TestJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.stream.Collectors;

import static org.apache.flink.client.deployment.application.PackagedProgramRetrieverAdapter.newBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link JarFilePackagedProgramRetriever}. */
public class JarFilePackagedProgramRetrieverTest extends PackagedProgramRetrieverTestBase {

    @Test
    public void testJobGraphRetrieval()
            throws FlinkException, ProgramInvocationException, IOException {
        testRetrieveFromJarFileWithoutUserLib();
        testRetrieveFromJarFileWithUserLib();
    }

    private void testRetrieveFromJarFileWithoutUserLib()
            throws IOException, FlinkException, ProgramInvocationException {
        final File testJar = TestJob.getTestJobJar();
        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS).setJarFile(testJar).build();
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

        assertThat(jobGraph.getUserJars(), containsInAnyOrder(new Path(testJar.toURI())));
        assertThat(jobGraph.getClasspaths().isEmpty(), is(true));
    }

    private void testRetrieveFromJarFileWithUserLib()
            throws IOException, FlinkException, ProgramInvocationException {
        final File testJar = TestJob.getTestJobJar();
        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS)
                        .setJarFile(testJar)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .build();
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

        assertThat(
                jobGraph.getUserJars(),
                containsInAnyOrder(new org.apache.flink.core.fs.Path(testJar.toURI())));
        assertThat(
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_URLS.stream().map(URL::toString).toArray()));
    }
}
