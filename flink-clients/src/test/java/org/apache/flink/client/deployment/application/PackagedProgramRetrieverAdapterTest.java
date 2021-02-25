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
import org.apache.flink.client.testjar.TestJob;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.client.deployment.application.PackagedProgramRetrieverAdapter.newBuilder;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link PackagedProgramRetrieverAdapter}. */
public class PackagedProgramRetrieverAdapterTest extends TestLogger {

    @Test
    public void testBuildPackagedProgramRetriever() throws IOException {
        final String[] programArguments = {"--arg", "suffix"};
        PackagedProgramRetriever retrieverUnderTest =
                newBuilder(programArguments)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .build();
        assertTrue(retrieverUnderTest instanceof ClassPathPackagedProgramRetriever);

        final File testJar = TestJob.getTestJobJar();
        retrieverUnderTest = newBuilder(programArguments).setJarFile(testJar).build();
        assertTrue(retrieverUnderTest instanceof JarFilePackagedProgramRetriever);

        final String[] pythonArguments = {"-py", "test.py"};
        retrieverUnderTest = newBuilder(pythonArguments).build();
        assertTrue(retrieverUnderTest instanceof PythonBasedPackagedProgramRetriever);
    }
}
