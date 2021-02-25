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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.application.ClassPathPackagedProgramRetriever.JarsOnClassPath;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.TestJob;
import org.apache.flink.client.testjar.TestJobInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.flink.client.deployment.application.ClassPathPackagedProgramRetriever.JarsOnClassPath.JAVA_CLASS_PATH;
import static org.apache.flink.client.deployment.application.ClassPathPackagedProgramRetriever.JarsOnClassPath.PATH_SEPARATOR;
import static org.apache.flink.client.deployment.application.PackagedProgramRetrieverAdapter.newBuilder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link ClassPathPackagedProgramRetriever}. */
public class ClassPathPackagedProgramRetrieverTest extends PackagedProgramRetrieverTestBase {

    @Test
    public void testJobGraphRetrieval()
            throws FlinkException, ProgramInvocationException, IOException {
        testJobGraphRetrievalFromClassPath();
        testJobGraphRetrievalJobClassNameHasPrecedenceOverClassPath();
        testJobGraphRetrievalFailIfJobDirDoesNotHaveEntryClass();
        testJobGraphRetrievalFailIfDoesNotFindTheEntryClassInTheJobDir();
    }

    private void testJobGraphRetrievalFromClassPath()
            throws IOException, FlinkException, ProgramInvocationException {
        final int parallelism = 42;
        final JobID jobId = new JobID();

        final Configuration configuration = new Configuration();
        configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());

        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .build();

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, configuration);

        assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
        assertThat(
                jobGraph.getSavepointRestoreSettings(),
                is(equalTo(SavepointRestoreSettings.none())));
        assertThat(jobGraph.getMaximumParallelism(), is(parallelism));
        assertEquals(jobGraph.getJobID(), jobId);
    }

    private void testJobGraphRetrievalJobClassNameHasPrecedenceOverClassPath()
            throws IOException, FlinkException, ProgramInvocationException {
        final File testJar = new File("non-existing");

        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS)
                        // Both a class name is specified and a JAR "is" on the class path
                        // The class name should have precedence.
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .setJarsOnClassPath(() -> Collections.singleton(testJar))
                        .build();

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

        assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
    }

    private void testJobGraphRetrievalFailIfJobDirDoesNotHaveEntryClass()
            throws IOException, ProgramInvocationException {
        final File testJar = TestJob.getTestJobJar();
        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS)
                        .setJarsOnClassPath(() -> Collections.singleton(testJar))
                        .setUserLibDirectory(userDirHasNotEntryClass)
                        .build();
        try {
            retrieveJobGraph(retrieverUnderTest, new Configuration());
            Assert.fail("This case should throw exception !");
        } catch (FlinkException e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e, "Failed to find job JAR on class path")
                            .isPresent());
        }
    }

    private void testJobGraphRetrievalFailIfDoesNotFindTheEntryClassInTheJobDir()
            throws IOException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS)
                        .setJobClassName(TestJobInfo.JOB_CLASS)
                        .setJarsOnClassPath(Collections::emptyList)
                        .setUserLibDirectory(userDirHasNotEntryClass)
                        .build();
        try {
            retrieveJobGraph(retrieverUnderTest, new Configuration());
            Assert.fail("This case should throw class not found exception!!");
        } catch (FlinkException e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e, "Could not find the provided job class")
                            .isPresent());
        }
    }

    @Test
    public void testSavepointRestoreSettings()
            throws FlinkException, IOException, ProgramInvocationException {
        final Configuration configuration = new Configuration();
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath("foobar", true);
        final JobID jobId = new JobID();

        configuration.setString(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        SavepointRestoreSettings.toConfiguration(savepointRestoreSettings, configuration);

        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .build();

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, configuration);

        assertThat(jobGraph.getSavepointRestoreSettings(), is(equalTo(savepointRestoreSettings)));
        assertEquals(jobGraph.getJobID(), jobId);
    }

    @Test
    public void testJarFromClassPathSupplierSanityCheck() {
        Iterable<File> jarFiles = JarsOnClassPath.INSTANCE.get();

        // Junit executes this test, so it should be returned as part of JARs on the class path
        assertThat(jarFiles, hasItem(hasProperty("name", containsString("junit"))));
    }

    @Test
    public void testRetrieveCorrectUserClassPathsWithoutSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS)
                        .setJarsOnClassPath(Collections::emptyList)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .build();
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

        assertThat(
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_URLS.stream().map(URL::toString).toArray()));
    }

    @Test
    public void testRetrieveCorrectUserClassPathsWithSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS)
                        .setJobClassName(TestJobInfo.JOB_CLASS)
                        .setJarsOnClassPath(Collections::emptyList)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .build();
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

        assertThat(
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList()),
                containsInAnyOrder(EXPECTED_URLS.stream().map(URL::toString).toArray()));
    }

    @Test
    public void testGetPackagedProgramWithConfiguration() throws IOException, FlinkException {
        final Configuration configuration = new Configuration();
        configuration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
        configuration.setBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        final PackagedProgramRetriever retrieverUnderTest =
                newBuilder(PROGRAM_ARGUMENTS)
                        .setConfiguration(configuration)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .build();
        assertTrue(
                retrieverUnderTest.getPackagedProgram().getUserCodeClassLoader()
                        instanceof FlinkUserCodeClassLoaders.ParentFirstClassLoader);
    }

    @Test
    public void testJarFromClassPathSupplier() throws IOException {
        final File file1 = temporaryFolder.newFile();
        final File file2 = temporaryFolder.newFile();
        final File directory = temporaryFolder.newFolder();

        // Mock java.class.path property. The empty strings are important as the shell scripts
        // that prepare the Flink class path often have such entries.
        final String classPath =
                javaClassPath(
                        "",
                        "",
                        "",
                        file1.getAbsolutePath(),
                        "",
                        directory.getAbsolutePath(),
                        "",
                        file2.getAbsolutePath(),
                        "",
                        "");

        Iterable<File> jarFiles = setClassPathAndGetJarsOnClassPath(classPath);

        assertThat(jarFiles, contains(file1, file2));
    }

    private static String javaClassPath(String... entries) {
        String pathSeparator = System.getProperty(PATH_SEPARATOR);
        return String.join(pathSeparator, entries);
    }

    private static Iterable<File> setClassPathAndGetJarsOnClassPath(String classPath) {
        final String originalClassPath = System.getProperty(JAVA_CLASS_PATH);
        try {
            System.setProperty(JAVA_CLASS_PATH, classPath);
            return JarsOnClassPath.INSTANCE.get();
        } finally {
            // Reset property
            System.setProperty(JAVA_CLASS_PATH, originalClassPath);
        }
    }
}
