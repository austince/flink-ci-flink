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
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/** Adapter to provide a {@link Builder} to build a {@link PackagedProgramRetriever}. */
public final class PackagedProgramRetrieverAdapter {

    public static Builder newBuilder(String[] programArguments) {
        return new Builder(programArguments);
    }

    /** A builder for the {@link PackagedProgramRetriever}. */
    public static class Builder {

        private final String[] programArguments;

        private Configuration configuration = new Configuration();

        private Supplier<Iterable<File>> jarsOnClassPath;

        @Nullable private String jobClassName;

        @Nullable private File userLibDirectory;

        @Nullable private File jarFile;

        private Builder(String[] programArguments) {
            this.programArguments = requireNonNull(programArguments);
        }

        public Builder setConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder setJarsOnClassPath(Supplier<Iterable<File>> jarsOnClassPath) {
            this.jarsOnClassPath = jarsOnClassPath;
            return this;
        }

        public Builder setJobClassName(@Nullable String jobClassName) {
            this.jobClassName = jobClassName;
            return this;
        }

        public Builder setUserLibDirectory(File userLibDirectory) {
            this.userLibDirectory = userLibDirectory;
            return this;
        }

        public Builder setJarFile(File file) {
            this.jarFile = file;
            return this;
        }

        public PackagedProgramRetriever build() throws IOException {
            if (PackagedProgramUtils.isPython(jobClassName)
                    || PackagedProgramUtils.isPython(programArguments)) {
                return new PythonBasedPackagedProgramRetriever(
                        programArguments, configuration, userLibDirectory);
            }
            if (jarFile != null) {
                return new JarFilePackagedProgramRetriever(
                        programArguments, configuration, jobClassName, userLibDirectory, jarFile);
            }
            return new ClassPathPackagedProgramRetriever(
                    programArguments,
                    configuration,
                    jarsOnClassPath,
                    jobClassName,
                    userLibDirectory);
        }
    }
}
