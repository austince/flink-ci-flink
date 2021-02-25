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

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * An abstract {@link org.apache.flink.client.program.PackagedProgramRetriever
 * PackagedProgramRetriever} which creates the {@link
 * org.apache.flink.client.program.PackagedProgram PackagedProgram} containing the user's {@code
 * main()} from a class on the class path.
 */
@Internal
public abstract class AbstractPackagedProgramRetriever implements PackagedProgramRetriever {

    @Nonnull protected final String[] programArguments;

    @Nonnull protected final Configuration configuration;

    /** User class paths in relative form to the working directory. */
    @Nonnull protected final Collection<URL> userClassPaths;

    AbstractPackagedProgramRetriever(
            @Nonnull String[] programArguments,
            @Nonnull Configuration configuration,
            @Nullable File userLibDirectory)
            throws IOException {
        this.programArguments = requireNonNull(programArguments, "programArguments");
        this.configuration = requireNonNull(configuration);
        this.userClassPaths = discoverUserClassPaths(userLibDirectory);
    }

    private Collection<URL> discoverUserClassPaths(@Nullable File jobDir) throws IOException {
        if (jobDir == null) {
            return Collections.emptyList();
        }

        final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
        final Collection<URL> relativeJarURLs =
                FileUtils.listFilesInDirectory(jobDir.toPath(), FileUtils::isJarFile).stream()
                        .map(path -> FileUtils.relativizePath(workingDirectory, path))
                        .map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
                        .collect(Collectors.toList());
        return Collections.unmodifiableCollection(relativeJarURLs);
    }

    @Override
    public PackagedProgram getPackagedProgram() throws FlinkException {
        try {
            return buildPackagedProgram();
        } catch (ProgramInvocationException e) {
            throw new FlinkException("Could not load the provided entry point class.", e);
        }
    }

    public abstract PackagedProgram buildPackagedProgram()
            throws ProgramInvocationException, FlinkException;
}
