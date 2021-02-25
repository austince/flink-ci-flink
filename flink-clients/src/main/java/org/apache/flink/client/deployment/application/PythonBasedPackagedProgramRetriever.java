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
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * A python based {@link org.apache.flink.client.program.PackagedProgramRetriever
 * PackagedProgramRetriever} which creates the {@link
 * org.apache.flink.client.program.PackagedProgram PackagedProgram} when program arguments contain
 * "-py/--python" or "-pym/--pyModule".
 */
@Internal
public class PythonBasedPackagedProgramRetriever extends AbstractPackagedProgramRetriever {

    protected PythonBasedPackagedProgramRetriever(
            @Nonnull String[] programArguments,
            @Nonnull Configuration configuration,
            @Nullable File userLibDirectory)
            throws IOException {
        super(programArguments, configuration, userLibDirectory);
    }

    @Override
    public PackagedProgram buildPackagedProgram()
            throws ProgramInvocationException, FlinkException {
        // It is Python job if program arguments contain "-py/--python" or "-pym/--pyModule",
        // set the fixed jobClassName and jarFile path.
        String pythonJobClassName = PackagedProgramUtils.getPythonDriverClassName();
        File pythonJarFile = new File(PackagedProgramUtils.getPythonJar().getPath());
        return PackagedProgram.newBuilder()
                .setArguments(programArguments)
                .setConfiguration(configuration)
                .setUserClassPaths(new ArrayList<>(userClassPaths))
                .setJarFile(pythonJarFile)
                .setEntryPointClassName(pythonJobClassName)
                .build();
    }
}
