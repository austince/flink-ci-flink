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

package org.apache.flink.table.client.gateway.context;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.internal.BatchTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.util.TemporaryClassLoaderContext;

import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.apache.flink.table.client.gateway.context.SessionContext.SessionState;

/**
 * Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as it
 * might be reused across different query submissions.
 */
public class ExecutionContext {

    // TODO: merge the ExecutionContext into the SessionContext.
    // Members that should be reused in the same session.
    private final Environment environment;
    private final Configuration flinkConfig;
    private final SessionState sessionState;
    private final URLClassLoader classLoader;

    private final TableEnvironment tableEnv;

    public ExecutionContext(
            Environment environment,
            Configuration flinkConfig,
            URLClassLoader classLoader,
            SessionState sessionState) {
        this.environment = environment;
        this.flinkConfig = flinkConfig;
        this.sessionState = sessionState;
        this.classLoader = classLoader;

        this.tableEnv = createTableEnvironment();
    }

    /**
     * Create a new {@link ExecutionContext}.
     *
     * <p>It just copies from the {@link ExecutionContext} and rebuild a new {@link
     * TableEnvironment}. But it still needs {@link Environment} because {@link Environment} doesn't
     * allow modification.
     *
     * <p>When FLINK-21462 finishes, the constructor only uses {@link ExecutionContext} as input.
     */
    public ExecutionContext(Environment environment, ExecutionContext context) {
        this.environment = environment;
        this.flinkConfig = context.flinkConfig;
        this.sessionState = context.sessionState;
        this.classLoader = context.classLoader;
        // create a new table env
        this.tableEnv = createTableEnvironment();
    }

    /**
     * Executes the given supplier using the execution context's classloader as thread classloader.
     */
    public <R> R wrapClassLoader(Supplier<R> supplier) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            return supplier.get();
        }
    }

    public TableEnvironment getTableEnvironment() {
        return tableEnv;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Helper to create Table Environment
    // ------------------------------------------------------------------------------------------------------------------

    private TableEnvironment createTableEnvironment() {
        EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
        TableConfig config = new TableConfig();
        config.addConfiguration(flinkConfig);
        // Override the value in configuration.
        // TODO: use `table.planner` and `execution.runtime-mode` to configure the TableEnvironment
        config.addConfiguration(settings.toConfiguration());

        if (environment.getExecution().isStreamingPlanner()) {
            StreamExecutionEnvironment streamExecEnv = createStreamExecutionEnvironment();

            final Map<String, String> executorProperties = settings.toExecutorProperties();
            Executor executor = lookupExecutor(executorProperties, streamExecEnv);
            return createStreamTableEnvironment(
                    streamExecEnv,
                    settings,
                    config,
                    executor,
                    sessionState.catalogManager,
                    sessionState.moduleManager,
                    sessionState.functionCatalog,
                    classLoader);
        } else if (environment.getExecution().isBatchPlanner()) {
            ExecutionEnvironment execEnv = createExecutionEnvironment();
            return new BatchTableEnvironmentImpl(
                    execEnv, config, sessionState.catalogManager, sessionState.moduleManager);
        } else {
            throw new SqlExecutionException("Unsupported execution type specified.");
        }
    }

    private TableEnvironment createStreamTableEnvironment(
            StreamExecutionEnvironment env,
            EnvironmentSettings settings,
            TableConfig config,
            Executor executor,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog,
            ClassLoader userClassLoader) {

        final Map<String, String> plannerProperties = settings.toPlannerProperties();
        final Planner planner =
                ComponentFactoryService.find(PlannerFactory.class, plannerProperties)
                        .create(
                                plannerProperties,
                                executor,
                                config,
                                functionCatalog,
                                catalogManager);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                functionCatalog,
                config,
                env,
                planner,
                executor,
                settings.isStreamingMode(),
                userClassLoader);
    }

    private Executor lookupExecutor(
            Map<String, String> executorProperties,
            StreamExecutionEnvironment executionEnvironment) {
        try {
            ExecutorFactory executorFactory =
                    ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
            Method createMethod =
                    executorFactory
                            .getClass()
                            .getMethod("create", Map.class, StreamExecutionEnvironment.class);

            return (Executor)
                    createMethod.invoke(executorFactory, executorProperties, executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

    private StreamExecutionEnvironment createStreamExecutionEnvironment() {
        // We need not different StreamExecutionEnvironments to build and submit flink job,
        // instead we just use StreamExecutionEnvironment#executeAsync(StreamGraph) method
        // to execute existing StreamGraph.
        // This requires StreamExecutionEnvironment to have a full flink configuration.
        final StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(new Configuration(flinkConfig), classLoader);
        // for TimeCharacteristic validation in StreamTableEnvironmentImpl
        env.setStreamTimeCharacteristic(environment.getExecution().getTimeCharacteristic());
        if (env.getStreamTimeCharacteristic() == TimeCharacteristic.EventTime) {
            env.getConfig()
                    .setAutoWatermarkInterval(
                            environment.getExecution().getPeriodicWatermarksInterval());
        }
        return env;
    }

    private ExecutionEnvironment createExecutionEnvironment() {
        ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
        execEnv.getConfiguration().addAll(flinkConfig);
        return execEnv;
    }

    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return Objects.hash(environment, flinkConfig, sessionState, tableEnv, classLoader);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof ExecutionContext)) {
            return false;
        }

        ExecutionContext context = (ExecutionContext) obj;
        return Objects.equals(environment, context.environment)
                && Objects.equals(flinkConfig, context.flinkConfig)
                && Objects.equals(sessionState, context.sessionState)
                && Objects.equals(tableEnv, context.tableEnv)
                && Objects.equals(classLoader, context.classLoader);
    }
}
