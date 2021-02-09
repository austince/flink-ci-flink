/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.BitSet;

import static org.apache.flink.util.Preconditions.checkState;

/** Integration test for performing rescale of unaligned checkpoint. */
@RunWith(Parameterized.class)
public class UnalignedCheckpointRescaleITCase extends UnalignedCheckpointTestBase {
    public static final int NUM_GROUPS = 100;
    private final Topology topology;
    private final int oldParallelism;
    private final int newParallelism;

    enum Topology {
        PIPELINE {
            @Override
            UnalignedSettings createSettings(int parallelism) {
                int numShuffles = 8;
                final int numSlots = parallelism * numShuffles + 3;
                // aim for 3 TMs
                int slotsPerTaskManager = (numSlots + 2) / 3;
                return new UnalignedSettings(this::createPipeline)
                        .setParallelism(parallelism)
                        .setSlotSharing(false)
                        .setNumSlots(numSlots)
                        .setNumBuffers(getNumBuffers(parallelism + 1, numShuffles))
                        .setSlotsPerTaskManager(slotsPerTaskManager)
                        .setExpectedFailures(1);
            }

            private void createPipeline(
                    StreamExecutionEnvironment env,
                    long minCheckpoints,
                    boolean slotSharing,
                    int expectedRestarts) {
                final int parallelism = env.getParallelism();
                env.fromSource(
                                new LongSource(minCheckpoints, 1, expectedRestarts),
                                WatermarkStrategy.noWatermarks(),
                                "source")
                        .slotSharingGroup(slotSharing ? "default" : "source")
                        .setParallelism(1)
                        .shuffle()
                        .map(
                                new FailingMapper(
                                        state -> false,
                                        state ->
                                                state.completedCheckpoints >= minCheckpoints / 2
                                                        && state.runNumber == 0,
                                        state -> false,
                                        state -> false))
                        .name("failing-map")
                        .uid("failing-map")
                        .slotSharingGroup(slotSharing ? "default" : "map")
                        .global()
                        .map(i -> checkHeader(i))
                        .name("global")
                        .uid("global")
                        .slotSharingGroup(slotSharing ? "default" : "global")
                        .rebalance()
                        .map(i -> checkHeader(i))
                        .setParallelism(parallelism + 1)
                        .name("rebalance")
                        .uid("rebalance")
                        .slotSharingGroup(slotSharing ? "default" : "rebalance")
                        .shuffle()
                        .map(i -> checkHeader(i))
                        .name("upscale")
                        .uid("upscale")
                        .setParallelism(2 * parallelism)
                        .slotSharingGroup(slotSharing ? "default" : "upscale")
                        .shuffle()
                        .map(i -> checkHeader(i))
                        .name("downscale")
                        .uid("downscale")
                        .setParallelism(parallelism + 1)
                        .slotSharingGroup(slotSharing ? "default" : "downscale")
                        .keyBy(i -> withoutHeader(i) % NUM_GROUPS)
                        .map(new StatefulKeyedMap())
                        .shuffle()
                        .addSink(new VerifyingSink(minCheckpoints))
                        .setParallelism(1)
                        .name("sink")
                        .uid("sink")
                        .slotSharingGroup(slotSharing ? "default" : "sink");
            }
        };

        abstract UnalignedSettings createSettings(int parallelism);

        @Override
        public String toString() {
            return name().toLowerCase();
        }

        private static class StatefulKeyedMap extends RichMapFunction<Long, Long> {
            private static final ValueStateDescriptor<Long> DESC =
                    new ValueStateDescriptor<>("state", LongSerializer.INSTANCE);
            ValueState<Long> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(DESC);
            }

            @Override
            public Long map(Long value) throws Exception {
                final Long lastValue = state.value();
                checkState(
                        lastValue == null
                                || checkHeader(value) % NUM_GROUPS
                                        == checkHeader(lastValue) % NUM_GROUPS,
                        "Mismatched key group");
                state.update(value);
                return value;
            }
        }
    }

    @Parameterized.Parameters(name = "{0} {1} from {2} to {3}")
    public static Object[][] getScaleFactors() {
        return new Object[][] {
            new Object[] {"upscale", Topology.PIPELINE, 1, 2},
            new Object[] {"upscale", Topology.PIPELINE, 2, 3},
            new Object[] {"upscale", Topology.PIPELINE, 3, 7},
            new Object[] {"upscale", Topology.PIPELINE, 4, 8},
            new Object[] {"upscale", Topology.PIPELINE, 20, 21},
            new Object[] {"downscale", Topology.PIPELINE, 2, 1},
            new Object[] {"downscale", Topology.PIPELINE, 3, 2},
            new Object[] {"downscale", Topology.PIPELINE, 7, 3},
            new Object[] {"downscale", Topology.PIPELINE, 8, 4},
            new Object[] {"downscale", Topology.PIPELINE, 21, 20},
            new Object[] {"no scale", Topology.PIPELINE, 1, 1},
            new Object[] {"no scale", Topology.PIPELINE, 3, 3},
            new Object[] {"no scale", Topology.PIPELINE, 7, 7},
            new Object[] {"no scale", Topology.PIPELINE, 20, 20},
        };
    }

    public UnalignedCheckpointRescaleITCase(
            String desc, Topology topology, int oldParallelism, int newParallelism) {
        this.topology = topology;
        this.oldParallelism = oldParallelism;
        this.newParallelism = newParallelism;
    }

    @Test
    public void shouldRescaleUnalignedCheckpoint() throws Exception {
        final UnalignedSettings prescaleSettings = topology.createSettings(oldParallelism);
        prescaleSettings.setGenerateCheckpoint(true);
        final File checkpointDir = super.execute(prescaleSettings);

        // resume
        final UnalignedSettings postscaleSettings = topology.createSettings(newParallelism);
        postscaleSettings.setRestoreCheckpoint(checkpointDir).setGenerateCheckpoint(false);
        super.execute(postscaleSettings);
    }

    /**
     * A sink that checks if the members arrive in the expected order without any missing values.
     */
    protected static class VerifyingSink extends VerifyingSinkBase<VerifyingSink.State> {

        protected VerifyingSink(long minCheckpoints) {
            super(minCheckpoints);
        }

        @Override
        protected State createState() {
            return new State();
        }

        @Override
        public void invoke(Long value, Context context) throws Exception {
            final int intValue = (int) withoutHeader(value);
            if (state.encounteredNumbers.get(intValue)) {
                state.numDuplicates++;
                LOG.info(
                        "Duplicate record {} @ {} subtask ({} attempt)",
                        value,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber());
            }
            state.encounteredNumbers.set(intValue);
            state.numOutput++;

            if (backpressure) {
                // induce heavy backpressure until enough checkpoints have been written
                Thread.sleep(0, 100_000);
            }
            // after all checkpoints have been completed, the remaining data should be flushed out
            // fairly quickly
        }

        @Override
        public void close() throws Exception {
            state.numLostValues =
                    state.encounteredNumbers.length() - state.encounteredNumbers.cardinality();
            super.close();
        }

        static class State extends VerifyingSinkStateBase {
            private final BitSet encounteredNumbers = new BitSet();
        }
    }
}
