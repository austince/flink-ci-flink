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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.runners.python.beam.BeamDataStreamPythonFunctionRunner;
import org.apache.flink.streaming.api.utils.PythonOperatorUtils;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Row;

import java.util.Collections;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionInputFlag.EVENT_TIME_TIMER;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionInputFlag.PROC_TIME_TIMER;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionOutputFlag.DEL_EVENT_TIMER;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionOutputFlag.DEL_PROC_TIMER;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionOutputFlag.REGISTER_EVENT_TIMER;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.KeyedProcessFunctionOutputFlag.REGISTER_PROC_TIMER;

/** KeyedCoProcessOperator. */
public class PythonKeyedCoProcessOperator<OUT>
        extends TwoInputPythonFunctionOperator<Row, Row, Row, OUT>
        implements ResultTypeQueryable<OUT>, Triggerable<Row, VoidNamespace> {

    private static final String KEYED_CO_PROCESS_FUNCTION_URN =
            "flink:transform:keyed_process_function:v1";

    private static final String FLAT_MAP_CODER_URN = "flink:coder:flat_map:v1";

    /** The TypeInformation of current key. */
    private final TypeInformation<Row> keyTypeInfo;

    /** Serializer for current key. */
    private final TypeSerializer keyTypeSerializer;

    private final TypeInformation<OUT> outputTypeInfo;

    /** TimerService for current operator to register or fire timer. */
    private transient TimerService timerService;

    /** Reusable row for normal data runner inputs. */
    private transient Row reusableInput;

    /** Reusable row for timer data runner inputs. */
    private transient Row reusableTimerData;

    public PythonKeyedCoProcessOperator(
            Configuration config,
            TypeInformation<Row> inputTypeInfo1,
            TypeInformation<Row> inputTypeInfo2,
            TypeInformation<OUT> outputTypeInfo,
            DataStreamPythonFunctionInfo pythonFunctionInfo) {
        super(
                config,
                pythonFunctionInfo,
                FLAT_MAP_CODER_URN,
                constructRunnerInputTypeInfo(
                        inputTypeInfo1, inputTypeInfo2, constructKeyTypeInfo(inputTypeInfo1)),
                constructRunnerOutputTypeInfo(
                        outputTypeInfo, constructKeyTypeInfo(inputTypeInfo1)));
        this.keyTypeInfo = new RowTypeInfo(((RowTypeInfo) inputTypeInfo1).getTypeAt(0));
        this.keyTypeSerializer =
                PythonTypeUtils.TypeInfoToSerializerConverter.typeInfoSerializerConverter(
                        keyTypeInfo);
        this.outputTypeInfo = outputTypeInfo;
    }

    @Override
    public PythonFunctionRunner createPythonFunctionRunner() throws Exception {
        return new BeamDataStreamPythonFunctionRunner(
                getRuntimeContext().getTaskName(),
                createPythonEnvironmentManager(),
                getRunnerInputTypeInfo(),
                getRunnerOutputTypeInfo(),
                KEYED_CO_PROCESS_FUNCTION_URN,
                PythonOperatorUtils.getUserDefinedDataStreamStatefulFunctionProto(
                        getPythonFunctionInfo(),
                        getRuntimeContext(),
                        Collections.EMPTY_MAP,
                        keyTypeInfo),
                getCoderUrn(),
                getJobOptions(),
                getFlinkMetricContainer(),
                getKeyedStateBackend(),
                keyTypeSerializer,
                getContainingTask().getEnvironment().getMemoryManager(),
                getOperatorConfig()
                        .getManagedMemoryFractionOperatorUseCaseOfSlot(
                                ManagedMemoryUseCase.PYTHON,
                                getContainingTask()
                                        .getEnvironment()
                                        .getTaskManagerInfo()
                                        .getConfiguration(),
                                getContainingTask()
                                        .getEnvironment()
                                        .getUserCodeClassLoader()
                                        .asClassLoader()));
    }

    @Override
    public void open() throws Exception {
        InternalTimerService<VoidNamespace> internalTimerService =
                getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);
        timerService = new SimpleTimerService(internalTimerService);
        reusableInput = new Row(5);
        reusableTimerData = new Row(5);

        this.collector = new TimestampedCollector<>(output);
        super.open();
    }

    @Override
    public void processElement1(StreamRecord<Row> element) throws Exception {
        bufferedTimestamp.offer(element.getTimestamp());
        writeTimestampAndWatermark(reusableInput, element, timerService.currentWatermark());
        writeInput1(reusableInput, reuseRow, element);

        getRunnerInputTypeSerializer().serialize(reusableInput, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    @Override
    public void processElement2(StreamRecord<Row> element) throws Exception {
        bufferedTimestamp.offer(element.getTimestamp());
        writeTimestampAndWatermark(reusableInput, element, timerService.currentWatermark());
        writeInput2(reusableInput, reuseRow, element);

        getRunnerInputTypeSerializer().serialize(reusableInput, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    @Override
    public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
        byte[] rawResult = resultTuple.f0;
        int length = resultTuple.f1;
        if (PythonOperatorUtils.endOfLastFlatMap(length, rawResult)) {
            bufferedTimestamp.poll();
        } else {
            bais.setBuffer(rawResult, 0, length);
            Row runnerOutput = getRunnerOutputTypeSerializer().deserialize(baisWrapper);
            if (runnerOutput.getField(0) != null) {
                registerTimer(runnerOutput);
            } else {
                collector.setAbsoluteTimestamp(bufferedTimestamp.peek());
                collector.collect((OUT) runnerOutput.getField(3));
            }
        }
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return outputTypeInfo;
    }

    @Override
    public void onEventTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
        bufferedTimestamp.offer(timer.getTimestamp());
        processTimer(false, timer);
    }

    @Override
    public void onProcessingTime(InternalTimer<Row, VoidNamespace> timer) throws Exception {
        bufferedTimestamp.offer(Long.MIN_VALUE);
        processTimer(true, timer);
    }

    private void writeTimestampAndWatermark(
            Row reusableRunnerInput, StreamRecord<Row> element, long watermark) {
        if (element.hasTimestamp()) {
            reusableRunnerInput.setField(1, element.getTimestamp());
        }
        reusableRunnerInput.setField(2, timerService.currentWatermark());
    }

    private void writeInput1(
            Row reusableRunnerInput, Row reusableUnifiedUserInput, StreamRecord<Row> element) {
        reusableUnifiedUserInput.setField(0, true);
        // The input row is a tuple of key and value.
        reusableUnifiedUserInput.setField(1, element.getValue());
        // need to set null since it is a reuse row.
        reusableUnifiedUserInput.setField(2, null);

        reusableRunnerInput.setField(4, reusableUnifiedUserInput);
    }

    private void writeInput2(
            Row reusableRunnerInput, Row reusableUnifiedUserInput, StreamRecord<Row> element) {
        reusableUnifiedUserInput.setField(0, false);
        // need to set null since it is a reuse row.
        reusableUnifiedUserInput.setField(1, null);
        // The input row is a tuple of key and value.
        reusableUnifiedUserInput.setField(2, element.getValue());

        reusableRunnerInput.setField(4, reusableUnifiedUserInput);
    }

    /**
     * It is responsible to send timer data to python worker when a registered timer is fired. The
     * input data is a Row containing 4 fields: TimerFlag 0 for proc time, 1 for event time;
     * Timestamp of the fired timer; Current watermark and the key of the timer.
     *
     * @param procTime Whether is it a proc time timer, otherwise event time timer.
     * @param timer The fired timer.
     * @throws Exception The runnerInputSerializer might throw exception.
     */
    private void processTimer(boolean procTime, InternalTimer<Row, VoidNamespace> timer)
            throws Exception {
        long time = timer.getTimestamp();
        Row timerKey = Row.of(timer.getKey());
        if (procTime) {
            reusableTimerData.setField(0, PROC_TIME_TIMER.value);
        } else {
            reusableTimerData.setField(0, EVENT_TIME_TIMER.value);
        }
        reusableTimerData.setField(1, time);
        reusableTimerData.setField(2, timerService.currentWatermark());
        reusableTimerData.setField(3, timerKey);
        getRunnerInputTypeSerializer().serialize(reusableTimerData, baosWrapper);
        pythonFunctionRunner.process(baos.toByteArray());
        baos.reset();
        elementCount++;
        checkInvokeFinishBundleByCount();
        emitResults();
    }

    /**
     * Handler the timer registration request from python user defined function. Before registering
     * the timer, we must set the current key to be the key when the timer is register in python
     * side.
     *
     * @param runnerOutput The timer registration request data.
     */
    private void registerTimer(Row runnerOutput) {
        synchronized (getKeyedStateBackend()) {
            byte type = (byte) runnerOutput.getField(0);
            long time = (long) runnerOutput.getField(1);
            Object timerKey = ((Row) (runnerOutput.getField(2))).getField(0);
            setCurrentKey(timerKey);
            if (type == REGISTER_EVENT_TIMER.value) {
                this.timerService.registerEventTimeTimer(time);
            } else if (type == REGISTER_PROC_TIMER.value) {
                this.timerService.registerProcessingTimeTimer(time);
            } else if (type == DEL_EVENT_TIMER.value) {
                this.timerService.deleteEventTimeTimer(time);
            } else if (type == DEL_PROC_TIMER.value) {
                this.timerService.deleteProcessingTimeTimer(time);
            }
        }
    }

    private static TypeInformation<Row> constructKeyTypeInfo(TypeInformation<Row> inputTypeInfo) {
        return new RowTypeInfo(((RowTypeInfo) inputTypeInfo).getTypeAt(0));
    }

    private static TypeInformation<Row> constructRunnerInputTypeInfo(
            TypeInformation<Row> inputTypeInfo1,
            TypeInformation<Row> inputTypeInfo2,
            TypeInformation<Row> keyTypeInfo) {
        // structure: [isLeftUserInput, leftInput, rightInput]
        RowTypeInfo unifiedInputTypeInfo =
                new RowTypeInfo(Types.BOOLEAN, inputTypeInfo1, inputTypeInfo2);

        // structure: [isTimerTrigger, timestamp, currentWatermark, key, userInput]
        return Types.ROW(Types.BYTE, Types.LONG, Types.LONG, keyTypeInfo, unifiedInputTypeInfo);
    }

    private static TypeInformation<Row> constructRunnerOutputTypeInfo(
            TypeInformation<?> outputTypeInfo, TypeInformation<Row> keyTypeInfo) {
        // structure: [isTimerRegistration, timestamp, key, userOutput]
        return Types.ROW(Types.BYTE, Types.LONG, keyTypeInfo, outputTypeInfo);
    }
}
