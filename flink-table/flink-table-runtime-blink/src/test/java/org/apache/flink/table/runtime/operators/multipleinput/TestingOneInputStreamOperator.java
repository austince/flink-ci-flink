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

package org.apache.flink.table.runtime.operators.multipleinput;

import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.StateNameAwareStreamOperator;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link OneInputStreamOperator} for testing.
 */
public class TestingOneInputStreamOperator extends StateNameAwareStreamOperator<RowData>
		implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

	private final boolean emitDataInEndInput;

	private boolean isOpened = false;
	private Object currentStateKey = null;
	private StreamRecord<RowData> currentElement = null;
	private Watermark currentWatermark = null;
	private LatencyMarker currentLatencyMarker = null;
	private long checkpointIdOfPrepareSnapshotPreBarrier = -1;
	private boolean isSnapshotStateExecuted = false;
	private long checkpointIdOfNotifyCheckpointComplete = -1;
	private long checkpointIdOfNotifyCheckpointAborted = -1;
	private boolean isEnd = false;
	private boolean isDisposed = false;
	private boolean isClosed = false;
	private final List<StreamRecord<RowData>> receivedElements = new ArrayList<>();

	public TestingOneInputStreamOperator() {
		this(false);
	}

	public TestingOneInputStreamOperator(boolean emitDataInEndInput) {
		this.emitDataInEndInput = emitDataInEndInput;
	}

	@Override
	public void open() throws Exception {
		isOpened = true;
	}

	@Override
	public void setCurrentKey(Object key) {
		this.currentStateKey = key;
	}

	@Override
	public void processElement(StreamRecord<RowData> element) throws Exception {
		currentElement = element;
		if (emitDataInEndInput) {
			receivedElements.add(element);
		} else {
			output.collect(element);
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		currentWatermark = mark;
		output.emitWatermark(mark);
	}

	@Override
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		currentLatencyMarker = latencyMarker;
		output.emitLatencyMarker(latencyMarker);
	}

	@Override
	public void endInput() throws Exception {
		isEnd = true;
		if (emitDataInEndInput) {
			receivedElements.forEach(output::collect);
		} else {
			Preconditions.checkArgument(receivedElements.isEmpty());
		}
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		checkpointIdOfPrepareSnapshotPreBarrier = checkpointId;
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		isSnapshotStateExecuted = true;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		checkpointIdOfNotifyCheckpointComplete = checkpointId;
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {
		checkpointIdOfNotifyCheckpointAborted = checkpointId;
	}

	@Override
	public void dispose() throws Exception {
		this.isDisposed = true;
	}

	@Override
	public void close() throws Exception {
		isClosed = true;
	}

	public boolean isOpened() {
		return isOpened;
	}

	public Object getCurrentStateKey() {
		return currentStateKey;
	}

	public StreamRecord<RowData> getCurrentElement() {
		return currentElement;
	}

	public Watermark getCurrentWatermark() {
		return currentWatermark;
	}

	public LatencyMarker getCurrentLatencyMarker() {
		return currentLatencyMarker;
	}

	public long getCheckpointIdOfPrepareSnapshotPreBarrier() {
		return checkpointIdOfPrepareSnapshotPreBarrier;
	}

	public boolean isSnapshotStateExecuted() {
		return isSnapshotStateExecuted;
	}

	public long getCheckpointIdOfNotifyCheckpointComplete() {
		return checkpointIdOfNotifyCheckpointComplete;
	}

	public long getCheckpointIdOfNotifyCheckpointAborted() {
		return checkpointIdOfNotifyCheckpointAborted;
	}

	public boolean isEnd() {
		return isEnd;
	}

	public boolean isDisposed() {
		return isDisposed;
	}

	public boolean isClosed() {
		return isClosed;
	}
}
