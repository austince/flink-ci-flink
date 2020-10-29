/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.util.Objects;

/**
 * An event that is used to demultiplex virtual channels over the same physical channel.
 */
public final class VirtualChannelSelector extends RuntimeEvent {

	private final int taskIndex;
	private final int channelIndex;

	public VirtualChannelSelector(int taskIndex, int channelIndex) {
		this.taskIndex = taskIndex;
		this.channelIndex = channelIndex;
	}

	// ------------------------------------------------------------------------
	// Serialization
	// ------------------------------------------------------------------------

	@Override
	public void write(DataOutputView out) throws IOException {
		throw new UnsupportedOperationException("This method should never be called");
	}

	@Override
	public void read(DataInputView in) throws IOException {
		throw new UnsupportedOperationException("This method should never be called");
	}

	// ------------------------------------------------------------------------

	public int getTaskIndex() {
		return taskIndex;
	}

	public int getChannelIndex() {
		return channelIndex;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final VirtualChannelSelector that = (VirtualChannelSelector) o;
		return taskIndex == that.taskIndex &&
			channelIndex == that.channelIndex;
	}

	@Override
	public int hashCode() {
		return Objects.hash(taskIndex, channelIndex);
	}

	@Override
	public String toString() {
		return "VirtualChannelSelector{" +
			"taskIndex=" + taskIndex +
			", channelIndex=" + channelIndex +
			'}';
	}
}
