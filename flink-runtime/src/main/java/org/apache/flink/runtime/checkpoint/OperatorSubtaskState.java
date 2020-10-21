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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.state.AbstractChannelStateHandle.collectUniqueDelegates;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates the state for one parallel instance of an operator. The complete state of a (logical)
 * operator (e.g. a flatmap operator) consists of the union of all {@link OperatorSubtaskState}s from all
 * parallel tasks that physically execute parallelized, physical instances of the operator.
 *
 * <p>The full state of the logical operator is represented by {@link OperatorState} which consists of
 * {@link OperatorSubtaskState}s.
 *
 * <p>Typically, we expect all collections in this class to be of size 0 or 1, because there is up to one state handle
 * produced per state type (e.g. managed-keyed, raw-operator, ...). In particular, this holds when taking a snapshot.
 * The purpose of having the state handles in collections is that this class is also reused in restoring state.
 * Under normal circumstances, the expected size of each collection is still 0 or 1, except for scale-down. In
 * scale-down, one operator subtask can become responsible for the state of multiple previous subtasks. The collections
 * can then store all the state handles that are relevant to build up the new subtask state.
 */
public class OperatorSubtaskState implements CompositeStateHandle {

	private static final Logger LOG = LoggerFactory.getLogger(OperatorSubtaskState.class);

	private static final long serialVersionUID = -2394696997971923995L;

	/**
	 * Snapshot from the {@link org.apache.flink.runtime.state.OperatorStateBackend}.
	 */
	private final StateObjectCollection<OperatorStateHandle> managedOperatorState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.OperatorStateCheckpointOutputStream}.
	 */
	private final StateObjectCollection<OperatorStateHandle> rawOperatorState;

	/**
	 * Snapshot from {@link org.apache.flink.runtime.state.KeyedStateBackend}.
	 */
	private final StateObjectCollection<KeyedStateHandle> managedKeyedState;

	/**
	 * Snapshot written using {@link org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream}.
	 */
	private final StateObjectCollection<KeyedStateHandle> rawKeyedState;

	private final StateObjectCollection<InputChannelStateHandle> inputChannelState;

	private final StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState;

	/**
	 * The subpartitions mappings per partition set when the output operator for a partition was rescaled. The key is
	 * the partition id and the value contains all subtask indexes of the output operator before rescaling.
	 */
	private final VirtualChannelMapping inputChannelMapping;

	/**
	 * The input channel mappings per input set when the input operator for a gate was rescaled. The key is
	 * the gate index and the value contains all subtask indexes of the input operator before rescaling.
	 */
	private final VirtualChannelMapping outputChannelMapping;

	/**
	 * The state size. This is also part of the deserialized state handle.
	 * We store it here in order to not deserialize the state handle when
	 * gathering stats.
	 */
	private final long stateSize;

	private OperatorSubtaskState(
			StateObjectCollection<OperatorStateHandle> managedOperatorState,
			StateObjectCollection<OperatorStateHandle> rawOperatorState,
			StateObjectCollection<KeyedStateHandle> managedKeyedState,
			StateObjectCollection<KeyedStateHandle> rawKeyedState,
			StateObjectCollection<InputChannelStateHandle> inputChannelState,
			StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState,
			VirtualChannelMapping inputChannelMapping,
			VirtualChannelMapping outputChannelMapping) {

		this.managedOperatorState = checkNotNull(managedOperatorState);
		this.rawOperatorState = checkNotNull(rawOperatorState);
		this.managedKeyedState = checkNotNull(managedKeyedState);
		this.rawKeyedState = checkNotNull(rawKeyedState);
		this.inputChannelState = checkNotNull(inputChannelState);
		this.resultSubpartitionState = checkNotNull(resultSubpartitionState);
		this.inputChannelMapping = inputChannelMapping;
		this.outputChannelMapping = outputChannelMapping;

		long calculateStateSize = managedOperatorState.getStateSize();
		calculateStateSize += rawOperatorState.getStateSize();
		calculateStateSize += managedKeyedState.getStateSize();
		calculateStateSize += rawKeyedState.getStateSize();
		calculateStateSize += inputChannelState.getStateSize();
		calculateStateSize += resultSubpartitionState.getStateSize();
		stateSize = calculateStateSize;
	}

	@VisibleForTesting
	OperatorSubtaskState() {
		this(StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			StateObjectCollection.empty(),
			VirtualChannelMapping.NO_MAPPING,
			VirtualChannelMapping.NO_MAPPING);
	}

	// --------------------------------------------------------------------------------------------

	public StateObjectCollection<OperatorStateHandle> getManagedOperatorState() {
		return managedOperatorState;
	}

	public StateObjectCollection<OperatorStateHandle> getRawOperatorState() {
		return rawOperatorState;
	}

	public StateObjectCollection<KeyedStateHandle> getManagedKeyedState() {
		return managedKeyedState;
	}

	public StateObjectCollection<KeyedStateHandle> getRawKeyedState() {
		return rawKeyedState;
	}

	public StateObjectCollection<InputChannelStateHandle> getInputChannelState() {
		return inputChannelState;
	}

	public StateObjectCollection<ResultSubpartitionStateHandle> getResultSubpartitionState() {
		return resultSubpartitionState;
	}

	public VirtualChannelMapping getInputChannelMapping() {
		return inputChannelMapping;
	}

	public VirtualChannelMapping getOutputChannelMapping() {
		return outputChannelMapping;
	}

	@Override
	public void discardState() {
		try {
			List<StateObject> toDispose =
				new ArrayList<>(
					managedOperatorState.size() +
						rawOperatorState.size() +
						managedKeyedState.size() +
						rawKeyedState.size() +
						inputChannelState.size() +
						resultSubpartitionState.size());
			toDispose.addAll(managedOperatorState);
			toDispose.addAll(rawOperatorState);
			toDispose.addAll(managedKeyedState);
			toDispose.addAll(rawKeyedState);
			toDispose.addAll(collectUniqueDelegates(inputChannelState, resultSubpartitionState));
			StateUtil.bestEffortDiscardAllStateObjects(toDispose);
		} catch (Exception e) {
			LOG.warn("Error while discarding operator states.", e);
		}
	}

	@Override
	public void registerSharedStates(SharedStateRegistry sharedStateRegistry) {
		registerSharedState(sharedStateRegistry, managedKeyedState);
		registerSharedState(sharedStateRegistry, rawKeyedState);
	}

	private static void registerSharedState(
		SharedStateRegistry sharedStateRegistry,
		Iterable<KeyedStateHandle> stateHandles) {
		for (KeyedStateHandle stateHandle : stateHandles) {
			if (stateHandle != null) {
				stateHandle.registerSharedStates(sharedStateRegistry);
			}
		}
	}

	@Override
	public long getStateSize() {
		return stateSize;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		OperatorSubtaskState that = (OperatorSubtaskState) o;

		if (getStateSize() != that.getStateSize()) {
			return false;
		}
		if (!getManagedOperatorState().equals(that.getManagedOperatorState())) {
			return false;
		}
		if (!getRawOperatorState().equals(that.getRawOperatorState())) {
			return false;
		}
		if (!getManagedKeyedState().equals(that.getManagedKeyedState())) {
			return false;
		}
		if (!getInputChannelState().equals(that.getInputChannelState())) {
			return false;
		}
		if (!getResultSubpartitionState().equals(that.getResultSubpartitionState())) {
			return false;
		}
		return getRawKeyedState().equals(that.getRawKeyedState());
	}

	@Override
	public int hashCode() {
		int result = getManagedOperatorState().hashCode();
		result = 31 * result + getRawOperatorState().hashCode();
		result = 31 * result + getManagedKeyedState().hashCode();
		result = 31 * result + getRawKeyedState().hashCode();
		result = 31 * result + getInputChannelState().hashCode();
		result = 31 * result + getResultSubpartitionState().hashCode();
		result = 31 * result + (int) (getStateSize() ^ (getStateSize() >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "SubtaskState{" +
			"operatorStateFromBackend=" + managedOperatorState +
			", operatorStateFromStream=" + rawOperatorState +
			", keyedStateFromBackend=" + managedKeyedState +
			", keyedStateFromStream=" + rawKeyedState +
			", inputChannelState=" + inputChannelState +
			", resultSubpartitionState=" + resultSubpartitionState +
			", stateSize=" + stateSize +
			'}';
	}

	public boolean hasState() {
		return managedOperatorState.hasState()
			|| rawOperatorState.hasState()
			|| managedKeyedState.hasState()
			|| rawKeyedState.hasState()
			|| inputChannelState.hasState()
			|| resultSubpartitionState.hasState();
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * The builder for a new {@link OperatorSubtaskState} which can be obtained by {@link #builder()}.
	 */
	public static class Builder {
		private StateObjectCollection<OperatorStateHandle> managedOperatorState = StateObjectCollection.empty();
		private StateObjectCollection<OperatorStateHandle> rawOperatorState = StateObjectCollection.empty();
		private StateObjectCollection<KeyedStateHandle> managedKeyedState = StateObjectCollection.empty();
		private StateObjectCollection<KeyedStateHandle> rawKeyedState = StateObjectCollection.empty();
		private StateObjectCollection<InputChannelStateHandle> inputChannelState = StateObjectCollection.empty();
		private StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState = StateObjectCollection.empty();
		private VirtualChannelMapping inputChannelMappings = VirtualChannelMapping.NO_MAPPING;
		private VirtualChannelMapping outputChannelMappings = VirtualChannelMapping.NO_MAPPING;

		private Builder() {
		}

		public Builder setManagedOperatorState(StateObjectCollection<OperatorStateHandle> managedOperatorState) {
			this.managedOperatorState = checkNotNull(managedOperatorState);
			return this;
		}

		public Builder setManagedOperatorState(OperatorStateHandle managedOperatorState) {
			return setManagedOperatorState(StateObjectCollection.singleton(checkNotNull(managedOperatorState)));
		}

		public Builder setRawOperatorState(StateObjectCollection<OperatorStateHandle> rawOperatorState) {
			this.rawOperatorState = checkNotNull(rawOperatorState);
			return this;
		}

		public Builder setRawOperatorState(OperatorStateHandle rawOperatorState) {
			return setRawOperatorState(StateObjectCollection.singleton(checkNotNull(rawOperatorState)));
		}

		public Builder setManagedKeyedState(StateObjectCollection<KeyedStateHandle> managedKeyedState) {
			this.managedKeyedState = checkNotNull(managedKeyedState);
			return this;
		}

		public Builder setManagedKeyedState(KeyedStateHandle managedKeyedState) {
			return setManagedKeyedState(StateObjectCollection.singleton(checkNotNull(managedKeyedState)));
		}

		public Builder setRawKeyedState(StateObjectCollection<KeyedStateHandle> rawKeyedState) {
			this.rawKeyedState = checkNotNull(rawKeyedState);
			return this;
		}

		public Builder setRawKeyedState(KeyedStateHandle rawKeyedState) {
			return setRawKeyedState(StateObjectCollection.singleton(checkNotNull(rawKeyedState)));
		}

		public Builder setInputChannelState(StateObjectCollection<InputChannelStateHandle> inputChannelState) {
			this.inputChannelState = checkNotNull(inputChannelState);
			return this;
		}

		public Builder setResultSubpartitionState(StateObjectCollection<ResultSubpartitionStateHandle> resultSubpartitionState) {
			this.resultSubpartitionState = checkNotNull(resultSubpartitionState);
			return this;
		}

		public Builder setInputChannelMappings(VirtualChannelMapping inputChannelMappings) {
			this.inputChannelMappings = checkNotNull(inputChannelMappings);
			return this;
		}

		public Builder setOutputChannelMappings(VirtualChannelMapping outputChannelMappings) {
			this.outputChannelMappings = checkNotNull(outputChannelMappings);
			return this;
		}

		public OperatorSubtaskState build() {
			return new OperatorSubtaskState(
				managedOperatorState,
				rawOperatorState,
				managedKeyedState,
				rawKeyedState,
				inputChannelState,
				resultSubpartitionState,
				inputChannelMappings,
				outputChannelMappings);
		}
	}

	/**
	 * Captures ambiguous mappings of old channels to new channels.
	 *
	 * <p>For inputs, this mapping implies the following:
	 * <li>
	 *     <ul>{@link #oldTaskInstances} is set when there is a rescale on this task potentially leading to different
	 *     key groups. Upstream task has a corresponding {@link #partitionMappings} where it sends data over
	 *     virtual channel while specifying the channel index in the VirtualChannelSelector. This subtask then
	 *     demultiplexes over the virtual subtask index.</ul>
	 *     <ul>{@link #partitionMappings} is set when there is a downscale of the upstream task. Upstream task has
	 *     a corresponding {@link #oldTaskInstances} where it sends data over virtual channel while specifying the
	 *     subtask index in the VirtualChannelSelector. This subtask then demultiplexes over channel indexes.</ul>
	 * </li>
	 *
	 * <p>For outputs, it's vice-versa. The information must be kept in sync but they are used in opposite ways for
	 * multiplexing/demultiplexing.
	 *
	 * <p>Note that in the common rescaling case both information is set and need to be simultaneously used. If the
	 * input subtask subsumes the state of 3 old subtasks and a channel corresponds to 2 old channels, then there are
	 * 6 virtual channels to be demultiplexed.
	 */
	public static class VirtualChannelMapping implements Serializable {
		public static final PartitionMapping NO_CHANNEL_MAPPING = new PartitionMapping(emptyList());
		public static final List<PartitionMapping> NO_PARTITIONS = emptyList();
		public static final BitSet NO_SUBTASKS = new BitSet();
		public static final VirtualChannelMapping NO_MAPPING = new VirtualChannelMapping(NO_SUBTASKS, NO_PARTITIONS);

		/**
		 * Set when several operator instances are merged into one.
		 */
		private final BitSet oldTaskInstances;

		/**
		 * Set when channels are merged because the connected operator has been rescaled.
		 */
		private final List<PartitionMapping> partitionMappings;

		public VirtualChannelMapping(BitSet oldTaskInstances, List<PartitionMapping> partitionMappings) {
			this.oldTaskInstances = oldTaskInstances;
			this.partitionMappings = partitionMappings;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			final VirtualChannelMapping that = (VirtualChannelMapping) o;
			return oldTaskInstances.equals(that.oldTaskInstances) &&
				partitionMappings.equals(that.partitionMappings);
		}

		public int[] getOldTaskInstances(int defaultSubtask) {
			return oldTaskInstances.equals(NO_SUBTASKS) ?
				new int[] {defaultSubtask} :
				oldTaskInstances.stream().toArray();
		}

		public PartitionMapping getPartitionMapping(int partitionIndex) {
			if (partitionMappings.isEmpty()) {
				return NO_CHANNEL_MAPPING;
			}
			return partitionMappings.get(partitionIndex);
		}

		@Override
		public int hashCode() {
			return Objects.hash(oldTaskInstances, partitionMappings);
		}

		@Override
		public String toString() {
			return "VirtualChannelMapping{" +
				"oldTaskInstances=" + oldTaskInstances +
				", partitionMappings=" + partitionMappings +
				'}';
		}
	}

	/**
	 * Contains the fine-grain channel mappings that occur when a connected operator has been rescaled.
	 */
	public static class PartitionMapping implements Serializable {

		/**
		 * For each new channel (=index), all old channels are set.
		 */
		private final List<BitSet> newToOldChannelIndexes;

		/**
		 * For each old channel (=index), all new channels are set. Lazily calculated to keep
		 * {@link OperatorSubtaskState} small in terms of serialization cost.
		 */
		private transient List<BitSet> oldToNewChannelIndexes;

		public PartitionMapping(List<BitSet> newToOldChannelIndexes) {
			this.newToOldChannelIndexes = newToOldChannelIndexes;
		}

		public int[] getNewChannelIndexes(int oldChannelIndex) {
			if (newToOldChannelIndexes.isEmpty()) {
				return new int[]{oldChannelIndex};
			}
			if (oldToNewChannelIndexes == null) {
				oldToNewChannelIndexes = invert(newToOldChannelIndexes);
			}
			return oldToNewChannelIndexes.get(oldChannelIndex).stream().toArray();
		}

		public int[] getOldChannelIndexes(int newChannelIndex) {
			if (newToOldChannelIndexes.isEmpty()) {
				return new int[]{newChannelIndex};
			}
			return newToOldChannelIndexes.get(newChannelIndex).stream().toArray();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			final PartitionMapping that = (PartitionMapping) o;
			return newToOldChannelIndexes.equals(that.newToOldChannelIndexes);
		}

		@Override
		public int hashCode() {
			return Objects.hash(newToOldChannelIndexes);
		}

		@Override
		public String toString() {
			return "PartitionMapping{" +
				"newToOldChannelIndexes=" + newToOldChannelIndexes +
				'}';
		}

		static List<BitSet> invert(List<BitSet> mapping) {
			final List<BitSet> inverted = new ArrayList<>();
			for (int index = 0; index < mapping.size(); index++) {
				final BitSet bitSet = mapping.get(index);
				for (int invIndex = bitSet.nextSetBit(0); invIndex != -1; invIndex = bitSet.nextSetBit(invIndex + 1)) {
					while (invIndex >= inverted.size()) {
						inverted.add(new BitSet());
					}
					inverted.get(invIndex).set(index);
				}
			}
			return inverted;
		}

	}
}
