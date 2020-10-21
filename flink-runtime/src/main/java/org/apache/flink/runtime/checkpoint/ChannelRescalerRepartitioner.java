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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.api.writer.ChannelStateRescaler;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A repartitioner that assigns the same channel state to multiple subtasks according to some mapping.
 *
 * <p>The replicated data will then be filtered before processing the record.
 *
 * <p>Note that channel mappings are cached for the same parallelism changes.
 */
public class ChannelRescalerRepartitioner<T> implements OperatorStateRepartitioner<T> {
	private final ChannelStateRescaler channelStateRescaler;
	private final Map<Tuple2<Integer, Integer>, Map<Integer, Set<Integer>>> newToOldMappingCache = new HashMap<>(2);

	public ChannelRescalerRepartitioner(ChannelStateRescaler channelStateRescaler) {
		this.channelStateRescaler = channelStateRescaler;
	}

	private static <T> List<T> getOldState(List<List<T>> previousParallelSubtaskStates, Set<Integer> oldIndexes) {
		switch (oldIndexes.size()) {
			case 0:
				return Collections.emptyList();
			case 1:
				return previousParallelSubtaskStates.get(Iterables.getOnlyElement(oldIndexes));
			default:
				return oldIndexes.stream()
					.flatMap(oldIndex -> previousParallelSubtaskStates.get(oldIndex).stream())
					.collect(Collectors.toList());
		}
	}

	protected Map<Integer, Set<Integer>> createNewToOldMapping(int oldParallelism, int newParallelism) {
		return IntStream.range(0, newParallelism).boxed().
			collect(Collectors.toMap(
				Function.identity(),
				channelIndex -> channelStateRescaler.getOldChannels(
					channelIndex,
					oldParallelism,
					newParallelism)));
	}

	@Override
	public List<List<T>> repartitionState(
			List<List<T>> previousParallelSubtaskStates,
			int oldParallelism,
			int newParallelism) {
		final Map<Integer, Set<Integer>> newToOldMapping = getNewToOldMapping(oldParallelism, newParallelism);
		return IntStream.range(0, newParallelism)
			.mapToObj(newIndex -> getOldState(previousParallelSubtaskStates, newToOldMapping.get(newIndex)))
			.collect(Collectors.toList());
	}

	public Map<Integer, Set<Integer>> getNewToOldMapping(int oldParallelism, int newParallelism) {
		return newToOldMappingCache.computeIfAbsent(
			new Tuple2<>(oldParallelism, newParallelism),
			(unused) -> createNewToOldMapping(oldParallelism, newParallelism));
	}
}
