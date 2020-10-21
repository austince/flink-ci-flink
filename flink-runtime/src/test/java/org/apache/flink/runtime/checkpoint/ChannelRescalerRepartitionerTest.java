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

import org.apache.flink.runtime.io.network.api.writer.ChannelStateRescaler;

import org.junit.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link ChannelRescalerRepartitioner}.
 */
public class ChannelRescalerRepartitionerTest {
	@Test
	public void testBroadcastChannelMappingOnScaleDown() {
		assertMappingEquals(new int[][] {{0, 1, 2}, {0, 1, 2}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.BROADCAST).getNewToOldMapping(3, 2));
	}

	@Test
	public void testBroadcastChannelMappingOnNoScale() {
		// this may be a bit surprising, but the optimization should be done on call-site
		assertMappingEquals(new int[][] {{0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.BROADCAST).getNewToOldMapping(3, 3));
	}

	@Test
	public void testBroadcastChannelMappingOnScaleUp() {
		assertMappingEquals(new int[][] {{0, 1, 2}, {0, 1, 2}, {0, 1, 2}, {0, 1, 2}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.BROADCAST).getNewToOldMapping(3, 4));
	}

	@Test
	public void testBroadcastRedistributeOnScaleDown() {
		List<List<String>> oldStates = Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"));

		assertEquals(Arrays.asList(
				Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1"),
				Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1")),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.BROADCAST).repartitionState(oldStates, 3, 2));
	}

	@Test
	public void testBroadcastRedistributeOnNoScale() {
		List<List<String>> oldStates = Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"));

		assertEquals(Arrays.asList(
				Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1"),
				Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1"),
				Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1")),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.BROADCAST).repartitionState(oldStates, 3, 3));
	}

	@Test
	public void testBroadcastRedistributeOnScaleUp() {
		List<List<String>> oldStates = Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"));

		assertEquals(Arrays.asList(
				Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1"),
				Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1"),
				Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1"),
				Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1")),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.BROADCAST).repartitionState(oldStates, 3, 4));
	}

	@Test
	public void testRangeSelectorChannelMappingOnScaleDown() {
		// 3 partitions: [0; 43) [43; 87) [87; 128)
		// 2 partitions: [0; 64) [64; 128)
		assertMappingEquals(new int[][] {{0, 1}, {1, 2}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.RANGE).getNewToOldMapping(3, 2));

		assertMappingEquals(new int[][] {{0, 1, 2, 3, 4}, {5, 6, 7, 8, 9}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.RANGE).getNewToOldMapping(10, 2));

		assertMappingEquals(new int[][] {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.RANGE).getNewToOldMapping(10, 1));
	}

	@Test
	public void testRangeSelectorChannelMappingOnNoScale() {
		assertMappingEquals(new int[][] {{0}, {1}, {2}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.RANGE).getNewToOldMapping(3, 3));
	}

	@Test
	public void testRangeSelectorChannelMappingOnScaleUp() {
		assertMappingEquals(new int[][] {{0}, {0, 1}, {1, 2}, {2}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.RANGE).getNewToOldMapping(3, 4));

		assertMappingEquals(new int[][] {{0}, {0}, {0, 1}, {1}, {1, 2}, {2}, {2}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.RANGE).getNewToOldMapping(3, 7));
	}

	@Test
	public void testRangeSelectorRedistributeOnScaleDown() {
		List<List<String>> oldStates = Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"));

		assertEquals(Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1", "sub1state0"),
			Arrays.asList("sub1state0", "sub2state0", "sub2state1")),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.RANGE).repartitionState(oldStates, 3, 2));
	}

	@Test
	public void testRangeSelectorRedistributeOnNoScale() {
		List<List<String>> oldStates = Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"));

		assertEquals(Arrays.asList(
				Arrays.asList("sub0state0", "sub0state1"),
				Arrays.asList("sub1state0"),
				Arrays.asList("sub2state0", "sub2state1")),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.RANGE).repartitionState(oldStates, 3, 3));
	}

	@Test
	public void testRangeSelectorRedistributeOnScaleUp() {
		List<List<String>> oldStates = Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"));

		assertEquals(Arrays.asList(
				Arrays.asList("sub0state0", "sub0state1"),
				Arrays.asList("sub0state0", "sub0state1", "sub1state0"),
				Arrays.asList("sub1state0", "sub2state0", "sub2state1"),
				Arrays.asList("sub2state0", "sub2state1")),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.RANGE).repartitionState(oldStates, 3, 4));
	}

	@Test
	public void testRoundRobinChannelMappingOnScaleDown() {
		assertMappingEquals(new int[][] {{0, 4, 8}, {1, 5, 9}, {2, 6}, {3, 7}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(10, 4));

		assertMappingEquals(new int[][] {{0, 4}, {1}, {2}, {3}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(5, 4));

		assertMappingEquals(new int[][] {{0, 2, 4}, {1, 3}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(5, 2));

		assertMappingEquals(new int[][] {{0, 1, 2, 3, 4}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(5, 1));
	}

	@Test
	public void testRoundRobinChannelMappingOnNoScale() {
		assertMappingEquals(new int[][] {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(10, 10));

		assertMappingEquals(new int[][] {{0}, {1}, {2}, {3}, {4}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(5, 5));

		assertMappingEquals(new int[][] {{0}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(1, 1));
	}

	@Test
	public void testRoundRobinChannelMappingOnScaleUp() {
		assertMappingEquals(new int[][] {{0}, {1}, {2}, {3}, {}, {}, {}, {}, {}, {}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(4, 10));

		assertMappingEquals(new int[][] {{0}, {1}, {2}, {3}, {}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(4, 5));

		assertMappingEquals(new int[][] {{0}, {1}, {}, {}, {}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(2, 5));

		assertMappingEquals(new int[][] {{0}, {}, {}, {}, {}},
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).getNewToOldMapping(1, 5));
	}

	@Test
	public void testRoundRobinRedistributeOnScaleDown() {
		List<List<String>> oldStates = Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"));

		assertEquals(Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1", "sub2state0", "sub2state1"),
			Arrays.asList("sub1state0")),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).repartitionState(oldStates, 3, 2));

		assertEquals(Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1", "sub1state0", "sub2state0", "sub2state1")),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).repartitionState(oldStates, 3, 1));
	}

	@Test
	public void testRoundRobinRedistributeOnNoScale() {
		List<List<String>> oldStates = Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"));

		assertEquals(Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1")),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).repartitionState(oldStates, 3, 3));
	}

	@Test
	public void testRoundRobinRedistributeOnScaleUp() {
		List<List<String>> oldStates = Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"));

		assertEquals(Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"),
			Arrays.asList()),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).repartitionState(oldStates, 3, 4));

		assertEquals(Arrays.asList(
			Arrays.asList("sub0state0", "sub0state1"),
			Arrays.asList("sub1state0"),
			Arrays.asList("sub2state0", "sub2state1"),
			Arrays.asList(),
			Arrays.asList()),
			new ChannelRescalerRepartitioner<String>(ChannelStateRescaler.ROUND_ROBIN).repartitionState(oldStates, 3, 5));
	}

	static void assertMappingEquals(int[][] expected, List<BitSet> actual) {
		assertEquals(Arrays.stream(expected)
			.map(indexes -> {
				final BitSet bitSet = new BitSet();
				Arrays.stream(indexes).forEach(bitSet::set);
				return bitSet;
			})
			.collect(Collectors.toList()), actual);
	}
}
