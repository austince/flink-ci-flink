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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;

import java.util.stream.IntStream;

/**
 * Function to implement a custom partition assignment for keys.
 *
 * @param <K> The type of the key to be partitioned.
 */
@Public
@FunctionalInterface
public interface Partitioner<K> extends java.io.Serializable, Function {

	/**
	 * Computes the partition for the given key.
	 *
	 * @param key The key.
	 * @param numPartitions The number of partitions to partition into.
	 * @return The partition index.
	 */
	int partition(K key, int numPartitions);

	/**
	 * Returns all partitions that need to be read to restore the given new partition. The partitioner is then
	 * applied on the key of the restored record to filter all irrelevant records.
	 *
	 * <p>In particular, to create a partition X after rescaling, all partitions returned by this method are fully read
	 * and the key of each record is then fed into {@link #partition(Object, int)} to check if it belongs to X.
	 *
	 * <p>The default implementation states that all partitions need to be scanned and should be overwritten to improve
	 * performance.
	 */
	@PublicEvolving
	default int[] rescaleIntersections(int newPartition, int oldNumPartitions, int newNumPartitions) {
		// any old partition may contain a record that should be in the new partition after rescaling
		return IntStream.range(0, oldNumPartitions).toArray();
	}
}
