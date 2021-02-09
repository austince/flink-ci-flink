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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Helper class to create {@link InflightDataRescalingDescriptor} and {@link
 * RescaledChannelsMapping}.
 */
public class InflightDataRescalingDescriptorUtil {
    public static RescaledChannelsMapping mapping(Set<Integer>... channelMappings) {
        Map<Integer, Set<Integer>> mappings = new HashMap<>();
        for (int newChannelIndex = 0; newChannelIndex < channelMappings.length; newChannelIndex++) {
            mappings.put(newChannelIndex, channelMappings[newChannelIndex]);
        }
        return new RescaledChannelsMapping(mappings);
    }

    public static Set<Integer> set(int... indexes) {
        return IntStream.of(indexes).boxed().collect(Collectors.toSet());
    }
}
