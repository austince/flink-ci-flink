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

package org.apache.flink.client.cli;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.Optional;

/**
 * Describes a client configuration parameter.
 */
public class ClientOptions {

	public static final ConfigOption<Duration> EMBEDDED_RPC_TIMEOUT =
			ConfigOptions.key("execution.embedded-rpc-timeout")
					.durationType()
					.defaultValue(Duration.ofMillis(60 * 60 * 1000))
					.withDescription("The rpc timeout (in ms) when executing applications in \"Application Mode\". " +
							"This affects all rpc's available through the Job Client and job submission.");

	public static final ConfigOption<Duration> EMBEDDED_RPC_RETRY_PERIOD =
			ConfigOptions.key("execution.embedded-rpc-retry-period")
					.durationType()
					.defaultValue(Duration.ofMillis(2000))
					.withDescription("The retry period (in ms) between consecutive attempts to get the job status " +
							"when executing applications in \"Application Mode\".");

	public static final ConfigOption<Duration> CLIENT_TIMEOUT = ConfigOptions
			.key("client.timeout")
			.durationType()
			.defaultValue(Duration.ofSeconds(60))
			.withDescription("Timeout on the client side.");

	public static Duration getClientTimeout(Configuration configuration) {
		Optional<Duration> timeoutOptional = configuration.getOptional(CLIENT_TIMEOUT);
		if (timeoutOptional.isPresent()) {
			return timeoutOptional.get();
		} else {
			Optional<String> akkaClientTimeout = configuration.getOptional(AkkaOptions.AKKA_CLIENT_TIMEOUT);
			if (akkaClientTimeout.isPresent()) {
				return TimeUtils.parseDuration(akkaClientTimeout.get());
			} else {
				return CLIENT_TIMEOUT.defaultValue();
			}
		}
	}

}
