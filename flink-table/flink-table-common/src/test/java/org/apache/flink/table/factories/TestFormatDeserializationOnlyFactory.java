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

package org.apache.flink.table.factories;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;

import java.util.Collections;
import java.util.Set;

/**
 * Tests implementations for  {@link DeserializationFormatFactory}.
 */
public class TestFormatDeserializationOnlyFactory implements DeserializationFormatFactory {

	public static final String IDENTIFIER = "test-format-deserialization-only";

	/**
	 * Creates a format from the given context and format options.
	 *
	 * <p>The format options have been projected to top-level options (e.g. from {@code key.format.ignore-errors}
	 * to {@code format.ignore-errors}).
	 *
	 * @param context
	 * @param formatOptions
	 */
	@Override
	public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
		return null;
	}

	/**
	 * Returns a unique identifier among same factory interfaces.
	 *
	 * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code kafka}). If
	 * multiple factories exist for different versions, a version should be appended using "-" (e.g. {@code kafka-0.10}).
	 */
	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	/**
	 * Returns a set of {@link ConfigOption} that an implementation of this factory requires in addition to
	 * {@link #optionalOptions()}.
	 *
	 * <p>See the documentation of {@link Factory} for more information.
	 */
	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	/**
	 * Returns a set of {@link ConfigOption} that an implementation of this factory consumes in addition to
	 * {@link #requiredOptions()}.
	 *
	 * <p>See the documentation of {@link Factory} for more information.
	 */
	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return Collections.emptySet();
	}
}
