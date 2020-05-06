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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FileSystemFormatFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD;
import static org.apache.flink.table.descriptors.JsonValidator.FORMAT_IGNORE_PARSE_ERRORS;

/**
 * Json {@link FileSystemFormatFactory} for file system.
 */
public class JsonFileSystemFormatFactory implements FileSystemFormatFactory {
	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(FORMAT, "json");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		ArrayList<String> properties = new ArrayList<>();
		properties.add(FORMAT_FAIL_ON_MISSING_FIELD);
		properties.add(FORMAT_IGNORE_PARSE_ERRORS);
		return properties;
	}

	@Override
	public InputFormat<RowData, ?> createReader(ReaderContext context) {
		DescriptorProperties properties = getValidatedProperties(context.getFormatProperties());
		JsonRowDataFileInputFormat.Builder builder = JsonRowDataFileInputFormat.builder(
			context,
			context.getSchema(),
			context.getPartitionKeys(),
			context.getPaths());
		builder.setDefaultPartValue(context.getDefaultPartName())
			.setSelectFields(context.getProjectFields())
			.setLimit(context.getPushedDownLimit());
		// format properties
		properties.getOptionalBoolean(FORMAT_FAIL_ON_MISSING_FIELD)
			.ifPresent(builder::setFailOnMissingField);
		properties.getOptionalBoolean(FORMAT_IGNORE_PARSE_ERRORS)
			.ifPresent(builder::setIgnoreParseErrors);

		return builder.build();
	}

	@Override
	public Optional<Encoder<RowData>> createEncoder(WriterContext context) {
		final TableSchema nonPartKeySchema = new TableSchema.Builder()
			.fields(context.getFieldNamesWithoutPartKeys(), context.getFieldTypesWithoutPartKeys())
			.build();

		return Optional.of(JsonRowDataEncoder.builder(nonPartKeySchema).build());
	}

	@Override
	public Optional<BulkWriter.Factory<RowData>> createBulkWriterFactory(WriterContext context) {
		return Optional.empty();
	}

	@Override
	public boolean supportsSchemaDerivation() {
		return true;
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties properties = new DescriptorProperties(true);
		properties.putProperties(propertiesMap);
		properties.validateBoolean(FORMAT_FAIL_ON_MISSING_FIELD, true);
		properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, true);
		return properties;
	}
}
