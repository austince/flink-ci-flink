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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.PartitionPathUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link JsonRowDataFileInputFormat} is responsible to read {@link RowData} records
 * from json format files.
 */
public class JsonRowDataFileInputFormat extends AbstractJsonFileInputFormat<RowData> {

	private static final long serialVersionUID = 1L;

	private final List<DataType> fieldTypes;
	private final List<String> fieldNames;
	private final int[] selectFields;
	private final List<String> partitionKeys;
	private final String defaultPartValue;
	private final long limit;
	private final List<String> jsonSelectFieldNames;
	private final int[] jsonFieldMapping;
	private final JsonRowDataDeserializationSchema deserializationSchema;

	private transient boolean end;
	private transient long emitted;
	// reuse object for per record
	private transient GenericRowData rowData;
	private transient InputStreamReader inputStreamReader;
	private transient BufferedReader reader;
	private transient Charset charset;

	private JsonRowDataFileInputFormat(
		Path[] filePaths,
		RowType nonPartitionRowType,
		TypeInformation nonPartitionRowTypeInfo,
		List<DataType> dataTypes,
		List<String> fieldNames,
		int[] selectFields,
		List<String> partitionKeys,
		String defaultPartValue,
		boolean failOnMissingField,
		boolean ignoreParseErrors,
		long limit) {
		super(filePaths);
		this.fieldTypes = dataTypes;
		this.fieldNames = fieldNames;
		this.selectFields = selectFields;
		this.partitionKeys = partitionKeys;
		this.defaultPartValue = defaultPartValue;
		this.limit = limit;
		// project field
		List<String> selectFieldNames = Arrays.stream(selectFields)
			.mapToObj(fieldNames::get)
			.collect(Collectors.toList());
		this.jsonSelectFieldNames = selectFieldNames.stream()
			.filter(name -> !partitionKeys.contains(name)).collect(Collectors.toList());
		this.jsonFieldMapping = jsonSelectFieldNames.stream().mapToInt(selectFieldNames::indexOf).toArray();
		this.deserializationSchema = new JsonRowDataDeserializationSchema(
			nonPartitionRowType,
			nonPartitionRowTypeInfo,
			failOnMissingField,
			ignoreParseErrors);
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
		this.end = false;
		this.inputStreamReader = new InputStreamReader(jsonInputStream);
		this.reader = new BufferedReader(inputStreamReader);
		this.charset = Charset.forName("UTF-8");
		fillPartitionValueForRecord();
		this.emitted = 0L;
	}

	private void fillPartitionValueForRecord() {
		rowData = new GenericRowData(selectFields.length);
		Path path = currentSplit.getPath();
		LinkedHashMap<String, String> partSpec = PartitionPathUtils.extractPartitionSpecFromPath(path);
		for (int i = 0; i < selectFields.length; i++) {
			int selectField = selectFields[i];
			String name = fieldNames.get(selectField);
			if (partitionKeys.contains(name)) {
				String value = partSpec.get(name);
				value = defaultPartValue.equals(value) ? null : value;
				rowData.setField(i, PartitionPathUtils.restorePartValueFromType(value, fieldTypes.get(selectField)));
			}
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return emitted >= limit || end;
	}

	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		GenericRowData returnRecord = rowData;
		String line = reader.readLine();
		if (line == null) {
			this.end = true;
			return null;
		}
		GenericRowData jsonRow = (GenericRowData) deserializationSchema.deserialize(line.getBytes(charset));
		if (jsonRow != null) {
			for (int i = 0; i < jsonSelectFieldNames.size(); i++) {
				returnRecord.setField(jsonFieldMapping[i], jsonRow.getField(i));
			}
		}
		emitted++;
		return returnRecord;
	}

	@Override
	public void close() throws IOException {
		super.close();
		if (reader != null) {
			this.reader.close();
			this.reader = null;
		}
		if (inputStreamReader != null) {
			this.inputStreamReader.close();
			this.inputStreamReader = null;
		}
	}

	public static Builder builder(FileSystemFormatFactory.ReaderContext context, TableSchema tableSchema, List<String> partitionKeys, Path... filePaths) {
		return new Builder(context, tableSchema, partitionKeys, filePaths);
	}

	/**
	 * Builder for {@link JsonRowDataFileInputFormat}.
	 */
	public static class Builder {
		private Path[] filePaths;
		private List<DataType> fieldTypes;
		private List<String> fieldNames;
		private RowType nonPartRowType;
		private TypeInformation nonPartRowTypeInfo;
		private int[] selectFields;
		private List<String> partitionKeys;
		private String defaultPartValue;
		private boolean failOnMissingField;
		private boolean ignoreParseErrors;
		private long limit;

		private Builder(FileSystemFormatFactory.ReaderContext context, TableSchema tableSchema, List<String> partitionKeys, Path... filePaths) {
			checkNotNull(tableSchema, "Table schema must not be null.");
			checkNotNull(partitionKeys, "PartitionKeys must not be null.");
			checkNotNull(filePaths, "File paths must not be null.");
			this.filePaths = filePaths;
			this.partitionKeys = partitionKeys;
			this.fieldTypes = Arrays.asList(tableSchema.getFieldDataTypes());
			this.fieldNames = Arrays.asList(tableSchema.getFieldNames());
			// non partition field
			String[] nonPartFieldNames = fieldNames.stream()
				.filter(name -> !partitionKeys.contains(name)).toArray(String[]::new);
			DataType[] nonPartFieldTypes = Arrays.asList(nonPartFieldNames).stream()
				.map(name -> fieldTypes.get(fieldNames.indexOf(name))).toArray(DataType[]::new);
			TableSchema nonPartSchema = TableSchema.builder().fields(nonPartFieldNames, nonPartFieldTypes).build();

			this.nonPartRowType = RowType.of(
				Arrays.asList(nonPartFieldTypes).stream()
					.map(DataType::getLogicalType)
					.toArray(LogicalType[]::new),
				nonPartFieldNames);
			this.nonPartRowTypeInfo = context.createTypeInformation(nonPartSchema.toRowDataType());
		}

		public Builder setSelectFields(int[] selectFields) {
			this.selectFields = selectFields;
			return this;
		}

		public Builder setDefaultPartValue(String defaultPartValue) {
			this.defaultPartValue = defaultPartValue;
			return this;
		}

		public Builder setFailOnMissingField(boolean failOnMissingField) {
			this.failOnMissingField = failOnMissingField;
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public Builder setLimit(long limit) {
			this.limit = limit;
			return this;
		}

		public JsonRowDataFileInputFormat build() {
			return new JsonRowDataFileInputFormat(
				filePaths,
				nonPartRowType,
				nonPartRowTypeInfo,
				fieldTypes,
				fieldNames,
				selectFields,
				partitionKeys,
				defaultPartValue,
				failOnMissingField,
				ignoreParseErrors,
				limit);
		}
	}
}
