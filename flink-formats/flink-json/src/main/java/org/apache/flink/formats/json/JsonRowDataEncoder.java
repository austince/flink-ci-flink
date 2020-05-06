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

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Json {@link Encoder} to encode a {@link RowData}.
 */
public class JsonRowDataEncoder implements Encoder<RowData> {

	private static final long serialVersionUID = 1L;
	private static final String DEFAULT_CHARSET_NAME = "UTF-8";
	private static final String DEFAULT_LINE_DELIMITER = "\n";
	private transient Charset charset;

	private final JsonRowDataSerializationSchema serializationSchema;

	private JsonRowDataEncoder(RowType rowType) {
		this.serializationSchema = new JsonRowDataSerializationSchema(
			Preconditions.checkNotNull(rowType));
	}

	@Override
	public void encode(RowData element, OutputStream stream) throws IOException {
		if (charset == null) {
			charset = Charset.forName(DEFAULT_CHARSET_NAME);
		}
		stream.write(serializationSchema.serialize(element));
		stream.write(DEFAULT_LINE_DELIMITER.getBytes(charset));
	}

	public static Builder builder(TableSchema tableSchema) {
		return new Builder(tableSchema);
	}

	/**
	 * Builder for {@link JsonRowDataEncoder}.
	 */
	public static class Builder {
		private RowType rowType;

		private Builder(TableSchema tableSchema) {
			checkNotNull(tableSchema, "Table schema must not be null.");
			LogicalType[] logicalTypes = Arrays.asList(tableSchema.getFieldDataTypes())
				.stream().map(DataType::getLogicalType).toArray(LogicalType[]::new);
			this.rowType = RowType.of(logicalTypes, tableSchema.getFieldNames());
		}

		public JsonRowDataEncoder build() {
			return new JsonRowDataEncoder(rowType);
		}
	}
}
