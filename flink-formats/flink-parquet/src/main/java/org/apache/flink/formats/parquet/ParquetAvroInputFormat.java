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

package org.apache.flink.formats.parquet;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.types.Row;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link ParquetInputFormat} to read records from Parquet files and convert
 * them to Avro GenericRecord. Usage:
 *
 * <pre>{@code
 * final ParquetAvroInputFormat inputFormat = new ParquetAvroInputFormat(new Path(filePath), parquetSchema);
 * DataSource<GenericRecord> source = env.createInput(inputFormat, new GenericRecordAvroTypeInfo(inputFormat.getAvroSchema()));
 *
 * }</pre>
 */
public class ParquetAvroInputFormat extends ParquetInputFormat<GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetAvroInputFormat.class);

    private Schema avroSchema;
    private AvroRowSerializationSchema avroRowSerializationSchema;

    public ParquetAvroInputFormat(Path filePath, MessageType messageType) {
        super(filePath, messageType);
        avroSchema = new AvroSchemaConverter().convert(messageType);
        avroRowSerializationSchema = new AvroRowSerializationSchema(avroSchema.toString());
    }

    @Override
    public void selectFields(String[] fieldNames) {
        avroSchema = getProjectedSchema(fieldNames, avroSchema);
        avroRowSerializationSchema = new AvroRowSerializationSchema(avroSchema.toString());
        super.selectFields(fieldNames);
    }

    @Override
    protected GenericRecord convert(Row row) {
        return avroRowSerializationSchema.convertRowToAvroRecord(avroSchema, row);
    }

    private Schema getProjectedSchema(String[] projectedFieldNames, Schema sourceAvroSchema) {
        // Avro fields need to be in the same order than row field for compatibility with flink 1.12
        // (row
        // fields not accessible by name). Row field order now is the order of
        // ParquetInputFormat.selectFields()
        // for flink 1.13+ where row fields are accessible by name, we will match the fields names
        // between avro schema and row schema.

        List<Schema.Field> projectedFields = new ArrayList<>();
        for (String fieldName : projectedFieldNames) {
            final Schema.Field f = sourceAvroSchema.getField(fieldName);
            if (f != null) {
                projectedFields.add(
                        new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal()));
            }
        }

        Schema projectedAvroSchema =
                Schema.createRecord(sourceAvroSchema.getName() + "_projected", null, null, false);
        projectedAvroSchema.setFields(projectedFields);
        return projectedAvroSchema;
    }

    public Schema getAvroSchema() {
        return avroSchema;
    }
}
