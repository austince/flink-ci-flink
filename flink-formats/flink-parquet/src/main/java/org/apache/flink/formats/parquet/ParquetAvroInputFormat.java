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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    public ParquetAvroInputFormat(Path filePath, MessageType messageType) {
        super(filePath, messageType);
        avroSchema = new AvroSchemaConverter().convert(messageType);
    }

    @Override
    public void selectFields(String[] fieldNames) {
        // TODO debug, does not work
        super.selectFields(fieldNames);
        avroSchema = getProjectedSchema(fieldNames, avroSchema);
    }

    @Override
    protected GenericRecord convert(Row row) {
        AvroRowSerializationSchema avroRowSerializationSchema =
                new AvroRowSerializationSchema(avroSchema.toString());
        return avroRowSerializationSchema.convertRowToAvroRecord(avroSchema, row);
    }

    private Schema getProjectedSchema(String[] fieldNames, Schema sourceAvroSchema) {
        Set<String> projectedFieldNames = new HashSet<>();
        Collections.addAll(projectedFieldNames, fieldNames);

        List<Schema.Field> projectedFields = new ArrayList<>();
        for (Schema.Field f : sourceAvroSchema.getFields()) {
            if (projectedFieldNames.contains(f.name())) {
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
