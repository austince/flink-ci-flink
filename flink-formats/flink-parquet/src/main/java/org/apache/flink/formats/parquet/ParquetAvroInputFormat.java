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

/**
 * An implementation of {@link ParquetInputFormat} to read records from Parquet files and convert
 * them to Avro GenericRecord.
 */
public class ParquetAvroInputFormat extends ParquetInputFormat<GenericRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetAvroInputFormat.class);

    private MessageType parquetSchema;

    public ParquetAvroInputFormat(Path filePath, MessageType messageType) {
        super(filePath, messageType);
        parquetSchema = messageType;
    }

    @Override
    protected GenericRecord convert(Row row) {
        final Schema avroSchema = new AvroSchemaConverter().convert(parquetSchema);
        AvroRowSerializationSchema avroRowSerializationSchema =
                new AvroRowSerializationSchema(avroSchema.toString());
        return avroRowSerializationSchema.convertRowToAvroRecord(avroSchema, row);
    }
}
