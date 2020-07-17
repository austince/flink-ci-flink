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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.SerializerTestInstance;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinaryMapData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryArrayWriter;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.DeeplyEqualsChecker;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static org.apache.flink.table.data.StringData.fromString;

/**
 * Test for {@link RowDataSerializer}.
 */
@RunWith(Parameterized.class)
public class RowDataSerializerTest extends SerializerTestInstance<RowData> {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private final RowDataSerializer serializer;
	private final RowData[] testData;

	public RowDataSerializerTest(RowDataSerializer serializer, RowData[] testData) {
		super(
			new DeeplyEqualsChecker()
				.withCustomCheck(
					(o1, o2) -> o1 instanceof RowData && o2 instanceof RowData,
						(o1, o2, checker) -> deepEqualsRowData((RowData) o1, (RowData) o2,
								(RowDataSerializer) serializer.duplicate(),
								(RowDataSerializer) serializer.duplicate())
				),
			serializer,
			RowData.class,
			-1,
			testData);
		this.serializer = serializer;
		this.testData = testData;
	}

	@Parameterized.Parameters
	public static Collection<Object[]> parameters() {
		return Arrays.asList(
				testRowDataSerializer(),
				testLargeRowDataSerializer(),
				testRowDataSerializerWithComplexTypes(),
				testRowDataSerializerWithKryo());
	}

	private static Object[] testRowDataSerializer() {
		InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.ofFields(new IntType(), new VarCharType(VarCharType.MAX_LENGTH));
		GenericRowData row1 = new GenericRowData(2);
		row1.setField(0, 1);
		row1.setField(1, fromString("a"));

		GenericRowData row2 = new GenericRowData(2);
		row2.setField(0, 2);
		row2.setField(1, null);

		RowDataSerializer serializer = typeInfo.toRowSerializer();
		return new Object[] {serializer, new RowData[]{row1, row2}};
	}

	private static Object[] testLargeRowDataSerializer() {
		InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.ofFields(
			new IntType(),
			new IntType(),
			new IntType(),
			new IntType(),
			new IntType(),
			new IntType(),
			new IntType(),
			new IntType(),
			new IntType(),
			new IntType(),
			new IntType(),
			new IntType(),
			new VarCharType(VarCharType.MAX_LENGTH));

		GenericRowData row = new GenericRowData(13);
		row.setField(0, 2);
		row.setField(1, null);
		row.setField(3, null);
		row.setField(4, null);
		row.setField(5, null);
		row.setField(6, null);
		row.setField(7, null);
		row.setField(8, null);
		row.setField(9, null);
		row.setField(10, null);
		row.setField(11, null);
		row.setField(12, fromString("Test"));

		RowDataSerializer serializer = typeInfo.toRowSerializer();
		return new Object[] {serializer, new RowData[]{row}};
	}

	private static Object[] testRowDataSerializerWithComplexTypes() {
		InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.ofFields(
			new IntType(),
			new DoubleType(),
			new VarCharType(VarCharType.MAX_LENGTH),
			new ArrayType(new IntType()),
			new MapType(new IntType(), new IntType()));

		GenericRowData[] data = new GenericRowData[]{
			createRow(null, null, null, null, null),
			createRow(0, null, null, null, null),
			createRow(0, 0.0, null, null, null),
			createRow(0, 0.0, fromString("a"), null, null),
			createRow(1, 0.0, fromString("a"), null, null),
			createRow(1, 1.0, fromString("a"), null, null),
			createRow(1, 1.0, fromString("b"), null, null),
			createRow(1, 1.0, fromString("b"), createArray(1), createMap(new int[]{1}, new int[]{1})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2), createMap(new int[]{1, 4}, new int[]{1, 2})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2, 3), createMap(new int[]{1, 5}, new int[]{1, 3})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2, 3, 4), createMap(new int[]{1, 6}, new int[]{1, 4})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2, 3, 4, 5), createMap(new int[]{1, 7}, new int[]{1, 5})),
			createRow(1, 1.0, fromString("b"), createArray(1, 2, 3, 4, 5, 6), createMap(new int[]{1, 8}, new int[]{1, 6}))
		};

		RowDataSerializer serializer = typeInfo.toRowSerializer();
		return new Object[] {serializer, data};
	}

	private static Object[] testRowDataSerializerWithKryo() {
		RawValueDataSerializer<WrappedString> rawValueSerializer = new RawValueDataSerializer<>(
				new KryoSerializer<>(WrappedString.class, new ExecutionConfig()));
		RowDataSerializer serializer = new RowDataSerializer(new LogicalType[]{
				new RawType(RawValueData.class, rawValueSerializer)},
				new TypeSerializer[]{rawValueSerializer});

		GenericRowData row = new GenericRowData(1);
		row.setField(0, RawValueData.fromObject(new WrappedString("a")));

		return new Object[] {serializer, new GenericRowData[]{row}};
	}

	// ----------------------------------------------------------------------------------------------

	private static BinaryArrayData createArray(int... ints) {
		BinaryArrayData array = new BinaryArrayData();
		BinaryArrayWriter writer = new BinaryArrayWriter(array, ints.length, 4);
		for (int i = 0; i < ints.length; i++) {
			writer.writeInt(i, ints[i]);
		}
		writer.complete();
		return array;
	}

	private static BinaryMapData createMap(int[] keys, int[] values) {
		return BinaryMapData.valueOf(createArray(keys), createArray(values));
	}

	private static GenericRowData createRow(Object f0, Object f1, Object f2, Object f3, Object f4) {
		GenericRowData row = new GenericRowData(5);
		row.setField(0, f0);
		row.setField(1, f1);
		row.setField(2, f2);
		row.setField(3, f3);
		row.setField(4, f4);
		return row;
	}

	private static boolean deepEqualsRowData(
		RowData should, RowData is, RowDataSerializer serializer1, RowDataSerializer serializer2) {
		if (should.getArity() != is.getArity()) {
			return false;
		}
		BinaryRowData row1 = serializer1.toBinaryRow(should);
		BinaryRowData row2 = serializer2.toBinaryRow(is);

		return Objects.equals(row1, row2);
	}

	private void checkDeepEquals(RowData should, RowData is) {
		boolean equals = deepEqualsRowData(should, is,
				(RowDataSerializer) serializer.duplicate(), (RowDataSerializer) serializer.duplicate());
		Assert.assertTrue(equals);
	}

	@Test
	public void testCopy() {
		for (RowData row : testData) {
			checkDeepEquals(row, serializer.copy(row));
		}

		for (RowData row : testData) {
			checkDeepEquals(row, serializer.copy(row, new GenericRowData(row.getArity())));
		}

		for (RowData row : testData) {
			checkDeepEquals(row, serializer.copy(serializer.toBinaryRow(row),
					new GenericRowData(row.getArity())));
		}

		for (RowData row : testData) {
			checkDeepEquals(row, serializer.copy(serializer.toBinaryRow(row)));
		}

		for (RowData row : testData) {
			checkDeepEquals(row, serializer.copy(serializer.toBinaryRow(row),
					new BinaryRowData(row.getArity())));
		}
	}

	@Test
	public void testWrongCopy() {
		thrown.expect(IllegalArgumentException.class);
		serializer.copy(new GenericRowData(serializer.getArity() + 1));
	}

	@Test
	public void testWrongCopyReuse() {
		thrown.expect(IllegalArgumentException.class);
		for (RowData row : testData) {
			checkDeepEquals(row, serializer.copy(row, new GenericRowData(row.getArity() + 1)));
		}
	}

	@Test
	public void testToBinaryRowWithCompactDecimal() {
		testToBinaryRowWithDecimal(4);
	}

	@Test
	public void testToBinaryRowWithNotCompactDecimal() {
		testToBinaryRowWithDecimal(38);
	}

	private void testToBinaryRowWithDecimal(int precision) {
		DecimalData decimal = DecimalData.fromBigDecimal(new BigDecimal(123), precision, 0);

		BinaryRowData expected = new BinaryRowData(2);
		BinaryRowWriter writer = new BinaryRowWriter(expected);
		writer.writeDecimal(0, decimal, precision);
		writer.writeNullDecimal(1, precision);
		writer.complete();

		RowDataSerializer serializer = new RowDataSerializer(
			new ExecutionConfig(),
			RowType.of(new DecimalType(precision, 0), new DecimalType(precision, 0)));
		GenericRowData genericRow = new GenericRowData(2);
		genericRow.setField(0, decimal);
		genericRow.setField(1, null);
		BinaryRowData actual = serializer.toBinaryRow(genericRow);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testToBinaryRowWithCompactTimestamp() {
		testToBinaryRowWithTimestamp(3);
	}

	@Test
	public void testToBinaryRowWithNotCompactTimestamp() {
		testToBinaryRowWithTimestamp(9);
	}

	private void testToBinaryRowWithTimestamp(int precision) {
		TimestampData timestamp = TimestampData.fromTimestamp(new Timestamp(123));

		BinaryRowData expected = new BinaryRowData(2);
		BinaryRowWriter writer = new BinaryRowWriter(expected);
		writer.writeTimestamp(0, timestamp, precision);
		writer.writeNullTimestamp(1, precision);
		writer.complete();

		RowDataSerializer serializer = new RowDataSerializer(
			new ExecutionConfig(),
			RowType.of(new TimestampType(precision), new TimestampType(precision)));
		GenericRowData genericRow = new GenericRowData(2);
		genericRow.setField(0, timestamp);
		genericRow.setField(1, null);
		BinaryRowData actual = serializer.toBinaryRow(genericRow);

		Assert.assertEquals(expected, actual);
	}

	/**
	 * Class used for concurrent testing with KryoSerializer.
	 */
	private static class WrappedString {

		private final String content;

		WrappedString(String content) {
			this.content = content;
		}
	}

}
