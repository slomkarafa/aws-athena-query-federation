/*-
 * #%L
 * athena-deltalake
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.connectors.athena.deltalake.converter;

import com.amazonaws.athena.connector.lambda.data.ArrowTypeComparator;
import com.amazonaws.athena.connector.lambda.data.FieldResolver;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.*;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.arrow.vector.types.Types.MinorType.MAP;

public class ParquetFieldResolver implements FieldResolver {

    private static final Logger logger = LoggerFactory.getLogger(ParquetFieldResolver.class);

    private final int rowNum;

    public ParquetFieldResolver(int rowNum) {
        this.rowNum = rowNum;
    }

    @Override
    public Object getFieldValue(Field field, Object value) {
        Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        String fieldName = field.getName();
        Group record = (Group) value;
        try {
            if (minorType == MAP && record.getType().containsField("key_value") && record.getFieldRepetitionCount("key_value") > 0) {
                var keyValField = field.getChildren().get(0).getChildren();
                var keyField = keyValField.get(0);
                var valField = keyValField.get(1);
                return IntStream
                    .range(0, record.getFieldRepetitionCount("key_value"))
                    .mapToObj(idx -> record.getGroup("key_value", idx))
                    .collect(Collectors.toMap(x -> getFieldValue(keyField, x), x -> getFieldValue(valField, x)));
            } else if (record.getFieldRepetitionCount(fieldName) > 0) {
                switch (minorType) {
//                    case MAP:
//                        var innerRecord = record.getGroup(field)
//                        var keyValField = field.getChildren().get(0).getChildren();
//                        var keyField = keyValField.get(0);
//                        var valField = keyValField.get(1);
//                        return IntStream
//                            .range(0, record.getFieldRepetitionCount("key_value"))
//                            .mapToObj(idx -> record.getGroup("key_value", idx))
//                            .collect(Collectors.toMap(x -> getFieldValue(keyField, x), x -> getFieldValue(valField, x)));
                    case STRUCT:
                    case MAP:
                        return record.getGroup(fieldName, rowNum);
                    case LIST:
                        Group list = record.getGroup(fieldName, rowNum);
                        var a = field.getChildren();
                            var b = a.get(0);
                        var x = IntStream
                            .range(0, list.getFieldRepetitionCount(0))
//                            .mapToObj(idx -> list.getGroup(0, idx))
                            .mapToObj(idx -> {
                                var f = field.getChildren().get(0);
                                var arrayField = new Field("element", f.getFieldType(), List.of());
                                return getFieldValue(arrayField, list.getGroup(0, idx));
                            })
                            .collect(Collectors.toList());

                        return x;
                    case TINYINT:
                        return (byte) record.getInteger(fieldName, rowNum);
                    case SMALLINT:
                        return (short) record.getInteger(fieldName, rowNum);
                    case INT:
                        return record.getInteger(fieldName, rowNum);
                    case BIGINT:
                        return record.getLong(fieldName, rowNum);
                    case BIT:
                        return record.getBoolean(fieldName, rowNum);
                    case FLOAT4:
                        return record.getFloat(fieldName, rowNum);
                    case FLOAT8:
                        return record.getDouble(fieldName, rowNum);
                    case VARCHAR:
                        return record.getString(fieldName, rowNum);
                    case DATEDAY:
                        return record.getInteger(fieldName, rowNum);
                    case DATEMILLI:
                        PrimitiveType.PrimitiveTypeName primitiveTypeName =
                            record.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName();
                        if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                            return record.getLong(fieldName, rowNum);
                        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT96) {
                            int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
                            long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
                            long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
                            byte[] bytes = record.getInt96(fieldName, rowNum).getBytes();
                            long timeOfDayNanos =
                                Longs.fromBytes(
                                    bytes[7],
                                    bytes[6],
                                    bytes[5],
                                    bytes[4],
                                    bytes[3],
                                    bytes[2],
                                    bytes[1],
                                    bytes[0]
                                );
                            int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);
                            return ((julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
                        } else {
                            throw new UnsupportedOperationException("Timestamp type is not handled with parquet type: " + primitiveTypeName
                                .name());
                        }
                    case DECIMAL:
                    case DECIMAL256:
                        ArrowType.Decimal fieldDecimalType = ((ArrowType.Decimal) field.getType());
                        PrimitiveType.PrimitiveTypeName primitiveTypeName2 =
                            record.getType().getType(fieldName).asPrimitiveType().getPrimitiveTypeName();
                        if (primitiveTypeName2 == PrimitiveType.PrimitiveTypeName.INT64) {
                            return BigDecimal.valueOf(record.getLong(fieldName, rowNum), fieldDecimalType.getScale());
                        } else if (primitiveTypeName2 == PrimitiveType.PrimitiveTypeName.INT32) {
                            return BigDecimal.valueOf(
                                record.getInteger(fieldName, rowNum),
                                fieldDecimalType.getScale()
                            );
                        } else if (primitiveTypeName2 == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                            return new BigDecimal(
                                new BigInteger(record.getBinary(fieldName, rowNum).getBytes()),
                                fieldDecimalType.getScale()
                            );
                        } else {
                            throw new UnsupportedOperationException(
                                "Parquet physical type used for Decimal not supported: " + primitiveTypeName2.name());
                        }
                    case VARBINARY:
                        return record.getBinary(fieldName, rowNum).getBytes();
                    default:
                        throw new IllegalArgumentException("Unsupported type " + minorType);
                }
            }
        } catch (InvalidRecordException ignored) {
            logger.error(ignored.toString());
            logger.error(Arrays.toString(ignored.getStackTrace()));
        }
        return null;
    }
}
