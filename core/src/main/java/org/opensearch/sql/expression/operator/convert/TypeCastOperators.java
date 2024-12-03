/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.convert;

import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.function.FunctionDSL.impl;
import static org.opensearch.sql.expression.function.FunctionDSL.implWithProperties;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandling;
import static org.opensearch.sql.expression.function.FunctionDSL.nullMissingHandlingWithProperties;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.DefaultFunctionResolver;
import org.opensearch.sql.expression.function.FunctionDSL;

@UtilityClass
public class TypeCastOperators {

  /** Register Type Cast Operator. */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(castToString());
    repository.register(castToByte());
    repository.register(castToShort());
    repository.register(castToInt());
    repository.register(castToLong());
    repository.register(castToFloat());
    repository.register(castToDouble());
    repository.register(castToBoolean());
    repository.register(castToDate());
    repository.register(castToTime());
    repository.register(castToTimestamp());
    repository.register(castToDatetime());
  }

  private static DefaultFunctionResolver castToString() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_STRING.getName(),
        Stream.concat(
                Arrays.asList(
                        BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN, TIME, DATE, TIMESTAMP,
                        DATETIME)
                    .stream()
                    .map(
                        type ->
                            impl(
                                nullMissingHandling(
                                    (v) -> new ExprStringValue(v.value().toString())),
                                STRING,
                                type)),
                Stream.of(impl(nullMissingHandling((v) -> v), STRING, STRING)))
            .collect(Collectors.toList()));
  }

  private static DefaultFunctionResolver castToByte() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_BYTE.getName(),
        impl(
            nullMissingHandling((v) -> new ExprByteValue(Byte.valueOf(v.stringValue()))),
            BYTE,
            STRING),
        impl(nullMissingHandling((v) -> new ExprByteValue(v.byteValue())), BYTE, DOUBLE),
        impl(
            nullMissingHandling((v) -> new ExprByteValue(v.booleanValue() ? 1 : 0)),
            BYTE,
            BOOLEAN));
  }

  private static DefaultFunctionResolver castToShort() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_SHORT.getName(),
        impl(
            nullMissingHandling((v) -> new ExprShortValue(Short.valueOf(v.stringValue()))),
            SHORT,
            STRING),
        impl(nullMissingHandling((v) -> new ExprShortValue(v.shortValue())), SHORT, DOUBLE),
        impl(
            nullMissingHandling((v) -> new ExprShortValue(v.booleanValue() ? 1 : 0)),
            SHORT,
            BOOLEAN));
  }

  private static DefaultFunctionResolver castToInt() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_INT.getName(),
        impl(
            nullMissingHandling((v) -> new ExprIntegerValue(Integer.valueOf(v.stringValue()))),
            INTEGER,
            STRING),
        impl(nullMissingHandling((v) -> new ExprIntegerValue(v.integerValue())), INTEGER, DOUBLE),
        impl(
            nullMissingHandling((v) -> new ExprIntegerValue(v.booleanValue() ? 1 : 0)),
            INTEGER,
            BOOLEAN));
  }

  private static DefaultFunctionResolver castToLong() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_LONG.getName(),
        impl(
            nullMissingHandling((v) -> new ExprLongValue(Long.valueOf(v.stringValue()))),
            LONG,
            STRING),
        impl(nullMissingHandling((v) -> new ExprLongValue(v.longValue())), LONG, DOUBLE),
        impl(
            nullMissingHandling((v) -> new ExprLongValue(v.booleanValue() ? 1L : 0L)),
            LONG,
            BOOLEAN));
  }

  private static DefaultFunctionResolver castToFloat() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_FLOAT.getName(),
        impl(
            nullMissingHandling((v) -> new ExprFloatValue(Float.valueOf(v.stringValue()))),
            FLOAT,
            STRING),
        impl(nullMissingHandling((v) -> new ExprFloatValue(v.floatValue())), FLOAT, DOUBLE),
        impl(
            nullMissingHandling((v) -> new ExprFloatValue(v.booleanValue() ? 1f : 0f)),
            FLOAT,
            BOOLEAN));
  }

  private static DefaultFunctionResolver castToDouble() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_DOUBLE.getName(),
        impl(
            nullMissingHandling((v) -> new ExprDoubleValue(Double.valueOf(v.stringValue()))),
            DOUBLE,
            STRING),
        impl(nullMissingHandling((v) -> new ExprDoubleValue(v.doubleValue())), DOUBLE, DOUBLE),
        impl(
            nullMissingHandling((v) -> new ExprDoubleValue(v.booleanValue() ? 1D : 0D)),
            DOUBLE,
            BOOLEAN));
  }

  private static DefaultFunctionResolver castToBoolean() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_BOOLEAN.getName(),
        impl(
            nullMissingHandling((v) -> ExprBooleanValue.of(Boolean.valueOf(v.stringValue()))),
            BOOLEAN,
            STRING),
        impl(
            nullMissingHandling((v) -> ExprBooleanValue.of(v.doubleValue() != 0)), BOOLEAN, DOUBLE),
        impl(nullMissingHandling((v) -> v), BOOLEAN, BOOLEAN));
  }

  private static DefaultFunctionResolver castToDate() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_DATE.getName(),
        impl(nullMissingHandling((v) -> new ExprDateValue(v.stringValue())), DATE, STRING),
        impl(nullMissingHandling((v) -> new ExprDateValue(v.dateValue())), DATE, DATETIME),
        impl(nullMissingHandling((v) -> new ExprDateValue(v.dateValue())), DATE, TIMESTAMP),
        impl(nullMissingHandling((v) -> v), DATE, DATE));
  }

  private static DefaultFunctionResolver castToTime() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_TIME.getName(),
        impl(nullMissingHandling((v) -> new ExprTimeValue(v.stringValue())), TIME, STRING),
        impl(nullMissingHandling((v) -> new ExprTimeValue(v.timeValue())), TIME, DATETIME),
        impl(nullMissingHandling((v) -> new ExprTimeValue(v.timeValue())), TIME, TIMESTAMP),
        impl(nullMissingHandling((v) -> v), TIME, TIME));
  }

  // `DATE`/`TIME`/`DATETIME` -> `DATETIME`/TIMESTAMP` cast tested in BinaryPredicateOperatorTest
  private static DefaultFunctionResolver castToTimestamp() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_TIMESTAMP.getName(),
        impl(
            nullMissingHandling((v) -> new ExprTimestampValue(v.stringValue())), TIMESTAMP, STRING),
        impl(
            nullMissingHandling((v) -> new ExprTimestampValue(v.timestampValue())),
            TIMESTAMP,
            DATETIME),
        impl(
            nullMissingHandling((v) -> new ExprTimestampValue(v.timestampValue())),
            TIMESTAMP,
            DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (fp, v) -> new ExprTimestampValue(((ExprTimeValue) v).timestampValue(fp))),
            TIMESTAMP,
            TIME),
        impl(nullMissingHandling((v) -> v), TIMESTAMP, TIMESTAMP));
  }

  private static DefaultFunctionResolver castToDatetime() {
    return FunctionDSL.define(
        BuiltinFunctionName.CAST_TO_DATETIME.getName(),
        impl(nullMissingHandling((v) -> new ExprDatetimeValue(v.stringValue())), DATETIME, STRING),
        impl(
            nullMissingHandling((v) -> new ExprDatetimeValue(v.datetimeValue())),
            DATETIME,
            TIMESTAMP),
        impl(nullMissingHandling((v) -> new ExprDatetimeValue(v.datetimeValue())), DATETIME, DATE),
        implWithProperties(
            nullMissingHandlingWithProperties(
                (fp, v) -> new ExprDatetimeValue(((ExprTimeValue) v).datetimeValue(fp))),
            DATETIME,
            TIME),
        impl(nullMissingHandling((v) -> v), DATETIME, DATETIME));
  }
}
