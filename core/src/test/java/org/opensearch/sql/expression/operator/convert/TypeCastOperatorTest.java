/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.convert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.IP;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.FunctionExpression;

class TypeCastOperatorTest {

  private static Stream<ExprValue> numberData() {
    return Stream.of(
        new ExprByteValue(3),
        new ExprShortValue(3),
        new ExprIntegerValue(3),
        new ExprLongValue(3L),
        new ExprFloatValue(3.14f),
        new ExprDoubleValue(3.1415D));
  }

  private static Stream<ExprValue> stringData() {
    return Stream.of(new ExprStringValue("strV"));
  }

  private static Stream<ExprValue> boolData() {
    return Stream.of(ExprBooleanValue.of(true));
  }

  private static Stream<ExprValue> date() {
    return Stream.of(new ExprDateValue("2020-12-24"));
  }

  private static Stream<ExprValue> time() {
    return Stream.of(new ExprTimeValue("01:01:01"));
  }

  private static Stream<ExprValue> timestamp() {
    return Stream.of(new ExprTimestampValue("2020-12-24 01:01:01"));
  }

  @ParameterizedTest(name = "castString({0})")
  @MethodSource({"numberData", "stringData", "boolData", "date", "time", "timestamp"})
  void castToString(ExprValue value) {
    FunctionExpression expression = DSL.castString(DSL.literal(value));
    assertEquals(STRING, expression.type());
    assertEquals(new ExprStringValue(value.value().toString()), expression.valueOf());
  }

  @ParameterizedTest(name = "castToByte({0})")
  @MethodSource({"numberData"})
  void castToByte(ExprValue value) {
    FunctionExpression expression = DSL.castByte(DSL.literal(value));
    assertEquals(BYTE, expression.type());
    assertEquals(new ExprByteValue(value.byteValue()), expression.valueOf());
  }

  @ParameterizedTest(name = "castToShort({0})")
  @MethodSource({"numberData"})
  void castToShort(ExprValue value) {
    FunctionExpression expression = DSL.castShort(DSL.literal(value));
    assertEquals(SHORT, expression.type());
    assertEquals(new ExprShortValue(value.shortValue()), expression.valueOf());
  }

  @ParameterizedTest(name = "castToInt({0})")
  @MethodSource({"numberData"})
  void castToInt(ExprValue value) {
    FunctionExpression expression = DSL.castInt(DSL.literal(value));
    assertEquals(INTEGER, expression.type());
    assertEquals(new ExprIntegerValue(value.integerValue()), expression.valueOf());
  }

  @Test
  void castStringToByte() {
    FunctionExpression expression = DSL.castByte(DSL.literal("100"));
    assertEquals(BYTE, expression.type());
    assertEquals(new ExprByteValue(100), expression.valueOf());
  }

  @Test
  void castStringToShort() {
    FunctionExpression expression = DSL.castShort(DSL.literal("100"));
    assertEquals(SHORT, expression.type());
    assertEquals(new ExprShortValue(100), expression.valueOf());
  }

  @Test
  void castStringToInt() {
    FunctionExpression expression = DSL.castInt(DSL.literal("100"));
    assertEquals(INTEGER, expression.type());
    assertEquals(new ExprIntegerValue(100), expression.valueOf());
  }

  @Test
  void castStringToIntException() {
    FunctionExpression expression = DSL.castInt(DSL.literal("invalid"));
    assertThrows(RuntimeException.class, () -> expression.valueOf());
  }

  @Test
  void castBooleanToByte() {
    FunctionExpression expression = DSL.castByte(DSL.literal(true));
    assertEquals(BYTE, expression.type());
    assertEquals(new ExprByteValue(1), expression.valueOf());

    expression = DSL.castByte(DSL.literal(false));
    assertEquals(BYTE, expression.type());
    assertEquals(new ExprByteValue(0), expression.valueOf());
  }

  @Test
  void castBooleanToShort() {
    FunctionExpression expression = DSL.castShort(DSL.literal(true));
    assertEquals(SHORT, expression.type());
    assertEquals(new ExprShortValue(1), expression.valueOf());

    expression = DSL.castShort(DSL.literal(false));
    assertEquals(SHORT, expression.type());
    assertEquals(new ExprShortValue(0), expression.valueOf());
  }

  @Test
  void castUndefinedToShort() {
    Short value = 42;
    // json cast is an UNDEFINED type expression
    FunctionExpression expression = DSL.castShort(DSL.castJson(DSL.literal(value.toString())));
    assertEquals(SHORT, expression.type());
    assertEquals(new ExprShortValue(value), expression.valueOf());
  }

  @Test
  void castBooleanToInt() {
    FunctionExpression expression = DSL.castInt(DSL.literal(true));
    assertEquals(INTEGER, expression.type());
    assertEquals(new ExprIntegerValue(1), expression.valueOf());

    expression = DSL.castInt(DSL.literal(false));
    assertEquals(INTEGER, expression.type());
    assertEquals(new ExprIntegerValue(0), expression.valueOf());
  }

  @Test
  void castUndefinedToInt() {
    Integer value = 42;
    // json cast is an UNDEFINED type expression
    FunctionExpression expression = DSL.castInt(DSL.castJson(DSL.literal(value.toString())));
    assertEquals(INTEGER, expression.type());
    assertEquals(new ExprIntegerValue(value), expression.valueOf());
  }

  @ParameterizedTest(name = "castToLong({0})")
  @MethodSource({"numberData"})
  void castToLong(ExprValue value) {
    FunctionExpression expression = DSL.castLong(DSL.literal(value));
    assertEquals(LONG, expression.type());
    assertEquals(new ExprLongValue(value.longValue()), expression.valueOf());
  }

  @Test
  void castStringToLong() {
    FunctionExpression expression = DSL.castLong(DSL.literal("100"));
    assertEquals(LONG, expression.type());
    assertEquals(new ExprLongValue(100), expression.valueOf());
  }

  @Test
  void castStringToLongException() {
    FunctionExpression expression = DSL.castLong(DSL.literal("invalid"));
    assertThrows(RuntimeException.class, () -> expression.valueOf());
  }

  @Test
  void castBooleanToLong() {
    FunctionExpression expression = DSL.castLong(DSL.literal(true));
    assertEquals(LONG, expression.type());
    assertEquals(new ExprLongValue(1), expression.valueOf());

    expression = DSL.castLong(DSL.literal(false));
    assertEquals(LONG, expression.type());
    assertEquals(new ExprLongValue(0), expression.valueOf());
  }

  @Test
  void castUndefinedToLong() {
    Long value = 42l;
    // json cast is an UNDEFINED type expression
    FunctionExpression expression = DSL.castLong(DSL.castJson(DSL.literal(value.toString())));
    assertEquals(LONG, expression.type());
    assertEquals(new ExprLongValue(value), expression.valueOf());
  }

  @ParameterizedTest(name = "castToFloat({0})")
  @MethodSource({"numberData"})
  void castToFloat(ExprValue value) {
    FunctionExpression expression = DSL.castFloat(DSL.literal(value));
    assertEquals(FLOAT, expression.type());
    assertEquals(new ExprFloatValue(value.floatValue()), expression.valueOf());
  }

  @Test
  void castStringToFloat() {
    FunctionExpression expression = DSL.castFloat(DSL.literal("100.0"));
    assertEquals(FLOAT, expression.type());
    assertEquals(new ExprFloatValue(100.0), expression.valueOf());
  }

  @Test
  void castStringToFloatException() {
    FunctionExpression expression = DSL.castFloat(DSL.literal("invalid"));
    assertThrows(RuntimeException.class, () -> expression.valueOf());
  }

  @Test
  void castBooleanToFloat() {
    FunctionExpression expression = DSL.castFloat(DSL.literal(true));
    assertEquals(FLOAT, expression.type());
    assertEquals(new ExprFloatValue(1), expression.valueOf());

    expression = DSL.castFloat(DSL.literal(false));
    assertEquals(FLOAT, expression.type());
    assertEquals(new ExprFloatValue(0), expression.valueOf());
  }

  @Test
  void castUndefinedToFloat() {
    Float value = 23.45f;
    // json cast is an UNDEFINED type expression
    FunctionExpression expression = DSL.castFloat(DSL.castJson(DSL.literal(value.toString())));
    assertEquals(FLOAT, expression.type());
    assertEquals(new ExprFloatValue(value), expression.valueOf());
  }

  @ParameterizedTest(name = "castToDouble({0})")
  @MethodSource({"numberData"})
  void castToDouble(ExprValue value) {
    FunctionExpression expression = DSL.castDouble(DSL.literal(value));
    assertEquals(DOUBLE, expression.type());
    assertEquals(new ExprDoubleValue(value.doubleValue()), expression.valueOf());
  }

  @Test
  void castStringToDouble() {
    FunctionExpression expression = DSL.castDouble(DSL.literal("100.0"));
    assertEquals(DOUBLE, expression.type());
    assertEquals(new ExprDoubleValue(100), expression.valueOf());
  }

  @Test
  void castStringToDoubleException() {
    FunctionExpression expression = DSL.castDouble(DSL.literal("invalid"));
    assertThrows(RuntimeException.class, () -> expression.valueOf());
  }

  @Test
  void castBooleanToDouble() {
    FunctionExpression expression = DSL.castDouble(DSL.literal(true));
    assertEquals(DOUBLE, expression.type());
    assertEquals(new ExprDoubleValue(1), expression.valueOf());

    expression = DSL.castDouble(DSL.literal(false));
    assertEquals(DOUBLE, expression.type());
    assertEquals(new ExprDoubleValue(0), expression.valueOf());
  }

  @Test
  void castUndefinedToDouble() {
    Double value = 23.45e5;
    // json cast is an UNDEFINED type expression
    FunctionExpression expression = DSL.castDouble(DSL.castJson(DSL.literal(value)));
    assertEquals(DOUBLE, expression.type());
    assertEquals(new ExprDoubleValue(value), expression.valueOf());
  }

  @ParameterizedTest(name = "castToBoolean({0})")
  @MethodSource({"numberData"})
  void castToBoolean(ExprValue value) {
    FunctionExpression expression = DSL.castBoolean(DSL.literal(value));
    assertEquals(BOOLEAN, expression.type());
    assertEquals(ExprBooleanValue.of(true), expression.valueOf());
  }

  @Test
  void castZeroToBoolean() {
    FunctionExpression expression = DSL.castBoolean(DSL.literal(0));
    assertEquals(BOOLEAN, expression.type());
    assertEquals(ExprBooleanValue.of(false), expression.valueOf());
  }

  @Test
  void castStringToBoolean() {
    FunctionExpression expression = DSL.castBoolean(DSL.literal("True"));
    assertEquals(BOOLEAN, expression.type());
    assertEquals(ExprBooleanValue.of(true), expression.valueOf());
  }

  @Test
  void castBooleanToBoolean() {
    FunctionExpression expression = DSL.castBoolean(DSL.literal(true));
    assertEquals(BOOLEAN, expression.type());
    assertEquals(ExprBooleanValue.of(true), expression.valueOf());
  }

  @Test
  void castUndefinedToBoolean() {
    // json cast is an UNDEFINED type expression
    FunctionExpression expression = DSL.castBoolean(DSL.castJson(DSL.literal("true")));
    assertEquals(BOOLEAN, expression.type());
    assertEquals(ExprBooleanValue.of(true), expression.valueOf());
  }

  @Test
  void castToDate() {
    FunctionExpression expression = DSL.castDate(DSL.literal("2012-08-07"));
    assertEquals(DATE, expression.type());
    assertEquals(new ExprDateValue("2012-08-07"), expression.valueOf());

    expression = DSL.castDate(DSL.literal(new ExprTimestampValue("2012-08-07 01:01:01")));
    assertEquals(DATE, expression.type());
    assertEquals(new ExprDateValue("2012-08-07"), expression.valueOf());

    expression = DSL.castDate(DSL.literal(new ExprTimestampValue("2012-08-07 01:01:01")));
    assertEquals(DATE, expression.type());
    assertEquals(new ExprDateValue("2012-08-07"), expression.valueOf());

    expression = DSL.castDate(DSL.literal(new ExprDateValue("2012-08-07")));
    assertEquals(DATE, expression.type());
    assertEquals(new ExprDateValue("2012-08-07"), expression.valueOf());
  }

  @Test
  void castToTime() {
    FunctionExpression expression = DSL.castTime(DSL.literal("01:01:01"));
    assertEquals(TIME, expression.type());
    assertEquals(new ExprTimeValue("01:01:01"), expression.valueOf());

    expression = DSL.castTime(DSL.literal(new ExprTimestampValue("2012-08-07 01:01:01")));
    assertEquals(TIME, expression.type());
    assertEquals(new ExprTimeValue("01:01:01"), expression.valueOf());

    expression = DSL.castTime(DSL.literal(new ExprTimeValue("01:01:01")));
    assertEquals(TIME, expression.type());
    assertEquals(new ExprTimeValue("01:01:01"), expression.valueOf());
  }

  @Test
  void castToTimestamp() {
    FunctionExpression expression = DSL.castTimestamp(DSL.literal("2012-08-07 01:01:01"));
    assertEquals(TIMESTAMP, expression.type());
    assertEquals(new ExprTimestampValue("2012-08-07 01:01:01"), expression.valueOf());

    expression = DSL.castTimestamp(DSL.literal(new ExprTimestampValue("2012-08-07 01:01:01")));
    assertEquals(TIMESTAMP, expression.type());
    assertEquals(new ExprTimestampValue("2012-08-07 01:01:01"), expression.valueOf());
  }

  @Test
  void castToIp() {
    FunctionExpression exp;

    final String ipv4String = "1.2.3.4";
    final String ipv6String = "2001:db7::ff00:42:8329";
    final String ipInvalidString = "INVALID";

    final ExprValue exprIpv4Value = new ExprIpValue(ipv4String);
    final ExprValue exprIpv6Value = new ExprIpValue(ipv6String);

    // From string
    exp = DSL.castIp(DSL.literal(ipv4String));
    assertEquals(IP, exp.type());
    assertEquals(exprIpv4Value, exp.valueOf());

    exp = DSL.castIp(DSL.literal(ipv6String));
    assertEquals(IP, exp.type());
    assertEquals(exprIpv6Value, exp.valueOf());

    exp = DSL.castIp(DSL.literal(ipInvalidString));
    assertThrows(
        SemanticCheckException.class,
        exp::valueOf,
        String.format("IP address string '%s' is not valid. Error details: .*", ipInvalidString));

    // From IP address
    exp = DSL.castIp(DSL.literal(exprIpv4Value));
    assertEquals(IP, exp.type());
    assertEquals(exprIpv4Value, exp.valueOf());

    exp = DSL.castIp(DSL.literal(exprIpv6Value));
    assertEquals(IP, exp.type());
    assertEquals(exprIpv6Value, exp.valueOf());

    // From invalid type
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.castIp(DSL.literal(0)),
        "cast_to_ip function expected {[IP],[STRING]}, but got [INTEGER]");

    // From null or missing value
    exp = DSL.castIp(DSL.literal(ExprNullValue.of()));
    assertEquals(IP, exp.type());
    assertTrue(exp.valueOf().isNull());

    exp = DSL.castIp(DSL.literal(ExprMissingValue.of()));
    assertEquals(IP, exp.type());
    assertTrue(exp.valueOf().isMissing());
  }

  @Test
  void castJson_returnsJsonObject() {
    FunctionExpression exp;

    // Setup
    String objectJson =
        "{\"foo\": \"foo\", \"fuzz\": true, \"bar\": 1234, \"bar2\": 12.34, \"baz\": null, "
            + "\"obj\": {\"internal\": \"value\"}, \"arr\": [\"string\", true, null]}";

    LinkedHashMap<String, ExprValue> objectMap = new LinkedHashMap<>();
    objectMap.put("foo", new ExprStringValue("foo"));
    objectMap.put("fuzz", ExprBooleanValue.of(true));
    objectMap.put("bar", new ExprLongValue(1234));
    objectMap.put("bar2", new ExprDoubleValue(12.34));
    objectMap.put("baz", ExprNullValue.of());
    objectMap.put(
        "obj", ExprTupleValue.fromExprValueMap(Map.of("internal", new ExprStringValue("value"))));
    objectMap.put(
        "arr",
        new ExprCollectionValue(
            List.of(new ExprStringValue("string"), ExprBooleanValue.of(true), ExprNullValue.of())));
    ExprValue expectedTupleExpr = ExprTupleValue.fromExprValueMap(objectMap);

    // exercise
    exp = DSL.castJson(DSL.literal(objectJson));

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprTupleValue);
    assertEquals(expectedTupleExpr, value);
  }

  @Test
  void castJson_returnsJsonArray() {
    FunctionExpression exp;

    // Setup
    String arrayJson = "[\"foo\", \"fuzz\", true, \"bar\", 1234, 12.34, null]";
    ExprValue expectedArrayExpr =
        new ExprCollectionValue(
            List.of(
                new ExprStringValue("foo"),
                new ExprStringValue("fuzz"),
                LITERAL_TRUE,
                new ExprStringValue("bar"),
                new ExprIntegerValue(1234),
                new ExprDoubleValue(12.34),
                LITERAL_NULL));

    // exercise
    exp = DSL.castJson(DSL.literal(arrayJson));

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprCollectionValue);
    assertEquals(expectedArrayExpr, value);
  }

  @Test
  void castJson_returnsScalar() {
    String scalarStringJson = "\"foobar\"";
    assertEquals(
        new ExprStringValue("foobar"), DSL.castJson(DSL.literal(scalarStringJson)).valueOf());

    String scalarNumberJson = "1234";
    assertEquals(new ExprIntegerValue(1234), DSL.castJson(DSL.literal(scalarNumberJson)).valueOf());

    String scalarBooleanJson = "true";
    assertEquals(LITERAL_TRUE, DSL.castJson(DSL.literal(scalarBooleanJson)).valueOf());

    String scalarNullJson = "null";
    assertEquals(LITERAL_NULL, DSL.castJson(DSL.literal(scalarNullJson)).valueOf());

    String empty = "";
    assertEquals(LITERAL_NULL, DSL.castJson(DSL.literal(empty)).valueOf());

    String emptyObject = "{}";
    assertEquals(
        ExprTupleValue.fromExprValueMap(Map.of()),
        DSL.castJson(DSL.literal(emptyObject)).valueOf());
  }

  @Test
  void castJson_returnsSemanticCheckException() {
    // invalid type
    assertThrows(
        SemanticCheckException.class, () -> DSL.castJson(DSL.literal("invalid")).valueOf());

    // missing bracket
    assertThrows(SemanticCheckException.class, () -> DSL.castJson(DSL.literal("{{[}}")).valueOf());

    // mnissing quote
    assertThrows(
        SemanticCheckException.class, () -> DSL.castJson(DSL.literal("\"missing quote")).valueOf());
  }
}
