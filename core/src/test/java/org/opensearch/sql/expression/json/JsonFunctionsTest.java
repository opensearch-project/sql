/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_FALSE;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_TRUE;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;

@ExtendWith(MockitoExtension.class)
public class JsonFunctionsTest {
  @Test
  public void json_valid_returns_false() {
    List<LiteralExpression> expressions =
        List.of(
            DSL.literal(LITERAL_MISSING), // missing returns false
            DSL.literal(LITERAL_NULL), // null returns false
            DSL.literal("invalid"), // invalid type
            DSL.literal("{{[}}"), // missing bracket
            DSL.literal("[}"), // missing bracket
            DSL.literal("}"), // missing bracket
            DSL.literal("\"missing quote"), // missing quote
            DSL.literal("abc"), // not a type
            DSL.literal("97ab"), // not a type
            DSL.literal("{1, 2, 3, 4}"), // invalid object
            DSL.literal("{\"invalid\":\"json\", \"string\"}"), // invalid object
            DSL.literal("{123: 1, true: 2, null: 3}"), // invalid object
            DSL.literal("[\"a\": 1, \"b\": 2]") // invalid array
            );

    expressions.stream()
        .forEach(
            expr ->
                assertEquals(
                    LITERAL_FALSE,
                    DSL.jsonValid(expr).valueOf(),
                    "Expected FALSE when calling jsonValid with " + expr));
  }

  @Test
  public void json_valid_throws_ExpressionEvaluationException() {
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.jsonValid(DSL.literal((ExprValueUtils.booleanValue(true)))).valueOf());
  }

  @Test
  public void json_valid_returns_true() {

    List<String> validJsonStrings =
        List.of(
            // test json objects are valid
            "{\"a\":\"1\",\"b\":\"2\"}",
            "{\"a\":1,\"b\":{\"c\":2,\"d\":3}}",
            "{\"arr1\": [1,2,3], \"arr2\": [4,5,6]}",

            // test json arrays are valid
            "[1, 2, 3, 4]",
            "[{\"a\":1,\"b\":2}, {\"c\":3,\"d\":2}]",

            // test json scalars are valid
            "\"abc\"",
            "1234",
            "12.34",
            "true",
            "false",
            "null",

            // test empty string is valid
            "");

    validJsonStrings.stream()
        .forEach(
            str ->
                assertEquals(
                    LITERAL_TRUE,
                    DSL.jsonValid(DSL.literal(str)).valueOf(),
                    String.format("String %s must be valid json", str)));
  }

  @Test
  void json_returnsJsonObject() {
    FunctionExpression exp;

    // Setup
    final String objectJson =
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
    exp = DSL.stringToJson(DSL.literal(objectJson));

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprTupleValue);
    assertEquals(expectedTupleExpr, value);

    // also test the empty object case
    assertEquals(
        ExprTupleValue.fromExprValueMap(Map.of()), DSL.stringToJson(DSL.literal("{}")).valueOf());
  }

  @Test
  void json_returnsJsonArray() {
    FunctionExpression exp;

    // Setup
    final String arrayJson = "[\"foo\", \"fuzz\", true, \"bar\", 1234, 12.34, null]";
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
    exp = DSL.stringToJson(DSL.literal(arrayJson));

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprCollectionValue);
    assertEquals(expectedArrayExpr, value);

    // also test the empty-array case
    assertEquals(new ExprCollectionValue(List.of()), DSL.stringToJson(DSL.literal("[]")).valueOf());
  }

  @Test
  void json_returnsScalar() {
    assertEquals(
        new ExprStringValue("foobar"), DSL.stringToJson(DSL.literal("\"foobar\"")).valueOf());

    assertEquals(new ExprIntegerValue(1234), DSL.stringToJson(DSL.literal("1234")).valueOf());

    assertEquals(new ExprDoubleValue(12.34), DSL.stringToJson(DSL.literal("12.34")).valueOf());

    assertEquals(LITERAL_TRUE, DSL.stringToJson(DSL.literal("true")).valueOf());
    assertEquals(LITERAL_FALSE, DSL.stringToJson(DSL.literal("false")).valueOf());

    assertEquals(LITERAL_NULL, DSL.stringToJson(DSL.literal("null")).valueOf());

    assertEquals(LITERAL_NULL, DSL.stringToJson(DSL.literal(LITERAL_NULL)).valueOf());

    assertEquals(LITERAL_MISSING, DSL.stringToJson(DSL.literal(LITERAL_MISSING)).valueOf());

    assertEquals(LITERAL_NULL, DSL.stringToJson(DSL.literal("")).valueOf());
  }

  @Test
  void json_returnsSemanticCheckException() {
    List<LiteralExpression> expressions =
        List.of(
            DSL.literal("invalid"), // invalid type
            DSL.literal("{{[}}"), // missing bracket
            DSL.literal("[}"), // missing bracket
            DSL.literal("}"), // missing bracket
            DSL.literal("\"missing quote"), // missing quote
            DSL.literal("abc"), // not a type
            DSL.literal("97ab"), // not a type
            DSL.literal("{1, 2, 3, 4}"), // invalid object
            DSL.literal("{123: 1, true: 2, null: 3}"), // invalid object
            DSL.literal("{\"invalid\":\"json\", \"string\"}"), // invalid object
            DSL.literal("[\"a\": 1, \"b\": 2]") // invalid array
            );

    expressions.stream()
        .forEach(
            expr ->
                assertThrows(
                    SemanticCheckException.class,
                    () -> DSL.castJson(expr).valueOf(),
                    "Expected to throw SemanticCheckException when calling castJson with " + expr));
  }

  @Test
  void json_returnsJsonObject() {
    FunctionExpression exp;

    // Setup
    final String objectJson =
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
    exp = DSL.json_function(DSL.literal(objectJson));

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprTupleValue);
    assertEquals(expectedTupleExpr, value);
  }

  @Test
  void json_returnsJsonArray() {
    FunctionExpression exp;

    // Setup
    final String arrayJson = "[\"foo\", \"fuzz\", true, \"bar\", 1234, 12.34, null]";
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
    exp = DSL.json_function(DSL.literal(arrayJson));

    // Verify
    var value = exp.valueOf();
    assertTrue(value instanceof ExprCollectionValue);
    assertEquals(expectedArrayExpr, value);
  }

  @Test
  void json_returnsScalar() {
    assertEquals(
        new ExprStringValue("foobar"), DSL.json_function(DSL.literal("\"foobar\"")).valueOf());

    assertEquals(new ExprIntegerValue(1234), DSL.json_function(DSL.literal("1234")).valueOf());

    assertEquals(LITERAL_TRUE, DSL.json_function(DSL.literal("true")).valueOf());

    assertEquals(LITERAL_NULL, DSL.json_function(DSL.literal("null")).valueOf());

    assertEquals(LITERAL_NULL, DSL.json_function(DSL.literal("")).valueOf());

    assertEquals(
        ExprTupleValue.fromExprValueMap(Map.of()), DSL.json_function(DSL.literal("{}")).valueOf());
  }

  @Test
  void json_returnsSemanticCheckException() {
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
