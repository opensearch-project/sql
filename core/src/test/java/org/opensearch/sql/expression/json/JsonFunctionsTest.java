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
import org.opensearch.sql.data.model.ExprFloatValue;
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
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;

@ExtendWith(MockitoExtension.class)
public class JsonFunctionsTest {
  private static final ExprValue JsonNestedObject =
      ExprValueUtils.stringValue("{\"a\":\"1\",\"b\":{\"c\":\"2\",\"d\":\"3\"}}");
  private static final ExprValue JsonObject =
      ExprValueUtils.stringValue("{\"a\":\"1\",\"b\":\"2\"}");
  private static final ExprValue JsonArray = ExprValueUtils.stringValue("[1, 2, 3, 4]");
  private static final ExprValue JsonScalarString = ExprValueUtils.stringValue("\"abc\"");
  private static final ExprValue JsonEmptyString = ExprValueUtils.stringValue("");
  private static final ExprValue JsonInvalidObject =
      ExprValueUtils.stringValue("{\"invalid\":\"json\", \"string\"}");
  private static final ExprValue JsonInvalidScalar = ExprValueUtils.stringValue("abc");

  @Test
  public void json_valid_returns_false() {
    assertEquals(LITERAL_FALSE, execute(JsonInvalidObject));
    assertEquals(LITERAL_FALSE, execute(JsonInvalidScalar));
    assertEquals(LITERAL_FALSE, execute(LITERAL_NULL));
    assertEquals(LITERAL_FALSE, execute(LITERAL_MISSING));
  }

  @Test
  public void json_valid_throws_ExpressionEvaluationException() {
    assertThrows(
        ExpressionEvaluationException.class, () -> execute(ExprValueUtils.booleanValue(true)));
  }

  @Test
  public void json_valid_returns_true() {
    assertEquals(LITERAL_TRUE, execute(JsonNestedObject));
    assertEquals(LITERAL_TRUE, execute(JsonObject));
    assertEquals(LITERAL_TRUE, execute(JsonArray));
    assertEquals(LITERAL_TRUE, execute(JsonScalarString));
    assertEquals(LITERAL_TRUE, execute(JsonEmptyString));
  }

  private ExprValue execute(ExprValue jsonString) {
    FunctionExpression exp = DSL.jsonValid(DSL.literal(jsonString));
    return exp.valueOf();
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

    // missing quote
    assertThrows(
        SemanticCheckException.class, () -> DSL.castJson(DSL.literal("\"missing quote")).valueOf());
  }

  @Test
  void json_extract_search() {
    Expression jsonArray = DSL.literal(ExprValueUtils.stringValue("{\"a\":1}"));
    ExprValue expectedExprValue = new ExprIntegerValue(1);
    Expression pathExpr = DSL.literal(ExprValueUtils.stringValue("$.a"));
    FunctionExpression expression = DSL.jsonExtract(jsonArray, pathExpr);
    assertEquals(expectedExprValue, expression.valueOf());
  }

  @Test
  void json_extract_search_arrays_out_of_bound() {
    Expression jsonArray = DSL.literal(ExprValueUtils.stringValue("{\"a\":[1,2,3}"));

    // index out of bounds
    assertThrows(
        SemanticCheckException.class,
        () -> DSL.jsonExtract(jsonArray, DSL.literal(new ExprStringValue("$.a[3]"))).valueOf());

    // negative index
    assertThrows(
        SemanticCheckException.class,
        () -> DSL.jsonExtract(jsonArray, DSL.literal(new ExprStringValue("$.a[-1]"))).valueOf());
  }

  @Test
  void json_extract_search_arrays() {
    Expression jsonArray =
        DSL.literal(
            ExprValueUtils.stringValue(
                "{\"a\":[1,2.3,\"abc\",true,null,{\"c\":{\"d\":1}},[1,2,3]]}"));
    List<ExprValue> expectedExprValue =
        List.of(
            new ExprIntegerValue(1),
            new ExprFloatValue(2.3),
            new ExprStringValue("abc"),
            LITERAL_TRUE,
            LITERAL_NULL,
            ExprTupleValue.fromExprValueMap(
                Map.of("c", ExprTupleValue.fromExprValueMap(Map.of("d", new ExprIntegerValue(1))))),
            new ExprCollectionValue(
                List.of(
                    new ExprIntegerValue(1), new ExprIntegerValue(2), new ExprIntegerValue(3))));

    // extract specific index from JSON list
    for (int i = 0; i < expectedExprValue.size(); i++) {
      String path = String.format("$.a[%d]", i);
      Expression pathExpr = DSL.literal(ExprValueUtils.stringValue(path));
      FunctionExpression expression = DSL.jsonExtract(jsonArray, pathExpr);
      assertEquals(expectedExprValue.get(i), expression.valueOf());
    }

    // extract nested object
    ExprValue nestedExpected =
        ExprTupleValue.fromExprValueMap(Map.of("d", new ExprIntegerValue(1)));
    Expression nestedPath = DSL.literal(ExprValueUtils.stringValue("$.a[5].c"));
    FunctionExpression nestedExpression = DSL.jsonExtract(jsonArray, nestedPath);
    assertEquals(nestedExpected, nestedExpression.valueOf());

    // extract * from JSON list
    Expression starPath = DSL.literal(ExprValueUtils.stringValue("$.a[*]"));
    FunctionExpression starExpression = DSL.jsonExtract(jsonArray, starPath);
    assertEquals(new ExprCollectionValue(expectedExprValue), starExpression.valueOf());
  }

  @Test
  void json_extract_returns_null() {
    List<String> jsonStrings =
        List.of(
            "{\"a\":\"1\",\"b\":\"2\"}",
            "{\"a\":1,\"b\":{\"c\":2,\"d\":3}}",
            "{\"arr1\": [1,2,3], \"arr2\": [4,5,6]}",
            "[1, 2, 3, 4]",
            "[{\"a\":1,\"b\":2}, {\"c\":3,\"d\":2}]",
            "\"abc\"",
            "1234",
            "12.34",
            "true",
            "false",
            "");

    jsonStrings.stream()
        .forEach(
            str ->
                assertEquals(
                    LITERAL_NULL,
                    DSL.jsonExtract(
                            DSL.literal((ExprValueUtils.stringValue(str))),
                            DSL.literal("$.a.path_not_found_key"))
                        .valueOf(),
                    String.format("JSON string %s should return null", str)));
  }

  @Test
  void json_extract_throws_SemanticCheckException() {
    // invalid path
    assertThrows(
        SemanticCheckException.class,
        () ->
            DSL.jsonExtract(
                    DSL.literal(new ExprStringValue("{\"a\":1}")),
                    DSL.literal(new ExprStringValue("$a")))
                .valueOf());

    // invalid json
    assertThrows(
        SemanticCheckException.class,
        () ->
            DSL.jsonExtract(
                    DSL.literal(new ExprStringValue("{\"invalid\":\"json\", \"string\"}")),
                    DSL.literal(new ExprStringValue("$.a")))
                .valueOf());
  }

  @Test
  void json_extract_throws_ExpressionEvaluationException() {
    // null json
    assertThrows(
        ExpressionEvaluationException.class,
        () ->
            DSL.jsonExtract(DSL.literal(LITERAL_NULL), DSL.literal(new ExprStringValue("$.a")))
                .valueOf());

    // null path
    assertThrows(
        ExpressionEvaluationException.class,
        () ->
            DSL.jsonExtract(
                    DSL.literal(new ExprStringValue("{\"a\":1}")), DSL.literal(LITERAL_NULL))
                .valueOf());

    // missing json
    assertThrows(
        ExpressionEvaluationException.class,
        () ->
            DSL.jsonExtract(DSL.literal(LITERAL_MISSING), DSL.literal(new ExprStringValue("$.a")))
                .valueOf());

    // missing path
    assertThrows(
        ExpressionEvaluationException.class,
        () ->
            DSL.jsonExtract(
                    DSL.literal(new ExprStringValue("{\"a\":1}")), DSL.literal(LITERAL_MISSING))
                .valueOf());
  }
}
