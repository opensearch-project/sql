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
  @Test
  public void json_valid_returns_false() {
    assertEquals(
        LITERAL_FALSE,
        DSL.jsonValid(DSL.literal(ExprValueUtils.stringValue("{\"invalid\":\"json\", \"string\"}")))
            .valueOf());
    assertEquals(
        LITERAL_FALSE, DSL.jsonValid(DSL.literal((ExprValueUtils.stringValue("abc")))).valueOf());
    assertEquals(LITERAL_FALSE, DSL.jsonValid(DSL.literal((LITERAL_NULL))).valueOf());
    assertEquals(LITERAL_FALSE, DSL.jsonValid(DSL.literal((LITERAL_MISSING))).valueOf());
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
                    DSL.jsonValid(DSL.literal((ExprValueUtils.stringValue(str)))).valueOf(),
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
    ExprValue expected = new ExprIntegerValue(1);
    execute_extract_json(expected, "{\"a\":1}", "$.a");
  }

  @Test
  void json_extract_search_arrays_out_of_bound() {
    execute_extract_json(LITERAL_NULL, "{\"a\":[1,2,3]}", "$.a[4]");
  }

  @Test
  void json_extract_search_arrays() {
    String jsonArray = "{\"a\":[1,2.3,\"abc\",true,null,{\"c\":{\"d\":1}},[1,2,3]]}";
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
      execute_extract_json(expectedExprValue.get(i), jsonArray, path);
    }

    // extract nested object
    ExprValue nestedExpected =
        ExprTupleValue.fromExprValueMap(Map.of("d", new ExprIntegerValue(1)));
    execute_extract_json(nestedExpected, jsonArray, "$.a[5].c");

    // extract * from JSON list
    ExprValue starExpected = new ExprCollectionValue(expectedExprValue);
    execute_extract_json(starExpected, jsonArray, "$.a[*]");
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

    jsonStrings.forEach(
        str -> execute_extract_json(LITERAL_NULL, str, "$.a.path_not_found_key")
    );

    // null json
    assertEquals(LITERAL_NULL, DSL.jsonExtract(DSL.literal(LITERAL_NULL), DSL.literal(new ExprStringValue("$.a"))).valueOf());

    // missing json
    assertEquals(LITERAL_MISSING, DSL.jsonExtract(DSL.literal(LITERAL_MISSING), DSL.literal(new ExprStringValue("$.a"))).valueOf());
  }

  @Test
  void json_extract_throws_SemanticCheckException() {
    // invalid path
    SemanticCheckException invalidPathError = assertThrows(
        SemanticCheckException.class,
        () ->
            DSL.jsonExtract(
                    DSL.literal(new ExprStringValue("{\"a\":1}")),
                    DSL.literal(new ExprStringValue("$a")))
                .valueOf());
    assertEquals(
            "JSON path '\"$a\"' is not valid. Error details: Illegal character at position 1 expected '.' or '['",
            invalidPathError.getMessage());


    // invalid json
    SemanticCheckException invalidJsonError = assertThrows(
        SemanticCheckException.class,
        () ->
            DSL.jsonExtract(
                    DSL.literal(new ExprStringValue("{\"invalid\":\"json\", \"string\"}")),
                    DSL.literal(new ExprStringValue("$.a")))
                .valueOf());
    assertEquals(
            "JSON string '\"{\"invalid\":\"json\", \"string\"}\"' is not valid. Error details: net.minidev.json.parser.ParseException: Unexpected character (}) at position 26.",
            invalidJsonError.getMessage());
  }

  @Test
  void json_extract_throws_ExpressionEvaluationException() {
    // null path
    assertThrows(
        ExpressionEvaluationException.class,
        () ->
            DSL.jsonExtract(
                    DSL.literal(new ExprStringValue("{\"a\":1}")), DSL.literal(LITERAL_NULL))
                .valueOf());

    // missing path
    assertThrows(
        ExpressionEvaluationException.class,
        () ->
            DSL.jsonExtract(
                    DSL.literal(new ExprStringValue("{\"a\":1}")), DSL.literal(LITERAL_MISSING))
                .valueOf());
  }

  private static void execute_extract_json(ExprValue expected, String json, String path) {
    Expression pathExpr = DSL.literal(ExprValueUtils.stringValue(path));
    Expression jsonExpr = DSL.literal(ExprValueUtils.stringValue(json));
    ExprValue actual = DSL.jsonExtract(jsonExpr, pathExpr).valueOf();
    assertEquals(expected, actual);
  }
}
