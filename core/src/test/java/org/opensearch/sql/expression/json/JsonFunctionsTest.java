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
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;

@ExtendWith(MockitoExtension.class)
public class JsonFunctionsTest {

  private static final String JsonSetTestData =
          "{\"members\":[{\"name\":\"Alice\",\"age\":19,\"phoneNumbers\":[{\"home\":\"alice_home_landline\"},{\"work\":\"alice_work_phone\"}]},{\"name\":\"Ben\",\"age\":30,\"phoneNumbers\":[{\"home\":\"ben_home_landline\"},{\"work\":\"ben_work_phone\"}]}]}";

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
  void json_extract_search_arrays() {
    String jsonArray = "{\"a\":[1,2.3,\"abc\",true,null,{\"c\":{\"d\":1}},[1,2,3]]}";
    List<ExprValue> expectedExprValues =
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
    for (int i = 0; i < expectedExprValues.size(); i++) {
      String path = String.format("$.a[%d]", i);
      execute_extract_json(expectedExprValues.get(i), jsonArray, path);
    }

    // extract nested object
    ExprValue nestedExpected =
        ExprTupleValue.fromExprValueMap(Map.of("d", new ExprIntegerValue(1)));
    execute_extract_json(nestedExpected, jsonArray, "$.a[5].c");

    // extract * from JSON list
    ExprValue starExpected = new ExprCollectionValue(expectedExprValues);
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

    jsonStrings.forEach(str -> execute_extract_json(LITERAL_NULL, str, "$.a.path_not_found_key"));

    // null string literal
    assertEquals(LITERAL_NULL, DSL.jsonExtract(DSL.literal("null"), DSL.literal("$.a")).valueOf());

    // null json
    assertEquals(
        LITERAL_NULL, DSL.jsonExtract(DSL.literal(LITERAL_NULL), DSL.literal("$.a")).valueOf());

    // missing json
    assertEquals(
        LITERAL_MISSING,
        DSL.jsonExtract(DSL.literal(LITERAL_MISSING), DSL.literal("$.a")).valueOf());

    // array out of bounds
    execute_extract_json(LITERAL_NULL, "{\"a\":[1,2,3]}", "$.a[4]");
  }

  @Test
  void json_extract_throws_SemanticCheckException() {
    // invalid path
    SemanticCheckException invalidPathError =
        assertThrows(
            SemanticCheckException.class,
            () -> DSL.jsonExtract(DSL.literal("{\"a\":1}"), DSL.literal("$a")).valueOf());
    assertEquals(
        "JSON path '$a' is not valid. Error details: Illegal character at position 1 expected"
            + " '.' or '['",
        invalidPathError.getMessage());

    // invalid json
    SemanticCheckException invalidJsonError =
        assertThrows(
            SemanticCheckException.class,
            () ->
                DSL.jsonExtract(
                        DSL.literal("{\"invalid\":\"json\", \"string\"}"), DSL.literal("$.a"))
                    .valueOf());
    assertTrue(
        invalidJsonError
            .getMessage()
            .startsWith(
                "JSON string '{\"invalid\":\"json\", \"string\"}' is not valid. Error"
                    + " details:"));
  }

  @Test
  void json_extract_throws_ExpressionEvaluationException() {
    // null path
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.jsonExtract(DSL.literal("{\"a\":1}"), DSL.literal(LITERAL_NULL)).valueOf());

    // missing path
    assertThrows(
        ExpressionEvaluationException.class,
        () -> DSL.jsonExtract(DSL.literal("{\"a\":1}"), DSL.literal(LITERAL_MISSING)).valueOf());
  }

  @Test
  void json_extract_search_list_of_paths() {
    final String objectJson =
        "{\"foo\": \"foo\", \"fuzz\": true, \"bar\": 1234, \"bar2\": 12.34, \"baz\": null, "
            + "\"obj\": {\"internal\": \"value\"}, \"arr\": [\"string\", true, null]}";

    ExprValue expected =
        new ExprCollectionValue(
            List.of(new ExprStringValue("foo"), new ExprFloatValue(12.34), LITERAL_NULL));
    Expression pathExpr1 = DSL.literal(ExprValueUtils.stringValue("$.foo"));
    Expression pathExpr2 = DSL.literal(ExprValueUtils.stringValue("$.bar2"));
    Expression pathExpr3 = DSL.literal(ExprValueUtils.stringValue("$.potato"));
    Expression jsonExpr = DSL.literal(ExprValueUtils.stringValue(objectJson));
    ExprValue actual = DSL.jsonExtract(jsonExpr, pathExpr1, pathExpr2, pathExpr3).valueOf();
    assertEquals(expected, actual);
  }


  @Test
  void json_set_InsertByte() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("{}"), DSL.literal("$.test"), DSL.literal((byte) 'a'));
    assertEquals("{\"test\":97}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertShort() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("{}"), DSL.literal("$.test"), DSL.literal(Short.valueOf("123")));
    assertEquals("{\"test\":123}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertInt() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("{}"), DSL.literal("$.test"), DSL.literal(123));
    assertEquals("{\"test\":123}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertLong() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("{}"), DSL.literal("$.test"), DSL.literal(123L));
    assertEquals("{\"test\":123}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertFloat() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("{}"), DSL.literal("$.test"), DSL.literal(123.123F));
    assertEquals("{\"test\":123.123}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertDouble() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("{}"), DSL.literal("$.test"), DSL.literal(123.123));
    assertEquals("{\"test\":123.123}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertString() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("{}"), DSL.literal("$.test"), DSL.literal("test_value"));
    assertEquals("{\"test\":\"test_value\"}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertBoolean() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("{}"), DSL.literal("$.test"), DSL.literal(Boolean.TRUE));
    assertEquals("{\"test\":true}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertDate() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{}"),
                    DSL.literal("$.test"),
                    DSL.date(DSL.literal(new ExprDateValue("2020-08-17"))));
    assertEquals("{\"test\":\"2020-08-17\"}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertTime() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{}"),
                    DSL.literal("$.test"),
                    DSL.time(DSL.literal(new ExprTimeValue("01:01:01"))));
    assertEquals("{\"test\":\"01:01:01\"}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertTimestamp() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{}"),
                    DSL.literal("$.test"),
                    DSL.timestamp(DSL.literal("2008-05-15 22:00:00")));
    assertEquals("{\"test\":\"2008-05-15 22:00:00\"}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertInterval() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{}"),
                    DSL.literal("$.test"),
                    DSL.interval(DSL.literal(1), DSL.literal("second")));
    assertEquals("{\"test\":{\"seconds\":1}}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertIp() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{}"), DSL.literal("$.test"), DSL.castIp(DSL.literal("192.168.1.1")));
    assertEquals("{\"test\":\"192.168.1.1\"}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertMap() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{}"),
                    DSL.literal("$.test"),
                    DSL.literal(
                            ExprTupleValue.fromExprValueMap(Map.of("name", new ExprStringValue("alice")))));
    assertEquals("{\"test\":{\"name\":\"alice\"}}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_InsertArray() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{}"),
                    DSL.literal("$.test"),
                    DSL.literal(
                            new ExprCollectionValue(
                                    List.of(new ExprStringValue("Alice"), new ExprStringValue("Ben")))));
    assertEquals("{\"test\":[\"Alice\",\"Ben\"]}", functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_insert_invalid_path() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
                    DSL.literal("$$$$$$$$$"),
                    DSL.literal("18"));
    assertThrows(SemanticCheckException.class, functionExpression::valueOf);
  }

  @Test
  void json_set_insert_invalid_jsonObject() {
    FunctionExpression functionExpression =
            DSL.jsonSet(DSL.literal("[xxxx}}}}}"), DSL.literal("$.test"), DSL.literal("18"));
    assertThrows(SemanticCheckException.class, functionExpression::valueOf);
  }

  @Test
  void json_set_noMatch_property() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
                    DSL.literal("$.members[0].age.innerAge"),
                    DSL.literal("18"));
    assertEquals(
            "{\"members\":[{\"name\":\"alice\",\"age\":{\"innerAge\":\"18\"}}]}",
            functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_noMatch_array() {
    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
                    DSL.literal("$.members[0].age.innerArray"),
                    DSL.literal(
                            new ExprCollectionValue(
                                    List.of(new ExprStringValue("18"), new ExprStringValue("20")))));
    assertEquals(
            "{\"members\":[{\"name\":\"alice\",\"age\":{\"innerArray\":[\"18\",\"20\"]}}]}",
            functionExpression.valueOf().stringValue());
  }

  /**
   * In the case of jsonPath hit single match on property, it should overwrite the existing value,
   * regardless of the value type (Array, numeric....etc.)
   */
  @Test
  void json_set_singleMatch_property() {

    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal(JsonSetTestData),
                    DSL.literal("$.members[0].name"),
                    DSL.literal(new ExprStringValue("Alice Spring")));
    assertEquals(
            "{\"members\":[{\"name\":\"Alice"
                    + " Spring\",\"age\":19,\"phoneNumbers\":[{\"home\":\"alice_home_landline\"},{\"work\":\"alice_work_phone\"}]},{\"name\":\"Ben\",\"age\":30,\"phoneNumbers\":[{\"home\":\"ben_home_landline\"},{\"work\":\"ben_work_phone\"}]}]}",
            functionExpression.valueOf().stringValue());
  }

  /**
   * In the case of jsonPath hit single match on property, it should overwrite the existing value,
   * regardless of the value type (Array, numeric....etc.)
   */
  @Test
  void json_set_singleMatch_array() {

    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal(JsonSetTestData),
                    DSL.literal("$.members[0].phoneNumbers"),
                    DSL.literal(
                            new ExprCollectionValue(
                                    List.of(
                                            ExprTupleValue.fromExprValueMap(
                                                    Map.of("home", new ExprStringValue("alice_new_landline"))),
                                            ExprTupleValue.fromExprValueMap(
                                                    Map.of("work", new ExprStringValue("alice_new_work_phone")))))));
    assertEquals(
            "{\"members\":[{\"name\":\"Alice\",\"age\":19,\"phoneNumbers\":[{\"home\":\"alice_new_landline\"},{\"work\":\"alice_new_work_phone\"}]},{\"name\":\"Ben\",\"age\":30,\"phoneNumbers\":[{\"home\":\"ben_home_landline\"},{\"work\":\"ben_work_phone\"}]}]}",
            functionExpression.valueOf().stringValue());
  }

  /** The handling would stay identical regardless of single match || multiple matches. */
  @Test
  void json_set_multiMatches_property() {

    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal(JsonSetTestData),
                    DSL.literal("$.members..age"),
                    DSL.literal(new ExprLongValue(25)));
    assertEquals(
            "{\"members\":[{\"name\":\"Alice\",\"age\":25,\"phoneNumbers\":[{\"home\":\"alice_home_landline\"},{\"work\":\"alice_work_phone\"}]},{\"name\":\"Ben\",\"age\":25,\"phoneNumbers\":[{\"home\":\"ben_home_landline\"},{\"work\":\"ben_work_phone\"}]}]}",
            functionExpression.valueOf().stringValue());
  }

  @Test
  void json_set_multiMatches_array() {

    FunctionExpression functionExpression =
            DSL.jsonSet(
                    DSL.literal(JsonSetTestData),
                    DSL.literal("$.members..phoneNumbers"),
                    DSL.literal(
                            new ExprCollectionValue(
                                    List.of(
                                            ExprTupleValue.fromExprValueMap(
                                                    Map.of("home", new ExprStringValue("generic_new_landline"))),
                                            ExprTupleValue.fromExprValueMap(
                                                    Map.of("work", new ExprStringValue("generic_new_work_phone")))))));
    assertEquals(
            "{\"members\":[{\"name\":\"Alice\",\"age\":19,\"phoneNumbers\":[{\"home\":\"generic_new_landline\"},{\"work\":\"generic_new_work_phone\"}]},{\"name\":\"Ben\",\"age\":30,\"phoneNumbers\":[{\"home\":\"generic_new_landline\"},{\"work\":\"generic_new_work_phone\"}]}]}",
            functionExpression.valueOf().stringValue());
  }

  private static void execute_extract_json(ExprValue expected, String json, String path) {
    Expression pathExpr = DSL.literal(ExprValueUtils.stringValue(path));
    Expression jsonExpr = DSL.literal(ExprValueUtils.stringValue(json));
    ExprValue actual = DSL.jsonExtract(jsonExpr, pathExpr).valueOf();
    assertEquals(expected, actual);
  }
}
