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
  private static final String JsonSetTestData =
      "{\"members\":[{\"name\":\"Alice\",\"age\":19,\"phoneNumbers\":[{\"home\":\"alice_home_landline\"},{\"work\":\"alice_work_phone\"}]},{\"name\":\"Ben\",\"age\":30,\"phoneNumbers\":[{\"home\":\"ben_home_landline\"},{\"work\":\"ben_work_phone\"}]}]}";

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

    // mnissing quote
    assertThrows(
        SemanticCheckException.class, () -> DSL.castJson(DSL.literal("\"missing quote")).valueOf());
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
  void json_set_insert_new_property_nested() {
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
  void json_set_insert_invalid_path() {
    FunctionExpression functionExpression =
        DSL.jsonSet(
            DSL.literal("{\"members\":[{\"name\":\"alice\"}]}"),
            DSL.literal("$$$$$$$$$"),
            DSL.literal("18"));
    assertThrows(SemanticCheckException.class, () -> functionExpression.valueOf());
  }

  @Test
  void json_set_insert_invalid_jsonObject() {
    FunctionExpression functionExpression =
        DSL.jsonSet(DSL.literal("[xxxx}}}}}"), DSL.literal("$.test"), DSL.literal("18"));
    assertThrows(SemanticCheckException.class, () -> functionExpression.valueOf());
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
}
