/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.gson;
import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

public class CalcitePPLJsonBuiltinFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.DATE_FORMATS);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.DATE);
    loadIndex(Index.PEOPLE2);
    loadIndex(Index.BANK);
  }

  @Test
  public void testJson() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = json('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]'),"
                    + " b=json('{\"invalid\": \"json\"')| fields a,b | head 1",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(actual, schema("a", "string"), schema("b", "string"));

    verifyDataRows(actual, rows("[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]", null));
  }

  @Test
  public void testJsonObject() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = json_object('key', 123.45), b=json_object('outer',"
                    + " json_object('inner', 123.45))| fields a, b | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "struct"), schema("b", "struct"));

    verifyDataRows(
        actual,
        rows(
            gson.fromJson("{\"key\":123.45}", Map.class),
            gson.fromJson("{\"outer\":{\"inner\":123.45}}", Map.class)));
  }

  @Test
  public void testJsonArray() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = json_array(1, 2, 0, -1, 1.1, -0.11)| fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "array"));

    verifyDataRows(actual, rows(List.of(1.0, 2.0, 0, -1.0, 1.1, -0.11)));
  }

  @Ignore(
      "We do throw the exception, but current way will resolve safe, so we will get Unsupported"
          + " operator: json_array")
  @Test
  public void testJsonArrayWithWrongType() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              JSONObject actual =
                  executeQuery(
                      String.format(
                          "source=%s | eval a = json_array(1, 2, 0, true, \"hello\", -0.11)| fields"
                              + " a | head 1",
                          TEST_INDEX_PEOPLE2));
            });
    verifyErrorMessageContains(e, "unsupported format");
  }

  @Test
  public void testToJsonString() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = to_json_string(json_array(1, 2, 0, -1, 1.1, -0.11))| fields a"
                    + " | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("[1.0,2.0,0.0,-1.0,1.1,-0.11]"));
  }

  @Test
  public void testJsonArrayLength() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = json_array_length('[1,2,3,4]'), b ="
                    + " json_array_length('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]'), c ="
                    + " json_array_length('{\"key\": 1}') | fields a,b,c | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "integer"), schema("b", "integer"), schema("c", "integer"));

    verifyDataRows(actual, rows(4, 5, null));
  }

  @Ignore
  @Test
  public void testJsonExtract() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = json_extract('{\"a\":\"b\"}', '$.a'), b ="
                    + " json_extract('{\"a\":[{\"b\":1},{\"b\":2}]}', '$.a[1].b'), c ="
                    + " json_extract('{\"a\":[{\"b\":1},{\"b\":2}]}', '$.a[*].b'), d ="
                    + " json_extract('{\"invalid\": \"json\"') | fields a,b,c,d | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(
        actual,
        schema("a", "string"),
        schema("b", "string"),
        schema("c", "string"),
        schema("d", "string"));

    verifyDataRows(actual, rows("b", "2", "[1,2]", null));
  }

  @Test
  public void testJsonExtractWithNewPath() {
    String candidate = "[\n" +
            "    {\n" +
            "      \"name\": \"London\",\n" +
            "      \"Bridges\": [\n" +
            "        { \"name\": \"Tower Bridge\", \"length\": 801 },\n" +
            "        { \"name\": \"Millennium Bridge\", \"length\": 1066 }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"Venice\",\n" +
            "      \"Bridges\": [\n" +
            "        { \"name\": \"Rialto Bridge\", \"length\": 157 },\n" +
            "        { \"name\": \"Bridge of Sighs\", \"length\": 36 },\n" +
            "        { \"name\": \"Ponte della Paglia\" }\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"San Francisco\",\n" +
            "      \"Bridges\": [\n" +
            "        { \"name\": \"Golden Gate Bridge\", \"length\": 8981 },\n" +
            "        { \"name\": \"Bay Bridge\", \"length\": 23556 }\n" +
            "       ]\n" +
            "     }\n" +
            "   ]";
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | head 1 | eval a = json_extract('%s', '{}'), b= json_extract('%s', '{2}.Bridges{0}.length'), c= json_extract('%s', '{2}'), d=json_extract('%s', '{2}.Bridges{0}')| fields a, b, c, d | head 1",
                            TEST_INDEX_PEOPLE2, candidate, candidate, candidate, candidate));

    verifySchema(
            actual,
            schema("a", "string"),
            schema("b", "string"),
            schema("c", "string"),
            schema("d", "string"));

    verifyDataRows(actual, rows("b", "2", "[1,2]", null));
  }

  @Test
  public void testJsonKeys() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a ="
                    + " json_keys('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}'), b"
                    + " =json_keys('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]') | fields a,b | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "array"), schema("b", "array"));

    verifyDataRows(actual, rows(List.of("f1", "f2"), null));
  }

  @Test
  public void testJsonValid() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a =json_valid('[1,2,3,4]'), b =json_valid('{\"invalid\":"
                    + " \"json\"') | fields a,b | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "boolean"), schema("b", "boolean"));

    verifyDataRows(actual, rows(true, false));
  }

  @Test
  public void testArray() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a =array(1, 2, 0, -1, 1.1, -0.11) | fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "array"));

    verifyDataRows(actual, rows(List.of(1.0, 2.0, 0, -1.0, 1.1, -0.11)));
  }

  @Test
  public void testJsonSet() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a =json_set('{\"a\":[{\"b\":1},{\"b\":2}]}', array('a.b', '3'))|"
                    + " fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "struct"));

    verifyDataRows(actual, rows(gson.fromJson("{\"a\":[{\"b\":3},{\"b\":3}]}", Map.class)));
  }

  @Test
  public void testJsonDelete() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a"
                    + " =json_delete('{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}',"
                    + " array('age','gender'))| fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "struct"));

    verifyDataRows(
        actual, rows(gson.fromJson("{\"account_number\":1,\"balance\":39225}", Map.class)));
  }

  @Test
  public void testJsonDeleteWithNested() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a"
                    + " =json_delete('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}',"
                    + " array('f2.f3')) | fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "struct"));

    verifyDataRows(
        actual, rows(gson.fromJson("{\"f1\":\"abc\",\"f2\":{\"f4\":\"b\"}}", Map.class)));
  }

  @Test
  public void testJsonDeleteWithNestedAndArry() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a"
                    + " =json_delete('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',array('teacher',"
                    + " 'student.rank')) | fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "struct"));

    verifyDataRows(
        actual,
        rows(
            gson.fromJson("{\"student\":[{\"name\":\"Bob\"},{\"name\":\"Charlie\"}]}", Map.class)));
  }

  @Test
  public void testJsonAppend() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a"
                    + " =json_append('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                    + " array('student', '{\"name\":\"Tomy\",\"rank\":5}')),  b ="
                    + " json_append('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                    + " array('teacher', '\"Tom\"', 'teacher', '\"Walt\"')),c ="
                    + " json_append('{\"school\":{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}',"
                    + " array('school.teacher', '[\"Tom\", \"Walt\"]'))| fields a, b, c | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "struct"), schema("b", "struct"), schema("c", "struct"));

    verifyDataRows(
        actual,
        rows(
            gson.fromJson(
                "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2},{\"name\":\"Tomy\",\"rank\":5}]}",
                Map.class),
            gson.fromJson(
                "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}",
                Map.class),
            gson.fromJson(
                "{\"school\":{\"teacher\":[\"Alice\",[\"Tom\",\"Walt\"]],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}",
                Map.class)));
  }

  @Test
  public void testJsonExtend() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a ="
                    + " json_extend('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                    + " array('student', '{\"name\":\"Tommy\",\"rank\":5}')),  b ="
                    + " json_extend('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                    + " array('teacher', '\"Tom\"', 'teacher', '\"Walt\"')),c ="
                    + " json_extend('{\"school\":{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}',"
                    + " array('school.teacher', '[\"Tom\", \"Walt\"]'))| fields a, b, c | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "struct"), schema("b", "struct"), schema("c", "struct"));

    verifyDataRows(
        actual,
        rows(
            gson.fromJson(
                "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2},{\"name\":\"Tommy\",\"rank\":5}]}",
                Map.class),
            gson.fromJson(
                "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}",
                Map.class),
            gson.fromJson(
                "{\"school\":{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}",
                Map.class)));
  }
}
