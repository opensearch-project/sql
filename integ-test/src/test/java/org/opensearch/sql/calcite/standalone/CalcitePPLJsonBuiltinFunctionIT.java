/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.gson;
import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
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
    loadIndex(Index.JSON_TEST);
    loadIndex(Index.GAME_OF_THRONES);
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

    verifySchema(actual, schema("a", "string"), schema("b", "string"));

    verifyDataRows(actual, rows("{\"key\":123.45}", "{\"outer\":\"{\\\"inner\\\":123.45}\"}"));
  }

  @Test
  public void testJsonArray() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = json_array(1, 2, 0, -1, 1.1, -0.11)| fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows(gson.toJson(List.of(1, 2, 0, -1, 1.1, -0.11))));
  }

  @Test
  public void testJsonArrayWithDifferentType() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = json_array(1, '123', json_object(\"name\",  3))| fields a |"
                    + " head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("[1,\"123\",\"{\\\"name\\\":3}\"]"));
  }

  @Test
  public void testToJsonString() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s |  eval a = to_json_string(name) | fields a" + " | head 1",
                TEST_INDEX_GAME_OF_THRONES));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(
        actual, rows("{\"firstname\":\"Daenerys\",\"lastname\":\"Targaryen\",\"ofHerName\":1}"));
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

  @Test
  public void testJsonExtract() {
    String candidate =
        "[\n"
            + "{\n"
            + "\"name\":\"London\",\n"
            + "\"Bridges\":[\n"
            + "{\"name\":\"Tower Bridge\",\"length\":801.0},\n"
            + "{\"name\":\"Millennium Bridge\",\"length\":1066.0}\n"
            + "]\n"
            + "},\n"
            + "{\n"
            + "\"name\":\"Venice\",\n"
            + "\"Bridges\":[\n"
            + "{\"name\":\"Rialto Bridge\",\"length\":157.0},\n"
            + "{\"type\":\"Bridge of Sighs\",\"length\":36.0},\n"
            + "{\"type\":\"Ponte della Paglia\"}\n"
            + "]\n"
            + "},\n"
            + "{\n"
            + "\"name\":\"San Francisco\",\n"
            + "\"Bridges\":[\n"
            + "{\"name\":\"Golden Gate Bridge\",\"length\":8981.0},\n"
            + "{\"name\":\"Bay Bridge\",\"length\":23556.0}\n"
            + "]\n"
            + "}\n"
            + "]";
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval a = json_extract('%s', '{}'), b= json_extract('%s',"
                    + " '{2}.Bridges{0}.length'), c=json_extract('%s', '{}.Bridges{}.type'),"
                    + " d=json_extract('%s', '{2}.Bridges{0}')| fields a, b,c, d | head 1",
                TEST_INDEX_PEOPLE2, candidate, candidate, candidate, candidate));

    verifySchema(
        actual,
        schema("a", "string"),
        schema("b", "string"),
        schema("c", "string"),
        schema("d", "string"));

    verifyDataRows(
        actual,
        rows(
            gson.toJson(gson.fromJson(candidate, List.class)),
            "8981.0",
            "[\"Bridge of Sighs\",\"Ponte della Paglia\"]",
            "{\"name\":\"Golden Gate Bridge\",\"length\":8981.0}"));
  }

  @Test
  public void testJsonExtractWithMultiplyResult() {
    String candidate =
        "[\n"
            + "{\n"
            + "\"name\":\"London\",\n"
            + "\"Bridges\":[\n"
            + "{\"name\":\"Tower Bridge\",\"length\":801.0},\n"
            + "{\"name\":\"Millennium Bridge\",\"length\":1066.0}\n"
            + "]\n"
            + "},\n"
            + "{\n"
            + "\"name\":\"Venice\",\n"
            + "\"Bridges\":[\n"
            + "{\"name\":\"Rialto Bridge\",\"length\":157.0},\n"
            + "{\"type\":\"Bridge of Sighs\",\"length\":36.0},\n"
            + "{\"type\":\"Ponte della Paglia\"}\n"
            + "]\n"
            + "},\n"
            + "{\n"
            + "\"name\":\"San Francisco\",\n"
            + "\"Bridges\":[\n"
            + "{\"name\":\"Golden Gate Bridge\",\"length\":8981.0},\n"
            + "{\"name\":\"Bay Bridge\",\"length\":23556.0}\n"
            + "]\n"
            + "}\n"
            + "]";
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval c=json_extract('%s', '{}.Bridges{}.type',"
                    + " '{2}.Bridges{0}.length')| fields  c | head 1",
                TEST_INDEX_PEOPLE2, candidate, candidate, candidate, candidate));

    verifySchema(actual, schema("c", "string"));

    verifyDataRows(actual, rows("[[\"Bridge of Sighs\",\"Ponte della Paglia\"],8981.0]"));
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

    verifySchema(actual, schema("a", "string"), schema("b", "string"));

    verifyDataRows(actual, rows("[\"f1\",\"f2\"]", null));
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
  public void testJsonSet() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a =json_set('{\"a\":[{\"b\":1},{\"b\":2}]}', 'a{}.b', '3')|"
                    + " fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("{\"a\":[{\"b\":\"3\"},{\"b\":\"3\"}]}"));
  }

  @Test
  public void testJsonDelete() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a"
                    + " =json_delete('{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}',"
                    + " 'age','gender')| fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("{\"account_number\":1,\"balance\":39225}"));
  }

  @Test
  public void testJsonDeleteWithNested() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a"
                    + " =json_delete('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}',"
                    + " 'f2.f3') | fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("{\"f1\":\"abc\",\"f2\":{\"f4\":\"b\"}}"));
  }

  @Test
  public void testJsonDeleteWithNestedNothing() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a"
                    + " =json_delete('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}',"
                    + " 'f2.f100') | fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}"));
  }

  @Test
  public void testJsonDeleteWithNestedAndArray() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a"
                    + " =json_delete('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}','teacher',"
                    + " 'student{}.rank') | fields a | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"));

    verifyDataRows(actual, rows("{\"student\":[{\"name\":\"Bob\"},{\"name\":\"Charlie\"}]}"));
  }

  @Test
  public void testJsonAppend() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a"
                    + " =json_append('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                    + " 'student', json_object(\"name\", \"Tomy\",\"rank\", 5)),  b ="
                    + " json_append('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                    + " 'teacher', 'Tom', 'teacher', 'Walt'),c ="
                    + " json_append('{\"school\":{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}',"
                    + " 'school.teacher', json_array(\"Tom\", \"Walt\"))| fields a, b, c | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"), schema("b", "string"), schema("c", "string"));

    verifyDataRows(
        actual,
        rows(
            "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2},\"{\\\"name\\\":\\\"Tomy\\\",\\\"rank\\\":5}\"]}",
            "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}",
            "{\"school\":{\"teacher\":[\"Alice\",\"[\\\"Tom\\\",\\\"Walt\\\"]\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}"));
  }

  @Test
  public void testJsonExtend() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a ="
                    + " json_extend('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                    + " 'student', json_object(\"name\", \"Tommy\",\"rank\", 5)),  b ="
                    + " json_extend('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                    + " 'teacher', 'Tom', 'teacher', 'Walt'),c ="
                    + " json_extend('{\"school\":{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}',"
                    + " 'school.teacher', json_array(\"Tom\", \"Walt\"))| fields a, b, c | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "string"), schema("b", "string"), schema("c", "string"));

    verifyDataRows(
        actual,
        rows(
            "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2},\"{\\\"name\\\":\\\"Tommy\\\",\\\"rank\\\":5}\"]}",
            "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}",
            "{\"school\":{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}"));
  }
}
