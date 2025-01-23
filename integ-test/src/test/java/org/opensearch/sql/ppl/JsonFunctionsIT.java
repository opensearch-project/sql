/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_JSON_TEST;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class JsonFunctionsIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.JSON_TEST);
  }

  @Test
  public void test_json_valid() throws IOException {
    JSONObject result;

    result =
        executeQuery(
            String.format(
                "source=%s | where json_valid(json_string) | fields test_name",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"));
    verifyDataRows(
        result,
        rows("json nested object"),
        rows("json object"),
        rows("json array"),
        rows("json scalar string"),
        rows("json empty string"),
        rows("json nested list"));
  }

  @Test
  public void test_not_json_valid() throws IOException {
    JSONObject result;

    result =
        executeQuery(
            String.format(
                "source=%s | where not json_valid(json_string) | fields test_name",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"));
    verifyDataRows(result, rows("json invalid object"), rows("json null"));
  }

  @Test
  public void test_cast_json() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where json_valid(json_string) | eval casted=cast(json_string as json)"
                    + " | fields test_name, casted",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, "undefined"));
    verifyDataRows(
        result,
        rows(
            "json nested object",
            new JSONObject(Map.of("a", "1", "b", Map.of("c", "3"), "d", List.of(1, 2, 3)))),
        rows("json object", new JSONObject(Map.of("a", "1", "b", "2"))),
        rows("json array", new JSONArray(List.of(1, 2, 3, 4))),
        rows("json scalar string", "abc"),
        rows("json empty string", null),
        rows(
            "json nested list",
            new JSONObject(Map.of("a", "1", "b", List.of(Map.of("c", "2"), Map.of("c", "3"))))));
  }

  @Test
  public void test_json() throws IOException {
    JSONObject result;

    result =
        executeQuery(
            String.format(
                "source=%s | where json_valid(json_string) | eval casted=json(json_string) | fields"
                    + " test_name, casted",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, "undefined"));
    JSONObject firstRow = new JSONObject(Map.of("c", 2));
    verifyDataRows(
        result,
        rows(
            "json nested object",
            new JSONObject(Map.of("a", "1", "b", Map.of("c", "3"), "d", List.of(1, 2, 3)))),
        rows("json object", new JSONObject(Map.of("a", "1", "b", "2"))),
        rows("json array", new JSONArray(List.of(1, 2, 3, 4))),
        rows("json scalar string", "abc"),
        rows("json empty string", null),
        rows(
            "json nested list",
            new JSONObject(Map.of("a", "1", "b", List.of(Map.of("c", "2"), Map.of("c", "3"))))));
  }

  @Test
  public void test_json_extract() throws IOException {
    JSONObject result;
    result =
        executeQuery(
            String.format(
                "source=%s | where json_valid(json_string) | eval"
                    + " extracted=json_extract(json_string, '$.b') | fields test_name, extracted",
                TEST_INDEX_JSON_TEST));
    verifySchema(
        result, schema("test_name", null, "string"), schema("extracted", null, "undefined"));
    verifyDataRows(
        result,
        rows("json nested object", new JSONObject(Map.of("c", "3"))),
        rows("json object", "2"),
        rows("json array", null),
        rows("json scalar string", null),
        rows("json empty string", null),
        rows("json nested list", new JSONArray(List.of(Map.of("c", "2"), Map.of("c", "3")))));
  }
}
