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

import com.google.gson.Gson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class JsonFunctionsIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
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
        rows("json nested array"),
        rows("json array"),
        rows("json scalar string"),
        rows("json scalar int"),
        rows("json scalar float"),
        rows("json scalar double"),
        rows("json scalar boolean true"),
        rows("json scalar boolean false"),
        rows("json empty string"));
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
    String jsonType;
    if (isCalciteEnabled()) {
      jsonType = "string";
    } else {
      jsonType = "undefined";
    }
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, jsonType));
    verifyDataRows(
        result,
        rows(
            "json nested object",
            new JSONObject(
                Map.of("a", "1", "b", Map.of("c", "3"), "d", List.of(Boolean.FALSE, 3)))),
        rows("json object", new JSONObject(Map.of("a", "1", "b", "2"))),
        rows("json array", new JSONArray(List.of(1, 2, 3, 4))),
        rows(
            "json nested array",
            new JSONArray(List.of(1, 2, 3, Map.of("true", true, "number", 123)))),
        rows("json scalar string", "abc"),
        rows("json scalar int", 1234),
        rows("json scalar float", 12.34f),
        rows("json scalar double", 2.99792458e8),
        rows("json scalar boolean true", true),
        rows("json scalar boolean false", false),
        rows("json empty string", null));
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
    String jsonType;
    if (isCalciteEnabled()) {
      jsonType = "string";
    } else {
      jsonType = "undefined";
    }
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, jsonType));
    JSONObject firstRow = new JSONObject(Map.of("c", 2));
    Object nestedArray = new JSONArray(List.of(1, 2, 3, Map.of("true", true, "number", 123)));
    if (isCalciteEnabled()) {
      nestedArray = nestedArray.toString();
    }
    Object nestedObject = new JSONObject(
            Map.of("a", "1", "b", Map.of("c", "3"), "d", List.of(Boolean.FALSE, 3)));
    if (isCalciteEnabled()) {
      nestedObject = new Gson().toJson(Map.of("d", List.of(Boolean.FALSE, 3), "a", "1", "b", Map.of("c", "3")));
    }
    verifyDataRows(
        result,
        rows(
            "json nested object", nestedObject
            ),
        rows("json object", new JSONObject(Map.of("a", "1", "b", "2"))),
        rows("json array", new JSONArray(List.of(1, 2, 3, 4))),
        rows(
            "json nested array",
            nestedArray),
        rows("json scalar string", "abc"),
        rows("json scalar int", 1234),
        rows("json scalar float", 12.34),
        rows("json scalar double", 2.99792458e8),
        rows("json scalar boolean true", true),
        rows("json scalar boolean false", false),
        rows("json empty string", null));
  }

  @Test
  public void test_cast_json_scalar_to_type() throws IOException {
    // cast to integer
    JSONObject result;

    result =
        executeQuery(
            String.format(
                "source=%s | "
                    + "where test_name='json scalar int' | "
                    + "eval casted=cast(json(json_string) as int) | "
                    + "fields test_name, casted",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, "int"));
    verifyDataRows(result, rows("json scalar int", 1234));

    result =
        executeQuery(
            String.format(
                "source=%s | "
                    + "where test_name='json scalar int' | "
                    + "eval casted=cast(json(json_string) as long) | "
                    + "fields test_name, casted",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, "bigint"));
    verifyDataRows(result, rows("json scalar int", 1234l));

    result =
        executeQuery(
            String.format(
                "source=%s | "
                    + "where test_name='json scalar float' | "
                    + "eval casted=cast(json(json_string) as float) | "
                    + "fields test_name, casted",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, "float"));
    verifyDataRows(result, rows("json scalar float", 12.34f));

    result =
        executeQuery(
            String.format(
                "source=%s | "
                    + "where test_name='json scalar double' | "
                    + "eval casted=cast(json(json_string) as double) | "
                    + "fields test_name, casted",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, "double"));
    verifyDataRows(result, rows("json scalar double", 2.99792458e8));

    result =
        executeQuery(
            String.format(
                "source=%s | where test_name='json scalar boolean true' OR test_name='json scalar"
                    + " boolean false' | eval casted=cast(json(json_string) as boolean) | fields"
                    + " test_name, casted",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"), schema("casted", null, "boolean"));
    verifyDataRows(
        result, rows("json scalar boolean true", true), rows("json scalar boolean false", false));
  }
}
