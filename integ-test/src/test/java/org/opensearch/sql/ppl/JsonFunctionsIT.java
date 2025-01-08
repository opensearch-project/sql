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
                "source=%s | eval json=cast(json_string to json) | fields json",
                TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"));
    verifyDataRows(
        result,
        rows("json object"),
        rows("json array"),
        rows("json scalar string"),
        rows("json empty string"));
  }

  @Test
  public void test_json() throws IOException {
    JSONObject result;

    result =
        executeQuery(
            String.format(
                "source=%s | eval json=json(json_string) | fields json", TEST_INDEX_JSON_TEST));
    verifySchema(result, schema("test_name", null, "string"));
    verifyDataRows(result, rows("json invalid object"));
  }
}
