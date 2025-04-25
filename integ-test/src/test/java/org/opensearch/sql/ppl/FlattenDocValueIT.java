/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.FLATTENED_VALUE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_FLATTENED_VALUE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.hamcrest.TypeSafeMatcher;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class FlattenDocValueIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(FLATTENED_VALUE);
  }

  @Test
  public void testFlattenDocValue() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s", TEST_INDEX_FLATTENED_VALUE));
    verifySchema(result, schema("log", "struct"));
    TypeSafeMatcher<JSONArray> expectedRow =
        rows(new JSONObject("{ \"json\" : { \"status\": \"SUCCESS\", \"time\": 100} }"));
    verifyDataRows(result, expectedRow, expectedRow, expectedRow, expectedRow, expectedRow);
  }

  @Test
  public void testFlattenDocValueWithFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields log, log.json, log.json.status, log.json.time",
                TEST_INDEX_FLATTENED_VALUE));
    verifySchema(
        result,
        schema("log", "struct"),
        schema("log.json", "struct"),
        schema("log.json.status", "string"),
        schema("log.json.time", "bigint"));
    TypeSafeMatcher<JSONArray> expectedRow =
        rows(
            new JSONObject("{ \"json\" : { \"status\": \"SUCCESS\", \"time\": 100} }"),
            new JSONObject("{ \"status\": \"SUCCESS\", \"time\": 100}"),
            "SUCCESS",
            100);
    verifyDataRows(result, expectedRow, expectedRow, expectedRow, expectedRow, expectedRow);
  }
}
