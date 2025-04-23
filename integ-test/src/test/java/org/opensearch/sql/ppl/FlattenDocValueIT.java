/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

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
    putDocument(
        "test",
        1,
        "{\"log\": { \"json\" : { \"status\": \"SUCCESS\", \"time\": 100} } }"); // non-flatten
    putDocument(
        "test", 2, "{\"log.json\": { \"status\": \"SUCCESS\", \"time\": 100} }"); // partly-flatten
    putDocument(
        "test", 3, "{\"log.json.status\":  \"SUCCESS\", \"log.json.time\": 100 }"); // fully-flatten
    putDocument(
        "test",
        4,
        "{\"log.json\": { \"status\": \"SUCCESS\" }, \"log.json.time\": 100 }"); // partly-flatten +
    // fully-flatten
    putDocument(
        "test",
        7,
        "{\"log\": { \"json\" : {} }, \"log.json\": { \"status\": \"SUCCESS\" }, \"log.json.time\":"
            + " 100 }"); // all modes in one
  }

  @Test
  public void testFlattenDocValue() throws IOException {
    JSONObject result = executeQuery("source=test");
    verifySchema(result, schema("log", "struct"));
    TypeSafeMatcher<JSONArray> expectedRow =
        rows(new JSONObject("{ \"json\" : { \"status\": \"SUCCESS\", \"time\": 100} }"));
    verifyDataRows(result, expectedRow, expectedRow, expectedRow, expectedRow, expectedRow);
  }

  @Test
  public void testFlattenDocValueWithFields() throws IOException {
    JSONObject result =
        executeQuery("source=test | fields log, log.json, log.json.status, log.json.time");
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
