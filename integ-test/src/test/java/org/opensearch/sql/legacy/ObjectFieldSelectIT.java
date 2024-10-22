/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DEEP_NESTED;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Integration test for OpenSearch object field (and nested field). This class is focused on simple
 * SELECT-FROM query to ensure right column number and value is returned.
 */
public class ObjectFieldSelectIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.DEEP_NESTED);
  }

  @Test
  public void testSelectObjectFieldItself() {
    JSONObject response = new JSONObject(query("SELECT city FROM %s"));

    verifySchema(response, schema("city", null, "object"));

    // Expect object field itself is returned in a single cell
    verifyDataRows(
        response,
        rows(
            new JSONObject(
                "{\n"
                    + "  \"name\": \"Seattle\",\n"
                    + "  \"location\": {\"latitude\": 10.5}\n"
                    + "}")));
  }

  @Test
  public void testSelectObjectInnerFields() {
    JSONObject response =
        new JSONObject(query("SELECT city.location, city.location.latitude FROM %s"));

    verifySchema(
        response,
        schema("city.location", null, "object"),
        schema("city.location.latitude", null, "double"));

    // Expect inner regular or object field returned in its single cell
    verifyDataRows(response, rows(new JSONObject("{\"latitude\": 10.5}"), 10.5));
  }

  @Test
  public void testSelectNestedFieldItself() {
    JSONObject response = new JSONObject(query("SELECT projects FROM %s"));

    verifySchema(response, schema("projects", null, "nested"));

    // Expect nested field itself is returned in a single cell
    verifyDataRows(
        response,
        rows(
            new JSONArray(
                "[\n"
                    + "  {\"name\": \"AWS Redshift Spectrum querying\"},\n"
                    + "  {\"name\": \"AWS Redshift security\"},\n"
                    + "  {\"name\": \"AWS Aurora security\"}\n"
                    + "]")));
  }

  @Test
  public void testSelectObjectFieldOfArrayValuesItself() {
    JSONObject response = new JSONObject(query("SELECT accounts FROM %s"));

    // Only the first element of the list of is returned.
    verifyDataRows(response, rows(new JSONArray("[{\"id\":1},{\"id\":2}]")));
  }

  @Test
  public void testSelectObjectFieldOfArrayValuesInnerFields() {
    JSONObject response = new JSONObject(query("SELECT accounts.id FROM %s"));

    // Only the first element of the list of is returned.
    verifyDataRows(response, rows(1));
  }

  private String query(String sql) {
    return executeQuery(StringUtils.format(sql, TEST_INDEX_DEEP_NESTED), "jdbc");
  }
}
