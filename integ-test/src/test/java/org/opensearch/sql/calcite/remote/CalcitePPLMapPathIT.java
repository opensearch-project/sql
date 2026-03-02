/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_SPATH;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for MAP path materialization with symbol-based commands (Category A). Uses
 * {@code spath} to create MAP columns from JSON strings, then exercises each command on dotted
 * paths like {@code doc.user.name}.
 *
 * <p>Test data: 5 docs (4 data + 1 null) with nested user object (name, age, city). Designed to
 * also support future Category B (top/rare) and Category C (join/streamstats/lookup) tests.
 *
 * @see org.opensearch.sql.calcite.MapPathMaterializer
 */
public class CalcitePPLMapPathIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.SPATH);
  }

  @Test
  public void testMapPathWithRename() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=doc"
                    + " | rename doc.user.name as username"
                    + " | fields username, doc.user.age",
                TEST_INDEX_SPATH));
    verifySchema(result, schema("username", "string"), schema("doc.user.age", "string"));
    verifyDataRows(
        result,
        rows("John", "30"),
        rows("Alice", "25"),
        rows("John", "35"),
        rows("Bob", "40"),
        rows((Object) null, (Object) null));
  }

  @Test
  public void testMapPathWithFillnull() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=doc"
                    + " | fillnull using doc.user.name = 'N/A'"
                    + " | fields doc.user.name",
                TEST_INDEX_SPATH));
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRows(result, rows("John"), rows("Alice"), rows("John"), rows("Bob"), rows("N/A"));
  }

  @Test
  public void testMapPathWithReplace() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=doc"
                    + " | replace 'John' WITH 'Jonathan' IN doc.user.name"
                    + " | fields doc.user.name",
                TEST_INDEX_SPATH));
    verifySchema(result, schema("doc.user.name", "string"));
    verifyDataRows(
        result,
        rows("Jonathan"),
        rows("Alice"),
        rows("Jonathan"),
        rows("Bob"),
        rows((Object) null));
  }

  @Test
  public void testMapPathWithFieldsExclusion() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=doc"
                    + " | fields - doc.user.name"
                    + " | fields doc.user.age, doc.user.city",
                TEST_INDEX_SPATH));
    verifySchema(result, schema("doc.user.age", "string"), schema("doc.user.city", "string"));
    verifyDataRows(
        result,
        rows("30", "NYC"),
        rows("25", "LA"),
        rows("35", "SF"),
        rows("40", "NYC"),
        rows((Object) null, (Object) null));
  }

  @Test
  public void testMapPathWithAddtotals() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=doc"
                    + " | eval age_num = cast(doc.user.age as double)"
                    + " | addtotals col=true row=false age_num"
                    + " | fields doc.user.name, age_num",
                TEST_INDEX_SPATH));
    verifySchema(result, schema("doc.user.name", "string"), schema("age_num", "double"));
    verifyDataRows(
        result,
        rows("John", 30.0),
        rows("Alice", 25.0),
        rows("John", 35.0),
        rows("Bob", 40.0),
        rows(null, null),
        rows(null, 130.0));
  }

  @Test
  public void testMapPathWithMvcombine() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | spath input=doc"
                    + " | mvcombine doc.user.name"
                    + " | fields doc.user.name, doc.user.city",
                TEST_INDEX_SPATH));
    verifySchema(result, schema("doc.user.name", "array"), schema("doc.user.city", "string"));
    verifyDataRows(
        result,
        rows(new org.json.JSONArray("[\"John\"]"), "NYC"),
        rows(new org.json.JSONArray("[\"Alice\"]"), "LA"),
        rows(new org.json.JSONArray("[\"John\"]"), "SF"),
        rows(new org.json.JSONArray("[\"Bob\"]"), "NYC"),
        rows((Object) null, (Object) null));
  }
}
