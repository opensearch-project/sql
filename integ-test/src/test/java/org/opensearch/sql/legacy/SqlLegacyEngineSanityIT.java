/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PEOPLE;
import static org.opensearch.sql.util.Capability.TEXT_FIELD_EXACT_MATCH;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.util.RequiresCapability;

/**
 * Sanity tests for the legacy SQL engine. Many legacy integration tests (JoinIT, SubqueryIT,
 * MultiQueryIT, etc.) are @Ignored because they assert on the deprecated JSON response format
 * removed in 3.0. These tests provide minimal coverage that the legacy engine still executes
 * correctly for queries that fall back from the V2 engine.
 */
public class SqlLegacyEngineSanityIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.DOG);
    loadIndex(Index.PEOPLE);
  }

  @Test
  public void testInnerJoinFallback() throws IOException {
    JSONObject result =
        executeQuery(
            "SELECT a.firstname, d.dog_name FROM %s a JOIN %s d ON d.holdersName = a.firstname WHERE a.age > 35"
                .formatted(TEST_INDEX_PEOPLE, TEST_INDEX_DOG));
    verifyDataRows(result, rows("Hattie", "snoopy"));
  }

  @Test
  @RequiresCapability(TEXT_FIELD_EXACT_MATCH)
  public void testLeftJoinFallback() throws IOException {
    JSONObject result =
        executeQuery(
            "SELECT a.firstname, d.dog_name FROM %s a LEFT JOIN %s d ON d.holdersName = a.firstname WHERE a.firstname = 'Daenerys'"
                .formatted(TEST_INDEX_PEOPLE, TEST_INDEX_DOG));
    verifyDataRows(result, rows("Daenerys", "rex"));
  }

  @Test
  public void testInSubqueryFallback() throws IOException {
    JSONObject result =
        executeQuery(
            "SELECT a.firstname FROM %s a WHERE a.firstname IN (SELECT holdersName FROM %s)"
                .formatted(TEST_INDEX_PEOPLE, TEST_INDEX_DOG));
    verifyDataRows(result, rows("Daenerys"), rows("Hattie"));
  }
}
