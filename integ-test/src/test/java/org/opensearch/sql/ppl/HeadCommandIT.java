/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;

public class HeadCommandIT extends PPLIntegTestCase {

  @Before
  public void beforeTest() throws IOException {
    setQuerySizeLimit(200);
  }

  @After
  public void afterTest() throws IOException {
    resetQuerySizeLimit();
  }

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testHead() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields firstname, age | head", TEST_INDEX_ACCOUNT));
    verifyDataRows(result,
        rows("Amber", 32),
        rows("Hattie", 36),
        rows("Nanette", 28),
        rows("Dale", 33),
        rows("Elinor", 36),
        rows("Virginia", 39),
        rows("Dillard", 34),
        rows("Mcgee", 39),
        rows("Aurelia", 37),
        rows("Fulton", 23));
  }

  @Test
  public void testHeadWithNumber() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields firstname, age | head 3", TEST_INDEX_ACCOUNT));
    verifyDataRows(result,
        rows("Amber", 32),
        rows("Hattie", 36),
        rows("Nanette", 28));
  }

  @Test
  public void testHeadWithNumberAndFrom() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields firstname, age | head 3 from 4", TEST_INDEX_ACCOUNT));
    verifyDataRows(result,
        rows("Elinor", 36),
        rows("Virginia", 39),
        rows("Dillard", 34));
  }
}
