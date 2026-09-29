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
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

public class HeadCommandIT extends PPLIntegTestCase {

  @Before
  public void beforeTest() throws IOException {
    setQuerySizeLimit(200);
  }

  @After
  public void afterTest() throws IOException {
    resetQuerySizeLimit();
    resetMaxResultWindow(TEST_INDEX_ACCOUNT);
  }

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testHead() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number | fields firstname, age | head",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(
        result,
        rows("Bradshaw", 29),
        rows("Amber", 32),
        rows("Roberta", 22),
        rows("Levine", 26),
        rows("Rodriquez", 31),
        rows("Leola", 30),
        rows("Hattie", 36),
        rows("Levy", 22),
        rows("Jan", 35),
        rows("Opal", 39));
  }

  @Test
  public void testHeadWithNumber() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number | fields firstname, age | head 3",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Bradshaw", 29), rows("Amber", 32), rows("Roberta", 22));
  }

  @Ignore("Fix https://github.com/opensearch-project/sql/issues/703#issuecomment-1211422130")
  @Test
  public void testHeadWithNumberLargerThanQuerySizeLimit() throws IOException {
    setQuerySizeLimit(5);
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname, age | head 10", TEST_INDEX_ACCOUNT));
    verifyDataRows(
        result,
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
  public void testHeadWithNumberLargerThanMaxResultWindow() throws IOException {
    setMaxResultWindow(TEST_INDEX_ACCOUNT, 10);
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number | fields firstname, age | head 15",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(
        result,
        rows("Bradshaw", 29),
        rows("Amber", 32),
        rows("Roberta", 22),
        rows("Levine", 26),
        rows("Rodriquez", 31),
        rows("Leola", 30),
        rows("Hattie", 36),
        rows("Levy", 22),
        rows("Jan", 35),
        rows("Opal", 39),
        rows("Dominique", 37),
        rows("Jenkins", 20),
        rows("Stafford", 20),
        rows("Nanette", 28),
        rows("Erma", 39));
  }

  @Ignore("Fix https://github.com/opensearch-project/sql/issues/703#issuecomment-1211422130")
  @Test
  public void testHeadWithLargeNumber() throws IOException {
    setQuerySizeLimit(5);
    setMaxResultWindow(TEST_INDEX_ACCOUNT, 10);
    JSONObject result =
        executeQuery(
            String.format("source=%s | fields firstname, age | head 15", TEST_INDEX_ACCOUNT));
    verifyDataRows(
        result,
        rows("Amber", 32),
        rows("Hattie", 36),
        rows("Nanette", 28),
        rows("Dale", 33),
        rows("Elinor", 36),
        rows("Virginia", 39),
        rows("Dillard", 34),
        rows("Mcgee", 39),
        rows("Aurelia", 37),
        rows("Fulton", 23),
        rows("Burton", 31),
        rows("Josie", 32),
        rows("Hughes", 30),
        rows("Hall", 25),
        rows("Deidre", 33));
  }

  @Test
  public void testHeadWithNumberAndFrom() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number | fields firstname, age | head 3 from 4",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Rodriquez", 31), rows("Leola", 30), rows("Hattie", 36));
  }
}
