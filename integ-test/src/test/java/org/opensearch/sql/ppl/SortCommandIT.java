/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class SortCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.DOG);
  }

  @Test
  public void testSortCommand() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | sort age | fields age", TEST_INDEX_BANK));
    verifyOrder(result, rows(28), rows(32), rows(33), rows(34), rows(36), rows(36), rows(39));
  }

  @Test
  public void testSortWithNullValue() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort balance | fields firstname, balance",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyOrder(
        result,
        rows("Hattie", null),
        rows("Elinor", null),
        rows("Virginia", null),
        rows("Dale", 4180),
        rows("Nanette", 32838),
        rows("Amber JOHnny", 39225),
        rows("Dillard", 48086));
  }

  @Test
  public void testSortStringField() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | sort lastname | fields lastname", TEST_INDEX_BANK));
    verifyOrder(
        result,
        rows("Adams"),
        rows("Ayala"),
        rows("Bates"),
        rows("Bond"),
        rows("Duke Willmington"),
        rows("Mcpherson"),
        rows("Ratliff"));
  }

  @Test
  public void testSortMultipleFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | sort dog_name, age | fields dog_name, age", TEST_INDEX_DOG));
    verifyOrder(result, rows("rex", 2), rows("snoopy", 4));
  }
}
