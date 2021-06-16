/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;


import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

public class WindowFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.BANK);
  }

  @Test
  public void testOrderByNullFirst() {
    JSONObject response = new JSONObject(
        executeQuery("SELECT age, ROW_NUMBER() OVER(ORDER BY age DESC NULLS FIRST) "
            + "FROM " + TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES, "jdbc"));

    verifyDataRows(response,
        rows(null, 1),
        rows(36, 2),
        rows(36, 3),
        rows(34, 4),
        rows(33, 5),
        rows(32, 6),
        rows(28, 7));
  }

  @Test
  public void testOrderByNullLast() {
    JSONObject response = new JSONObject(
        executeQuery("SELECT age, ROW_NUMBER() OVER(ORDER BY age NULLS LAST) "
            + "FROM " + TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES, "jdbc"));

    verifyDataRows(response,
        rows(28, 1),
        rows(32, 2),
        rows(33, 3),
        rows(34, 4),
        rows(36, 5),
        rows(36, 6),
        rows(null, 7));
  }

  @Test
  public void testDistinctCountOverNull() {
    JSONObject response = new JSONObject(executeQuery(
        "SELECT lastname, COUNT(DISTINCT gender) OVER() "
            + "FROM " + TestsConstants.TEST_INDEX_BANK, "jdbc"));
    verifyDataRows(response,
        rows("Duke Willmington", 2),
        rows("Bond", 2),
        rows("Bates", 2),
        rows("Adams", 2),
        rows("Ratliff", 2),
        rows("Ayala", 2),
        rows("Mcpherson", 2));
  }

  @Test
  public void testDistinctCountOver() {
    JSONObject response = new JSONObject(executeQuery(
        "SELECT lastname, COUNT(DISTINCT gender) OVER(ORDER BY lastname) "
            + "FROM " + TestsConstants.TEST_INDEX_BANK, "jdbc"));
    verifyDataRowsInOrder(response,
        rows("Adams", 1),
        rows("Ayala", 2),
        rows("Bates", 2),
        rows("Bond", 2),
        rows("Duke Willmington", 2),
        rows("Mcpherson", 2),
        rows("Ratliff", 2));
  }

  @Test
  public void testDistinctCountPartition() {
    JSONObject response = new JSONObject(executeQuery(
        "SELECT lastname, COUNT(DISTINCT gender) OVER(PARTITION BY gender ORDER BY lastname) "
            + "FROM " + TestsConstants.TEST_INDEX_BANK, "jdbc"));
    verifyDataRowsInOrder(response,
        rows("Ayala", 1),
        rows("Bates", 1),
        rows("Mcpherson", 1),
        rows("Adams", 1),
        rows("Bond", 1),
        rows("Duke Willmington", 1),
        rows("Ratliff", 1));
  }

}
