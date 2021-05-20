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

package org.opensearch.sql.legacy;

import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;

import org.junit.Ignore;
import org.junit.Test;

public class TypeInformationIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.ONLINE);
  }

  /*
  numberOperators
   */
  @Test
  public void testAbsWithIntFieldReturnsInt() {
    JSONObject response =
        executeJdbcRequest("SELECT ABS(age) FROM " + TestsConstants.TEST_INDEX_ACCOUNT +
            " ORDER BY age LIMIT 5");

    verifySchema(response, schema("ABS(age)", null, "long"));
  }

  @Test
  public void testCeilWithLongFieldReturnsLong() {
    JSONObject response =
        executeJdbcRequest("SELECT CEIL(balance) FROM " + TestsConstants.TEST_INDEX_ACCOUNT +
            " ORDER BY balance LIMIT 5");

    verifySchema(response, schema("CEIL(balance)", null, "integer"));
  }

  /*
  mathConstants
   */
  @Test
  public void testPiReturnsDouble() {
    JSONObject response = executeJdbcRequest("SELECT PI() FROM " + TestsConstants.TEST_INDEX_ACCOUNT
        + " LIMIT 1");

    verifySchema(response, schema("PI()", null, "double"));
  }

  /*
  stringOperators
   */
  @Test
  public void testUpperWithStringFieldReturnsString() {
    JSONObject response = executeJdbcRequest("SELECT UPPER(firstname) AS firstname_alias FROM " +
        TestsConstants.TEST_INDEX_ACCOUNT + " ORDER BY firstname_alias LIMIT 2");

    verifySchema(response, schema("UPPER(firstname)", "firstname_alias", "keyword"));
  }

  @Test
  public void testLowerWithTextFieldReturnsText() {
    JSONObject response = executeJdbcRequest("SELECT LOWER(firstname) FROM " +
        TestsConstants.TEST_INDEX_ACCOUNT + " ORDER BY firstname LIMIT 2");

    verifySchema(response, schema("LOWER(firstname)", null, "keyword"));
  }

  /*
  stringFunctions
   */
  @Test
  public void testLengthWithTextFieldReturnsInt() {
    JSONObject response = executeJdbcRequest("SELECT length(firstname) FROM " +
        TestsConstants.TEST_INDEX_ACCOUNT + " ORDER BY firstname LIMIT 2");

    verifySchema(response, schema("length(firstname)", null, "integer"));
  }

  @Test
  public void testLengthWithGroupByExpr() {
    JSONObject response =
        executeJdbcRequest("SELECT Length(firstname) FROM " + TestsConstants.TEST_INDEX_ACCOUNT +
            " GROUP BY LENGTH(firstname) LIMIT 5");

    verifySchema(response, schema("Length(firstname)", null, "integer"));
  }

  /*
  trigFunctions
   */
  @Test
  public void testSinWithLongFieldReturnsDouble() {
    JSONObject response = executeJdbcRequest("SELECT sin(balance) FROM " +
        TestsConstants.TEST_INDEX_ACCOUNT + " ORDER BY firstname LIMIT 2");

    verifySchema(response, schema("sin(balance)", null, "double"));
  }

  @Test
  public void testRadiansWithLongFieldReturnsDouble() {
    JSONObject response = executeJdbcRequest("SELECT radians(balance) FROM " +
        TestsConstants.TEST_INDEX_ACCOUNT + " ORDER BY firstname LIMIT 2");

    verifySchema(response, schema("radians(balance)", null, "double"));
  }

  /*
  binaryOperators
   */
  @Test
  public void testAddWithIntReturnsInt() {
    JSONObject response = executeJdbcRequest("SELECT (balance + 5) AS balance_add_five FROM " +
        TestsConstants.TEST_INDEX_ACCOUNT + " ORDER BY firstname LIMIT 2");

    verifySchema(response, schema("(balance + 5)", "balance_add_five", "long"));
  }

  @Test
  public void testSubtractLongWithLongReturnsLong() {
    JSONObject response = executeJdbcRequest("SELECT (balance - balance) FROM " +
        TestsConstants.TEST_INDEX_ACCOUNT + " ORDER BY firstname LIMIT 2");

    verifySchema(response, schema("(balance - balance)", null, "long"));
  }

  /*
  dateFunctions
   */
  @Test
  public void testDayOfWeekWithKeywordReturnsText() {
    JSONObject response = executeJdbcRequest("SELECT DAYOFWEEK(insert_time) FROM "
        + TestsConstants.TEST_INDEX_ONLINE + " LIMIT 2");

    verifySchema(response,
        schema("DAYOFWEEK(insert_time)", null, "integer"));
  }

  @Test
  public void testYearWithKeywordReturnsText() {
    JSONObject response = executeJdbcRequest("SELECT YEAR(insert_time) FROM "
        + TestsConstants.TEST_INDEX_ONLINE + " LIMIT 2");

    verifySchema(response, schema("YEAR(insert_time)", null, "integer"));
  }

  private JSONObject executeJdbcRequest(String query) {
    return new JSONObject(executeQuery(query, "jdbc"));
  }

}
