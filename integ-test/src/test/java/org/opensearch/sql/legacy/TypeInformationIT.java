/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
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

    verifySchema(response, schema("CEIL(balance)", null, "long"));
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
}
