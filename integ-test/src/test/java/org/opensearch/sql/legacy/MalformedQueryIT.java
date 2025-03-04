/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import java.io.IOException;
import java.util.Locale;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.json.JSONObject;
import org.junit.Assert;
import org.opensearch.client.ResponseException;

/** Tests for clean handling of various types of invalid queries */
public class MalformedQueryIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_TWO);
  }

  public void testJoinWithInvalidCondition() throws IOException, ParseException {
    ResponseException result =
        assertThrows(
            "Expected Join query with malformed 'ON' to raise error, but didn't",
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        Locale.ROOT,
                        "SELECT a.firstname, b.age FROM %s AS a INNER JOIN %s AS b %%"
                            + " a.account_number=b.account_number",
                        TestsConstants.TEST_INDEX_BANK,
                        TestsConstants.TEST_INDEX_BANK_TWO)));
    var errMsg = new JSONObject(EntityUtils.toString(result.getResponse().getEntity()));

    Assert.assertEquals("SqlParseException", errMsg.getJSONObject("error").getString("type"));
    Assert.assertEquals(400, errMsg.getInt("status"));
  }

  public void testWrappedWildcardInSubquery() throws IOException, ParseException {
    ResponseException result =
        assertThrows(
            "Expected wildcard subquery to raise error, but didn't",
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        Locale.ROOT,
                        "SELECT a.first_name FROM %s AS a WHERE a.age IN (SELECT age FROM"
                            + " `opensearch-sql_test_index_*` WHERE age > 30)",
                        TestsConstants.TEST_INDEX_BANK,
                        TestsConstants.TEST_INDEX_BANK_TWO)));
    var errMsg = new JSONObject(EntityUtils.toString(result.getResponse().getEntity()));
    System.err.println("Full response: " + errMsg);

    Assert.assertEquals("IndexNotFoundException", errMsg.getJSONObject("error").getString("type"));
    Assert.assertEquals(404, errMsg.getInt("status"));
  }

  public void testUnwrappedWildcardInSubquery() throws IOException, ParseException {
    ResponseException result =
        assertThrows(
            "Expected wildcard subquery to raise error, but didn't",
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        Locale.ROOT,
                        "SELECT a.first_name FROM %s AS a WHERE a.age IN (SELECT age FROM * WHERE"
                            + " age > 30)",
                        TestsConstants.TEST_INDEX_BANK,
                        TestsConstants.TEST_INDEX_BANK_TWO)));
    var errMsg = new JSONObject(EntityUtils.toString(result.getResponse().getEntity()));
    System.err.println("Full response: " + errMsg);

    Assert.assertEquals("IndexNotFoundException", errMsg.getJSONObject("error").getString("type"));
    Assert.assertEquals(404, errMsg.getInt("status"));
  }
}
