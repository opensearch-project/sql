/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class JdbcTestIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ONLINE);
    loadIndex(Index.PEOPLE);
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.WEBLOG);
  }

  public void testPercentilesQuery() {
    JSONObject response =
        executeJdbcRequest(
            "SELECT percentiles(age, 25.0, 50.0, 75.0, 99.9) age_percentiles "
                + "FROM opensearch-sql_test_index_people");

    assertThat(response.getJSONArray("datarows").length(), equalTo(1));

    JSONObject percentileRow = (JSONObject) response.query("/datarows/0/0");

    assertEquals(31.5, percentileRow.getDouble("25.0"), 0.6);
    assertEquals(33.5, percentileRow.getDouble("50.0"), 0.6);
    assertEquals(36.5, percentileRow.getDouble("75.0"), 0.6);
    assertEquals(39.0, percentileRow.getDouble("99.9"), 0.6);
  }

  // https://github.com/opensearch-project/sql/issues/537
  @Test
  public void testSlowQuery() throws IOException {
    // set slow log threshold = 0s
    updateClusterSettings(new ClusterSetting(PERSISTENT, "plugins.sql.slowlog", "0"));

    JSONObject response =
        executeJdbcRequest(
            "SELECT percentiles(age, 25.0, 50.0, 75.0, 99.9) age_percentiles "
                + "FROM opensearch-sql_test_index_people");

    assertThat(response.getJSONArray("datarows").length(), equalTo(1));
    JSONObject percentileRow = (JSONObject) response.query("/datarows/0/0");
    assertEquals(31.5, percentileRow.getDouble("25.0"), 0.6);
    assertEquals(33.5, percentileRow.getDouble("50.0"), 0.6);
    assertEquals(36.5, percentileRow.getDouble("75.0"), 0.6);
    assertEquals(39.0, percentileRow.getDouble("99.9"), 0.6);

    wipeAllClusterSettings();
  }

  @Ignore(
      "flaky test, trigger resource not enough exception. "
          + "ORDER BY date_format(insert_time, 'dd-MM-YYYY') can't be pushed down ")
  public void testDateTimeInQuery() {
    JSONObject response =
        executeJdbcRequest(
            "SELECT date_format(insert_time, 'dd-MM-YYYY') "
                + "FROM opensearch-sql_test_index_online "
                + "ORDER BY date_format(insert_time, 'dd-MM-YYYY') "
                + "LIMIT 1");

    assertThat(
        response.getJSONArray("datarows").getJSONArray(0).getString(0), equalTo("17-08-2014"));
  }

  @Ignore(
      "flaky test, trigger resource not enough exception. "
          + "ORDER BY all_client/10 can't be pushed down ")
  public void testDivisionInQuery() {
    JSONObject response =
        executeJdbcRequest(
            "SELECT all_client/10 from opensearch-sql_test_index_online ORDER BY all_client/10 desc"
                + " limit 1");

    assertThat(response.getJSONArray("datarows").getJSONArray(0).getDouble(0), equalTo(16827.0));
  }

  public void testGroupByInQuery() {
    JSONObject response =
        executeJdbcRequest(
            "SELECT date_format(insert_time, 'YYYY-MM-dd'), COUNT(*) "
                + "FROM opensearch-sql_test_index_online "
                + "GROUP BY date_format(insert_time, 'YYYY-MM-dd')");

    assertThat(response.getJSONArray("schema").length(), equalTo(2));
    assertThat(response.getJSONArray("datarows").length(), equalTo(8));
  }

  @Test
  public void numberOperatorNameCaseInsensitiveTest() {
    assertSchemaContains(
        executeQuery(
            "SELECT ABS(age) FROM opensearch-sql_test_index_account "
                + "WHERE age IS NOT NULL ORDER BY age LIMIT 5",
            "jdbc"),
        "ABS(age)");
  }

  @Test
  public void trigFunctionNameCaseInsensitiveTest() {
    assertSchemaContains(
        executeQuery(
            "SELECT Cos(age) FROM opensearch-sql_test_index_account "
                + "WHERE age is NOT NULL ORDER BY age LIMIT 5",
            "jdbc"),
        "Cos(age)");
  }

  @Test
  public void stringOperatorNameCaseInsensitiveTest() {
    assertSchemaContains(
        executeQuery(
            "SELECT SubStrinG(lastname, 0, 2) FROM opensearch-sql_test_index_account "
                + "ORDER BY age LIMIT 5",
            "jdbc"),
        "SubStrinG(lastname, 0, 2)");
  }

  @Ignore("DATE_FORMAT function signature changed in new engine")
  @Test
  public void dateFunctionNameCaseInsensitiveTest() {
    assertTrue(
        executeQuery(
                "SELECT DATE_FORMAT(insert_time, 'yyyy-MM-dd', 'UTC') FROM"
                    + " opensearch-sql_test_index_online WHERE date_FORMAT(insert_time,"
                    + " 'yyyy-MM-dd', 'UTC') > '2014-01-01' GROUP BY DAte_format(insert_time,"
                    + " 'yyyy-MM-dd', 'UTC') ORDER BY date_forMAT(insert_time, 'yyyy-MM-dd',"
                    + " 'UTC')",
                "jdbc")
            .equalsIgnoreCase(
                executeQuery(
                    "SELECT date_format(insert_time, 'yyyy-MM-dd', 'UTC') FROM"
                        + " opensearch-sql_test_index_online WHERE date_format(insert_time,"
                        + " 'yyyy-MM-dd', 'UTC') > '2014-01-01' GROUP BY date_format(insert_time,"
                        + " 'yyyy-MM-dd', 'UTC') ORDER BY date_format(insert_time, 'yyyy-MM-dd',"
                        + " 'UTC')",
                    "jdbc")));
  }

  @Test
  public void ipTypeShouldPassJdbcFormatter() {
    assertThat(
        executeQuery(
            "SELECT host FROM " + TestsConstants.TEST_INDEX_WEBLOG + " ORDER BY host", "jdbc"),
        containsString("\"type\": \"ip\""));
  }

  @Test
  public void functionWithoutAliasShouldHaveEntireFunctionAsNameInSchema() {
    assertThat(
        executeQuery(
            "SELECT substring(lastname, 1, 2) FROM "
                + TestsConstants.TEST_INDEX_ACCOUNT
                + " ORDER BY substring(lastname, 1, 2)",
            "jdbc"),
        containsString("\"name\": \"substring(lastname, 1, 2)\""));
  }

  @Ignore("Handled by v2 engine which returns 'name': 'substring(lastname, 1, 2)' instead")
  @Test
  public void functionWithAliasShouldHaveAliasAsNameInSchema() {
    assertThat(
        executeQuery(
            "SELECT substring(lastname, 1, 2) AS substring FROM "
                + TestsConstants.TEST_INDEX_ACCOUNT
                + " ORDER BY substring",
            "jdbc"),
        containsString("\"name\": \"substring\""));
  }

  private void assertSchemaContains(String actualResponse, String expected) {
    JSONArray schema = new JSONObject(actualResponse).optJSONArray("schema");
    for (Object nameTypePair : schema) {
      String actual = ((JSONObject) nameTypePair).getString("name");
      if (expected.equals(actual)) {
        return;
      }
    }
    Assert.fail(
        "Expected field name ["
            + expected
            + "] is not found in response schema: "
            + actualResponse);
  }
}
