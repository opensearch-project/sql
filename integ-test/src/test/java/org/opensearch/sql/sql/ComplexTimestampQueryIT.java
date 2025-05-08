/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_TIME;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_TIME_NESTED;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class ComplexTimestampQueryIT extends SQLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(SQLIntegTestCase.Index.DATETIME);
    loadIndex(SQLIntegTestCase.Index.DATETIME_NESTED);
  }

  /** See: <a href="https://github.com/opensearch-project/sql/issues/3159">3159</a> */
  @Test
  public void joinWithTimestampFieldsSchema() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT one.login_time, two.login_time "
                + "FROM %s AS one JOIN %s AS two "
                + "ON one._id = two._id",
            TEST_INDEX_DATE_TIME,
            TEST_INDEX_DATE_TIME);

    JSONObject result = executeQuery(query);
    JSONArray schema = result.getJSONArray("schema");

    Assert.assertFalse(schema.isEmpty());
    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = schema.getJSONObject(i);
      Assert.assertEquals("timestamp", column.getString("type"));
    }
  }

  /** Control for joinWithTimestampFieldsSchema */
  @Test
  public void nonJoinTimestampFieldsSchema() throws IOException {
    String query =
        String.format(
            Locale.ROOT, "SELECT one.login_time " + "FROM %s AS one", TEST_INDEX_DATE_TIME);

    JSONObject result = executeQuery(query);
    JSONArray schema = result.getJSONArray("schema");

    Assert.assertFalse(schema.isEmpty());
    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = schema.getJSONObject(i);
      Assert.assertEquals("timestamp", column.getString("type"));
    }
  }

  /** See: <a href="https://github.com/opensearch-project/sql/issues/3204">3204</a> */
  // TODO currently out of scope due to V1/V2 engine feature mismatch. Should be fixed with Calcite.
  @Test
  @Ignore
  public void joinTimestampComparison() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT one.login_time, two.login_time "
                + "FROM %s AS one JOIN %s AS two "
                + "ON one._id = two._id "
                + "WHERE one.login_time > timestamp('2018-05-07 00:00:00')",
            TEST_INDEX_DATE_TIME,
            TEST_INDEX_DATE_TIME);

    JSONObject result = executeQuery(query);
    Assert.assertEquals(2, result.getJSONArray("datarows").length());
  }

  /** Control for joinTimestampComparison */
  @Test
  public void nonJoinTimestampComparison() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT login_time "
                + "FROM %s "
                + "WHERE login_time > timestamp('2018-05-07 00:00:00')",
            TEST_INDEX_DATE_TIME);

    JSONObject result = executeQuery(query);
    System.err.println(result.getJSONArray("datarows").toString());
    Assert.assertEquals(2, result.getJSONArray("datarows").length());
  }

  /** See: <a href="https://github.com/opensearch-project/sql/issues/1545">1545</a> */
  @Test
  public void selectDatetimeWithNested() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT tab.login_time " + "FROM %s AS tab, tab.projects AS pro",
            TEST_INDEX_DATE_TIME_NESTED);

    JSONObject result = executeQuery(query);
    JSONArray schema = result.getJSONArray("schema");

    Assert.assertFalse(schema.isEmpty());
    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = schema.getJSONObject(i);
      Assert.assertEquals("timestamp", column.getString("type"));
    }
  }

  /** Control for selectDatetimeWithNested */
  @Test
  public void selectDatetimeWithoutNested() throws IOException {
    String query =
        String.format(
            Locale.ROOT, "SELECT tab.login_time " + "FROM %s AS tab", TEST_INDEX_DATE_TIME_NESTED);

    JSONObject result = executeQuery(query);
    JSONArray schema = result.getJSONArray("schema");

    Assert.assertFalse(schema.isEmpty());
    for (int i = 0; i < schema.length(); i++) {
      JSONObject column = schema.getJSONObject(i);
      Assert.assertEquals("timestamp", column.getString("type"));
    }
  }
}
