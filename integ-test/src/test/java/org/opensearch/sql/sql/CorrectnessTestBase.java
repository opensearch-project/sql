/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static java.util.Collections.emptyMap;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.opensearch.sql.correctness.TestConfig;
import org.opensearch.sql.correctness.report.TestReport;
import org.opensearch.sql.correctness.report.TestSummary;
import org.opensearch.sql.correctness.runner.ComparisonTest;
import org.opensearch.sql.correctness.runner.connection.DBConnection;
import org.opensearch.sql.correctness.runner.connection.JDBCConnection;
import org.opensearch.sql.correctness.runner.connection.OpenSearchConnection;
import org.opensearch.sql.correctness.testset.TestDataSet;
import org.opensearch.sql.correctness.testset.TestQuerySet;
import org.opensearch.sql.legacy.RestIntegTestCase;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * SQL integration test base class. This is very similar to CorrectnessIT though enforce the success
 * of all tests rather than report failures only.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class CorrectnessTestBase extends RestIntegTestCase {

  /** Comparison test runner shared by all methods in this IT class. */
  private static ComparisonTest runner;

  @Override
  protected void init() throws Exception {
    if (runner != null) {
      return;
    }

    TestConfig config = new TestConfig(emptyMap());
    runner = new ComparisonTest(getOpenSearchConnection(), getOtherDBConnections(config));

    runner.connect();
    for (TestDataSet dataSet : config.getTestDataSets()) {
      runner.loadData(dataSet);
    }
  }

  /** Clean up test data and close other database connection. */
  @AfterClass
  public static void cleanUp() {
    if (runner == null) {
      return;
    }

    try {
      TestConfig config = new TestConfig(emptyMap());
      for (TestDataSet dataSet : config.getTestDataSets()) {
        runner.cleanUp(dataSet);
      }

      runner.close();
    } finally {
      runner = null;
    }
  }

  /**
   * Execute the given queries and compare result with other database. The queries will be
   * considered as one test batch.
   */
  protected void verify(String... queries) {
    TestReport result = runner.verify(new TestQuerySet(queries));
    TestSummary summary = result.getSummary();
    Assert.assertEquals(
        StringUtils.format(
            "Comparison test failed on queries: %s", new JSONObject(result).toString(2)),
        0,
        summary.getFailure());
  }

  /** Use OpenSearch cluster initialized by OpenSearch Gradle task. */
  private DBConnection getOpenSearchConnection() {
    String openSearchHost = client().getNodes().get(0).getHost().toString();
    return new OpenSearchConnection("jdbc:opensearch://" + openSearchHost, client());
  }

  /** Create database connection with database name and connect URL. */
  private DBConnection[] getOtherDBConnections(TestConfig config) {
    return config.getOtherDbConnectionNameAndUrls().entrySet().stream()
        .map(e -> new JDBCConnection(e.getKey(), e.getValue()))
        .toArray(DBConnection[]::new);
  }
}
