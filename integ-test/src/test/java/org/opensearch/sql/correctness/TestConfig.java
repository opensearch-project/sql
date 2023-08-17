/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness;

import static java.util.stream.Collectors.joining;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.sql.correctness.testset.TestDataSet;
import org.opensearch.sql.correctness.testset.TestQuerySet;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 *
 *
 * <pre>
 * Test configuration parse the following information from command line arguments:
 * 1) Test schema and data
 * 2) Test queries
 * 3) OpenSearch connection URL
 * 4) Other database connection URLs
 * </pre>
 */
public class TestConfig {

  private static final String DEFAULT_TEST_QUERIES = "tableau_integration_tests.txt";
  private static final String DEFAULT_OTHER_DB_URLS =
      "H2=jdbc:h2:mem:test;DB_CLOSE_DELAY=-1," + "SQLite=jdbc:sqlite::memory:";

  private final TestDataSet[] testDataSets;

  private final TestQuerySet testQuerySet;

  private final String openSearchHostUrl;

  /** Test against some database rather than OpenSearch via our JDBC driver */
  private final String dbConnectionUrl;

  private final Map<String, String> otherDbConnectionNameAndUrls = new HashMap<>();

  public TestConfig(Map<String, String> cliArgs) {
    testDataSets = buildDefaultTestDataSet(); // TODO: parse test data set argument
    testQuerySet = buildTestQuerySet(cliArgs);
    openSearchHostUrl = cliArgs.getOrDefault("esHost", "");
    dbConnectionUrl = cliArgs.getOrDefault("dbUrl", "");

    parseOtherDbConnectionInfo(cliArgs);
  }

  public TestDataSet[] getTestDataSets() {
    return testDataSets;
  }

  public TestQuerySet getTestQuerySet() {
    return testQuerySet;
  }

  public String getOpenSearchHostUrl() {
    return openSearchHostUrl;
  }

  public String getDbConnectionUrl() {
    return dbConnectionUrl;
  }

  public Map<String, String> getOtherDbConnectionNameAndUrls() {
    return otherDbConnectionNameAndUrls;
  }

  private TestDataSet[] buildDefaultTestDataSet() {
    return new TestDataSet[] {
      new TestDataSet(
          "opensearch_dashboards_sample_data_flights",
          readFile("opensearch_dashboards_sample_data_flights.json"),
          readFile("opensearch_dashboards_sample_data_flights.csv")),
      new TestDataSet(
          "opensearch_dashboards_sample_data_ecommerce",
          readFile("opensearch_dashboards_sample_data_ecommerce.json"),
          readFile("opensearch_dashboards_sample_data_ecommerce.csv")),
    };
  }

  private TestQuerySet buildTestQuerySet(Map<String, String> cliArgs) {
    String queryFilePath = cliArgs.getOrDefault("queries", ""); // Gradle set it empty always
    if (queryFilePath.isEmpty()) {
      queryFilePath = DEFAULT_TEST_QUERIES;
    }
    return new TestQuerySet(readFile(queryFilePath));
  }

  private void parseOtherDbConnectionInfo(Map<String, String> cliArgs) {
    String otherDbUrls = cliArgs.getOrDefault("otherDbUrls", "");
    if (otherDbUrls.isEmpty()) {
      otherDbUrls = DEFAULT_OTHER_DB_URLS;
    }

    for (String dbNameAndUrl : otherDbUrls.split(",")) {
      int firstEq = dbNameAndUrl.indexOf('=');
      String dbName = dbNameAndUrl.substring(0, firstEq);
      String dbUrl = dbNameAndUrl.substring(firstEq + 1);
      otherDbConnectionNameAndUrls.put(dbName, dbUrl);
    }
  }

  private static String readFile(String relativePath) {
    try {
      URL url = Resources.getResource("correctness/" + relativePath);
      return Resources.toString(url, Charsets.UTF_8);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to read test file [" + relativePath + "]");
    }
  }

  @Override
  public String toString() {
    return "\n=================================\n"
        + "Tested Database  : "
        + openSearchHostUrlToString()
        + "\nOther Databases  :\n"
        + otherDbConnectionInfoToString()
        + "\nTest data set(s) :\n"
        + testDataSetsToString()
        + "\nTest query set   : "
        + testQuerySet
        + "\n=================================\n";
  }

  private String testDataSetsToString() {
    return Arrays.stream(testDataSets).map(TestDataSet::toString).collect(joining("\n"));
  }

  private String openSearchHostUrlToString() {
    if (!dbConnectionUrl.isEmpty()) {
      return dbConnectionUrl;
    }
    return openSearchHostUrl.isEmpty()
        ? "(Use internal OpenSearch in workspace)"
        : openSearchHostUrl;
  }

  private String otherDbConnectionInfoToString() {
    return otherDbConnectionNameAndUrls.entrySet().stream()
        .map(e -> StringUtils.format(" %s = %s", e.getKey(), e.getValue()))
        .collect(joining("\n"));
  }
}
