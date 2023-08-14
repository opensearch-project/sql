/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness;

import static org.opensearch.sql.util.TestUtils.getResourceFilePath;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.google.common.collect.Maps;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import org.apache.hc.core5.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.RestClient;
import org.opensearch.sql.correctness.report.TestReport;
import org.opensearch.sql.correctness.runner.ComparisonTest;
import org.opensearch.sql.correctness.runner.connection.DBConnection;
import org.opensearch.sql.correctness.runner.connection.JDBCConnection;
import org.opensearch.sql.correctness.runner.connection.OpenSearchConnection;
import org.opensearch.sql.correctness.testset.TestDataSet;
import org.opensearch.test.OpenSearchIntegTestCase;

/** Correctness integration test by performing comparison test with other databases. */
@OpenSearchIntegTestCase.SuiteScopeTestCase
@OpenSearchIntegTestCase.ClusterScope(
    scope = OpenSearchIntegTestCase.Scope.SUITE,
    numDataNodes = 3,
    supportsDedicatedMasters = false)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class CorrectnessIT extends OpenSearchIntegTestCase {

  private static final Logger LOG = LogManager.getLogger();

  @Test
  public void performComparisonTest() throws URISyntaxException {
    TestConfig config = new TestConfig(getCmdLineArgs());
    LOG.info("Starting comparison test {}", config);

    try (ComparisonTest test =
        new ComparisonTest(getThisDBConnection(config), getOtherDBConnections(config))) {
      LOG.info("Loading test data set...");
      test.connect();
      for (TestDataSet dataSet : config.getTestDataSets()) {
        test.loadData(dataSet);
      }

      LOG.info("Verifying test queries...");
      TestReport report = test.verify(config.getTestQuerySet());

      LOG.info("Saving test report to disk...");
      store(report);

      LOG.info("Cleaning up test data...");
      for (TestDataSet dataSet : config.getTestDataSets()) {
        test.cleanUp(dataSet);
      }
    }
    LOG.info("Completed comparison test.");
  }

  private Map<String, String> getCmdLineArgs() {
    return Maps.fromProperties(System.getProperties());
  }

  private DBConnection getThisDBConnection(TestConfig config) throws URISyntaxException {
    String dbUrl = config.getDbConnectionUrl();
    if (dbUrl.isEmpty()) {
      return getOpenSearchConnection(config);
    }
    return new JDBCConnection("DB Tested", dbUrl);
  }

  /** Use OpenSearch cluster given on CLI arg or internal embedded in SQLIntegTestCase */
  private DBConnection getOpenSearchConnection(TestConfig config) throws URISyntaxException {
    RestClient client;
    String openSearchHost = config.getOpenSearchHostUrl();
    if (openSearchHost.isEmpty()) {
      client = getRestClient();
      openSearchHost = client.getNodes().get(0).getHost().toString();
    } else {
      client = RestClient.builder(HttpHost.create(openSearchHost)).build();
    }
    return new OpenSearchConnection("jdbc:opensearch://" + openSearchHost, client);
  }

  /** Create database connection with database name and connect URL */
  private DBConnection[] getOtherDBConnections(TestConfig config) {
    return config.getOtherDbConnectionNameAndUrls().entrySet().stream()
        .map(e -> new JDBCConnection(e.getKey(), e.getValue()))
        .toArray(DBConnection[]::new);
  }

  private void store(TestReport report) {
    try {
      // Create reports folder if not exists
      String folderPath = "reports/";
      Path path = Paths.get(getResourceFilePath(folderPath));
      if (Files.notExists(path)) {
        Files.createDirectory(path);
      }

      // Write to report file
      String relFilePath = folderPath + reportFileName();
      String absFilePath = getResourceFilePath(relFilePath);
      byte[] content = new JSONObject(report).toString(2).getBytes();

      LOG.info("Report file location is {}", absFilePath);
      Files.write(Paths.get(absFilePath), content);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to store report file", e);
    }
  }

  private String reportFileName() {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH");
    df.setTimeZone(TimeZone.getTimeZone("GMT"));
    String dateTime = df.format(new Date());
    return "report_" + dateTime + ".json";
  }
}
