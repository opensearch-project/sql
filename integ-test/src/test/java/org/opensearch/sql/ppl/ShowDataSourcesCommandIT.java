/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;

public class ShowDataSourcesCommandIT extends PPLIntegTestCase {

  /**
   * Integ tests are dependent on self generated metrics in prometheus instance. When running
   * individual integ tests there is no time for generation of metrics in the test prometheus
   * instance. This method gives prometheus time to generate metrics on itself.
   *
   * @throws InterruptedException
   */
  @BeforeClass
  protected static void metricGenerationWait() throws InterruptedException {
    Thread.sleep(10000);
  }

  @Override
  protected void init() throws InterruptedException, IOException {
    DataSourceMetadata createDSM =
        new DataSourceMetadata(
            "my_prometheus",
            DataSourceType.PROMETHEUS,
            ImmutableList.of(),
            ImmutableMap.of("prometheus.uri", "http://localhost:9090"));
    Request createRequest = getCreateDataSourceRequest(createDSM);
    Response response = client().performRequest(createRequest);
    Assert.assertEquals(201, response.getStatusLine().getStatusCode());
  }

  @After
  protected void deleteDataSourceMetadata() throws IOException {
    Request deleteRequest = getDeleteDataSourceRequest("my_prometheus");
    Response deleteResponse = client().performRequest(deleteRequest);
    Assert.assertEquals(204, deleteResponse.getStatusLine().getStatusCode());
  }

  @Test
  public void testShowDataSourcesCommands() throws IOException {
    JSONObject result = executeQuery("show datasources");
    verifyDataRows(result, rows("my_prometheus", "PROMETHEUS"), rows("@opensearch", "OPENSEARCH"));
    verifyColumn(result, columnName("DATASOURCE_NAME"), columnName("CONNECTOR_TYPE"));
  }

  @Test
  public void testShowDataSourcesCommandsWithWhereClause() throws IOException {
    JSONObject result = executeQuery("show datasources | where CONNECTOR_TYPE='PROMETHEUS'");
    verifyDataRows(result, rows("my_prometheus", "PROMETHEUS"));
    verifyColumn(result, columnName("DATASOURCE_NAME"), columnName("CONNECTOR_TYPE"));
  }
}
