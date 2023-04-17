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

import java.io.IOException;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;

public class ShowDataSourcesCommandIT extends PPLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.DATASOURCES);
  }

  /**
   * Integ tests are dependent on self generated metrics in prometheus instance.
   * When running individual integ tests there
   * is no time for generation of metrics in the test prometheus instance.
   * This method gives prometheus time to generate metrics on itself.
   * @throws InterruptedException
   */
  @BeforeClass
  protected static void metricGenerationWait() throws InterruptedException {
    Thread.sleep(10000);
  }

  @Test
  public void testShowDataSourcesCommands() throws IOException {
    JSONObject result = executeQuery("show datasources");
    verifyDataRows(result,
        rows("my_prometheus", "PROMETHEUS"),
        rows("@opensearch", "OPENSEARCH"));
    verifyColumn(
        result,
        columnName("DATASOURCE_NAME"),
        columnName("CONNECTOR_TYPE")
    );
  }

  @Test
  public void testShowDataSourcesCommandsWithWhereClause() throws IOException {
    JSONObject result = executeQuery("show datasources | where CONNECTOR_TYPE='PROMETHEUS'");
    verifyDataRows(result,
        rows("my_prometheus", "PROMETHEUS"));
    verifyColumn(
        result,
        columnName("DATASOURCE_NAME"),
        columnName("CONNECTOR_TYPE")
    );
  }

}
