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
import org.junit.jupiter.api.Test;

public class ShowDataSourcesCommandIT extends PPLIntegTestCase {
  @Override
  protected void init() throws Exception {
    loadIndex(Index.DATASOURCES);
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
