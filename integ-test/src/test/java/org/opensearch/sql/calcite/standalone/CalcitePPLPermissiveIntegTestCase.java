/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.OpenSearchDataSourceFactory;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

public abstract class CalcitePPLPermissiveIntegTestCase extends CalcitePPLIntegTestCase {

  @Override
  protected OpenSearchDataSourceFactory getDataSourceFactory(
      OpenSearchClient client, Settings settings) {
    return new OpenSearchDataSourceFactory(client, settings) {
      @Override
      public DataSource createDataSource(DataSourceMetadata metadata) {
        return new DataSource(
            metadata.getName(),
            DataSourceType.OPENSEARCH,
            new OpenSearchStorageEngine(client, settings, true));
      }
    };
  }

  @Override
  protected Settings getSettings() {
    return defaultSettings(getDefaultSettingsBuilder().put(Key.PPL_QUERY_PERMISSIVE, true).build());
  }

  protected void assertExplainYaml(String query, String expectedYaml) {
    String actualYaml = explainQueryYaml(query);
    assertTrue(getDiffMessage(expectedYaml, actualYaml), expectedYaml.equals(actualYaml));
  }

  private String getDiffMessage(String expectedYaml, String actualYaml) {
    return "Explain did not match:\n"
        + String.format(
            "# Expected: %s# Actual: %s", blockQuote(expectedYaml), blockQuote(actualYaml));
  }

  private String blockQuote(String str) {
    return "```\n" + str + "```\n";
  }
}
