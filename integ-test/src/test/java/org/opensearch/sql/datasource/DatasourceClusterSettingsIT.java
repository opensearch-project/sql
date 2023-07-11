/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class DatasourceClusterSettingsIT extends PPLIntegTestCase {

  private static final Logger LOG = LogManager.getLogger();
  @Test
  public void testGetDatasourceClusterSettings() throws IOException {
    JSONObject clusterSettings = getAllClusterSettings();
    assertThat(clusterSettings.query("/defaults/plugins.query.datasources.encryption.masterkey"),
        equalTo(null));
  }


  @Test
  public void testPutDatasourceClusterSettings() throws IOException {
    final ResponseException exception =
        expectThrows(ResponseException.class, () -> updateClusterSettings(new ClusterSetting(PERSISTENT,
    "plugins.query.datasources.encryption.masterkey",
        "masterkey")));
    JSONObject resp = new JSONObject(TestUtils.getResponseBody(exception.getResponse()));
    assertThat(resp.getInt("status"), equalTo(400));
    assertThat(resp.query("/error/root_cause/0/reason"),
        equalTo("final persistent setting [plugins.query.datasources.encryption.masterkey], not updateable"));
    assertThat(resp.query("/error/type"), equalTo("settings_exception"));
  }

}
