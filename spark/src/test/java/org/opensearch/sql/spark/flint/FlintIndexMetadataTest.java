/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.AUTO_REFRESH;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.ENV_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.OPTIONS_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.PROPERTIES_KEY;
import static org.opensearch.sql.spark.flint.FlintIndexMetadata.SERVERLESS_EMR_JOB_ID;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class FlintIndexMetadataTest {

  @Test
  public void testAutoRefreshSetToTrue() {
    FlintIndexMetadata indexMetadata =
        FlintIndexMetadata.fromMetatdata(
            new Metadata()
                .addEnv(SERVERLESS_EMR_JOB_ID, EMR_JOB_ID)
                .addOptions(AUTO_REFRESH, "true")
                .metadata());
    assertTrue(indexMetadata.isAutoRefresh());
  }

  @Test
  public void testAutoRefreshSetToFalse() {
    FlintIndexMetadata indexMetadata =
        FlintIndexMetadata.fromMetatdata(
            new Metadata()
                .addEnv(SERVERLESS_EMR_JOB_ID, EMR_JOB_ID)
                .addOptions(AUTO_REFRESH, "false")
                .metadata());
    assertFalse(indexMetadata.isAutoRefresh());
  }

  @Test
  public void testWithOutAutoRefresh() {
    FlintIndexMetadata indexMetadata =
        FlintIndexMetadata.fromMetatdata(
            new Metadata()
                .addEnv(SERVERLESS_EMR_JOB_ID, EMR_JOB_ID)
                .addOptions(AUTO_REFRESH, "false")
                .metadata());
    assertFalse(indexMetadata.isAutoRefresh());
  }

  static class Metadata {
    private final Map<String, Object> properties;
    private final Map<String, String> env;
    private final Map<String, String> options;

    private Metadata() {
      properties = new HashMap<>();
      env = new HashMap<>();
      options = new HashMap<>();
    }

    public Metadata addEnv(String key, String value) {
      env.put(key, value);
      return this;
    }

    public Metadata addOptions(String key, String value) {
      options.put(key, value);
      return this;
    }

    public Map<String, Object> metadata() {
      Map<String, Object> result = new HashMap<>();
      properties.put(ENV_KEY, env);
      result.put(OPTIONS_KEY, options);
      result.put(PROPERTIES_KEY, properties);
      return result;
    }
  }
}
