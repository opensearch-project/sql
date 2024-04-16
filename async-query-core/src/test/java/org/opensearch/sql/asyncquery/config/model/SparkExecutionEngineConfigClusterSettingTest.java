/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.asyncquery.config.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class SparkExecutionEngineConfigClusterSettingTest {

  @Test
  public void testToSparkExecutionEngineConfigWithoutAllFields() {
    String json =
        "{"
            + "\"applicationId\": \"app-1\","
            + "\"executionRoleARN\": \"role-1\","
            + "\"region\": \"us-west-1\""
            + "}";
    SparkExecutionEngineConfigClusterSetting config =
        SparkExecutionEngineConfigClusterSetting.toSparkExecutionEngineConfig(json);

    assertEquals("app-1", config.getApplicationId());
    assertEquals("role-1", config.getExecutionRoleARN());
    assertEquals("us-west-1", config.getRegion());
    assertNull(config.getSparkSubmitParameters());
  }

  @Test
  public void testToSparkExecutionEngineConfigWithAllFields() {
    String json =
        "{"
            + "\"applicationId\": \"app-1\","
            + "\"executionRoleARN\": \"role-1\","
            + "\"region\": \"us-west-1\","
            + "\"sparkSubmitParameters\": \"--conf A=1\""
            + "}";
    SparkExecutionEngineConfigClusterSetting config =
        SparkExecutionEngineConfigClusterSetting.toSparkExecutionEngineConfig(json);

    assertEquals("app-1", config.getApplicationId());
    assertEquals("role-1", config.getExecutionRoleARN());
    assertEquals("us-west-1", config.getRegion());
    assertEquals("--conf A=1", config.getSparkSubmitParameters());
  }
}
