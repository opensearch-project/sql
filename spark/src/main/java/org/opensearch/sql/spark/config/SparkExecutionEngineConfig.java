/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.gson.Gson;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkExecutionEngineConfig {
  private String applicationId;
  private String region;
  private String executionRoleARN;

  public static SparkExecutionEngineConfig toSparkExecutionEngineConfig(String jsonString) {
    return new Gson().fromJson(jsonString, SparkExecutionEngineConfig.class);
  }
}
