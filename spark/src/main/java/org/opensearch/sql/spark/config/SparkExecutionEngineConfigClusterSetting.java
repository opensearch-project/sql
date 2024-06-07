/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.gson.Gson;
import lombok.Data;

/**
 * This POJO is just for reading stringified json in `plugins.query.executionengine.spark.config`
 * setting.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkExecutionEngineConfigClusterSetting {
  // optional
  private String accountId;
  private String applicationId;
  private String region;
  private String executionRoleARN;

  /** Additional Spark submit parameters to append to request. */
  private String sparkSubmitParameters;

  public static SparkExecutionEngineConfigClusterSetting toSparkExecutionEngineConfig(
      String jsonString) {
    return new Gson().fromJson(jsonString, SparkExecutionEngineConfigClusterSetting.class);
  }
}
