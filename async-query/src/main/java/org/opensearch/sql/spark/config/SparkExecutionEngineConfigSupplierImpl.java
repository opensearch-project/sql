/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import static org.opensearch.sql.common.setting.Settings.Key.CLUSTER_NAME;

import lombok.AllArgsConstructor;
import org.opensearch.cluster.ClusterName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;

@AllArgsConstructor
public class SparkExecutionEngineConfigSupplierImpl implements SparkExecutionEngineConfigSupplier {

  private final Settings settings;
  private final SparkExecutionEngineConfigClusterSettingLoader settingLoader;

  @Override
  public SparkExecutionEngineConfig getSparkExecutionEngineConfig(
      AsyncQueryRequestContext asyncQueryRequestContext) {
    ClusterName clusterName = settings.getSettingValue(CLUSTER_NAME);
    return getBuilderFromSettingsIfAvailable().clusterName(clusterName.value()).build();
  }

  private SparkExecutionEngineConfig.SparkExecutionEngineConfigBuilder
      getBuilderFromSettingsIfAvailable() {
    return settingLoader
        .load()
        .map(
            setting ->
                SparkExecutionEngineConfig.builder()
                    .applicationId(setting.getApplicationId())
                    .executionRoleARN(setting.getExecutionRoleARN())
                    .region(setting.getRegion()))
        .orElse(SparkExecutionEngineConfig.builder());
  }
}
