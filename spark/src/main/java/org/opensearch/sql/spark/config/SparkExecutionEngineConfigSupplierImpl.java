package org.opensearch.sql.spark.config;

import static org.opensearch.sql.common.setting.Settings.Key.CLUSTER_NAME;
import static org.opensearch.sql.common.setting.Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG;

import java.security.AccessController;
import java.security.PrivilegedAction;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.cluster.ClusterName;
import org.opensearch.sql.common.setting.Settings;

@AllArgsConstructor
public class SparkExecutionEngineConfigSupplierImpl implements SparkExecutionEngineConfigSupplier {

  private Settings settings;

  @Override
  public SparkExecutionEngineConfig getSparkExecutionEngineConfig() {
    String sparkExecutionEngineConfigSettingString =
        this.settings.getSettingValue(SPARK_EXECUTION_ENGINE_CONFIG);
    SparkExecutionEngineConfig sparkExecutionEngineConfig = new SparkExecutionEngineConfig();
    if (!StringUtils.isBlank(sparkExecutionEngineConfigSettingString)) {
      SparkExecutionEngineConfigClusterSetting sparkExecutionEngineConfigClusterSetting =
          AccessController.doPrivileged(
              (PrivilegedAction<SparkExecutionEngineConfigClusterSetting>)
                  () ->
                      SparkExecutionEngineConfigClusterSetting.toSparkExecutionEngineConfig(
                          sparkExecutionEngineConfigSettingString));
      sparkExecutionEngineConfig.setApplicationId(
          sparkExecutionEngineConfigClusterSetting.getApplicationId());
      sparkExecutionEngineConfig.setExecutionRoleARN(
          sparkExecutionEngineConfigClusterSetting.getExecutionRoleARN());
      sparkExecutionEngineConfig.setSparkSubmitParameters(
          sparkExecutionEngineConfigClusterSetting.getSparkSubmitParameters());
      sparkExecutionEngineConfig.setRegion(sparkExecutionEngineConfigClusterSetting.getRegion());
    }
    ClusterName clusterName = settings.getSettingValue(CLUSTER_NAME);
    sparkExecutionEngineConfig.setClusterName(clusterName.value());
    return sparkExecutionEngineConfig;
  }
}
