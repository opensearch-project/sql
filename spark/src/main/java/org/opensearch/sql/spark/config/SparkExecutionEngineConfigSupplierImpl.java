package org.opensearch.sql.spark.config;

import static org.opensearch.sql.common.setting.Settings.Key.CLUSTER_NAME;
import static org.opensearch.sql.common.setting.Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG;

import java.security.AccessController;
import java.security.PrivilegedAction;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.cluster.ClusterName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.asyncquery.model.RequestContext;

@AllArgsConstructor
public class SparkExecutionEngineConfigSupplierImpl implements SparkExecutionEngineConfigSupplier {

  private Settings settings;

  @Override
  public SparkExecutionEngineConfig getSparkExecutionEngineConfig(RequestContext requestContext) {
    String sparkExecutionEngineConfigSettingString =
        this.settings.getSettingValue(SPARK_EXECUTION_ENGINE_CONFIG);
    SparkExecutionEngineConfig.SparkExecutionEngineConfigBuilder builder =
        SparkExecutionEngineConfig.builder();
    if (!StringUtils.isBlank(sparkExecutionEngineConfigSettingString)) {
      SparkExecutionEngineConfigClusterSetting setting =
          AccessController.doPrivileged(
              (PrivilegedAction<SparkExecutionEngineConfigClusterSetting>)
                  () ->
                      SparkExecutionEngineConfigClusterSetting.toSparkExecutionEngineConfig(
                          sparkExecutionEngineConfigSettingString));
      builder.applicationId(setting.getApplicationId());
      builder.executionRoleARN(setting.getExecutionRoleARN());
      builder.sparkSubmitParameterModifier(
          new OpenSearchSparkSubmitParameterModifier(setting.getSparkSubmitParameters()));
      builder.region(setting.getRegion());
    }
    ClusterName clusterName = settings.getSettingValue(CLUSTER_NAME);
    builder.clusterName(clusterName.value());
    return builder.build();
  }
}
