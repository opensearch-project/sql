/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import static org.opensearch.sql.common.setting.Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.common.setting.Settings;

@RequiredArgsConstructor
public class SparkExecutionEngineConfigClusterSettingLoader {
  private final Settings settings;

  public Optional<SparkExecutionEngineConfigClusterSetting> load() {
    String sparkExecutionEngineConfigSettingString =
        this.settings.getSettingValue(SPARK_EXECUTION_ENGINE_CONFIG);
    if (!StringUtils.isBlank(sparkExecutionEngineConfigSettingString)) {
      return Optional.of(
          AccessController.doPrivileged(
              (PrivilegedAction<SparkExecutionEngineConfigClusterSetting>)
                  () ->
                      SparkExecutionEngineConfigClusterSetting.toSparkExecutionEngineConfig(
                          sparkExecutionEngineConfigSettingString)));
    } else {
      return Optional.empty();
    }
  }
}
