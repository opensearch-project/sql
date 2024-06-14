/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.common.setting.Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG;
import static org.opensearch.sql.spark.constants.TestConstants.ACCOUNT_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.SPARK_SUBMIT_PARAMETERS;
import static org.opensearch.sql.spark.constants.TestConstants.US_WEST_REGION;

import java.util.Optional;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;

@ExtendWith(MockitoExtension.class)
class SparkExecutionEngineConfigClusterSettingLoaderTest {
  @Mock Settings settings;

  @InjectMocks
  SparkExecutionEngineConfigClusterSettingLoader sparkExecutionEngineConfigClusterSettingLoader;

  @Test
  public void blankConfig() {
    when(settings.getSettingValue(SPARK_EXECUTION_ENGINE_CONFIG)).thenReturn("");

    Optional<SparkExecutionEngineConfigClusterSetting> result =
        sparkExecutionEngineConfigClusterSettingLoader.load();

    assertTrue(result.isEmpty());
  }

  @Test
  public void validConfig() {
    when(settings.getSettingValue(SPARK_EXECUTION_ENGINE_CONFIG)).thenReturn(getConfigJson());

    SparkExecutionEngineConfigClusterSetting result =
        sparkExecutionEngineConfigClusterSettingLoader.load().get();

    Assertions.assertEquals(ACCOUNT_ID, result.getAccountId());
    Assertions.assertEquals(EMRS_APPLICATION_ID, result.getApplicationId());
    Assertions.assertEquals(EMRS_EXECUTION_ROLE, result.getExecutionRoleARN());
    Assertions.assertEquals(US_WEST_REGION, result.getRegion());
    Assertions.assertEquals(SPARK_SUBMIT_PARAMETERS, result.getSparkSubmitParameters());
  }

  String getConfigJson() {
    return new JSONObject()
        .put("accountId", ACCOUNT_ID)
        .put("applicationId", EMRS_APPLICATION_ID)
        .put("executionRoleARN", EMRS_EXECUTION_ROLE)
        .put("region", US_WEST_REGION)
        .put("sparkSubmitParameters", SPARK_SUBMIT_PARAMETERS)
        .toString();
  }
}
