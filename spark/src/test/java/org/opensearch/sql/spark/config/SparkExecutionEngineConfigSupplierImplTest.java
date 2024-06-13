/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.SPARK_SUBMIT_PARAMETERS;
import static org.opensearch.sql.spark.constants.TestConstants.TEST_CLUSTER_NAME;
import static org.opensearch.sql.spark.constants.TestConstants.US_WEST_REGION;

import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;

@ExtendWith(MockitoExtension.class)
public class SparkExecutionEngineConfigSupplierImplTest {

  @Mock private Settings settings;
  @Mock private AsyncQueryRequestContext asyncQueryRequestContext;

  @Test
  void testGetSparkExecutionEngineConfig() {
    SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier =
        new SparkExecutionEngineConfigSupplierImpl(settings);
    when(settings.getSettingValue(Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG))
        .thenReturn(getConfigJson());
    when(settings.getSettingValue(Settings.Key.CLUSTER_NAME))
        .thenReturn(new ClusterName(TEST_CLUSTER_NAME));

    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(asyncQueryRequestContext);
    SparkSubmitParameters parameters = SparkSubmitParameters.builder().build();
    sparkExecutionEngineConfig.getSparkSubmitParameterModifier().modifyParameters(parameters);

    Assertions.assertEquals(EMRS_APPLICATION_ID, sparkExecutionEngineConfig.getApplicationId());
    Assertions.assertEquals(EMRS_EXECUTION_ROLE, sparkExecutionEngineConfig.getExecutionRoleARN());
    Assertions.assertEquals(US_WEST_REGION, sparkExecutionEngineConfig.getRegion());
    Assertions.assertEquals(TEST_CLUSTER_NAME, sparkExecutionEngineConfig.getClusterName());
    Assertions.assertTrue(parameters.toString().contains(SPARK_SUBMIT_PARAMETERS));
  }

  String getConfigJson() {
    return new JSONObject()
        .put("applicationId", EMRS_APPLICATION_ID)
        .put("executionRoleARN", EMRS_EXECUTION_ROLE)
        .put("region", US_WEST_REGION)
        .put("sparkSubmitParameters", SPARK_SUBMIT_PARAMETERS)
        .toString();
  }

  @Test
  void testGetSparkExecutionEngineConfigWithNullSetting() {
    SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier =
        new SparkExecutionEngineConfigSupplierImpl(settings);
    when(settings.getSettingValue(Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG)).thenReturn(null);
    when(settings.getSettingValue(Settings.Key.CLUSTER_NAME))
        .thenReturn(new ClusterName(TEST_CLUSTER_NAME));

    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(asyncQueryRequestContext);

    Assertions.assertNull(sparkExecutionEngineConfig.getApplicationId());
    Assertions.assertNull(sparkExecutionEngineConfig.getExecutionRoleARN());
    Assertions.assertNull(sparkExecutionEngineConfig.getRegion());
    Assertions.assertNull(sparkExecutionEngineConfig.getSparkSubmitParameterModifier());
    Assertions.assertEquals(TEST_CLUSTER_NAME, sparkExecutionEngineConfig.getClusterName());
  }
}
