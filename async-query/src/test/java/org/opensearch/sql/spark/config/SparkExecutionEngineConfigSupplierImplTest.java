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

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;

@ExtendWith(MockitoExtension.class)
public class SparkExecutionEngineConfigSupplierImplTest {

  @Mock private Settings settings;
  @Mock private AsyncQueryRequestContext asyncQueryRequestContext;

  @Mock
  private SparkExecutionEngineConfigClusterSettingLoader
      sparkExecutionEngineConfigClusterSettingLoader;

  @Test
  void testGetSparkExecutionEngineConfig() {
    SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier =
        new SparkExecutionEngineConfigSupplierImpl(
            settings, sparkExecutionEngineConfigClusterSettingLoader);
    when(settings.getSettingValue(Settings.Key.CLUSTER_NAME))
        .thenReturn(new ClusterName(TEST_CLUSTER_NAME));
    when(sparkExecutionEngineConfigClusterSettingLoader.load())
        .thenReturn(Optional.of(getClusterSetting()));

    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(asyncQueryRequestContext);

    Assertions.assertEquals(EMRS_APPLICATION_ID, sparkExecutionEngineConfig.getApplicationId());
    Assertions.assertEquals(EMRS_EXECUTION_ROLE, sparkExecutionEngineConfig.getExecutionRoleARN());
    Assertions.assertEquals(US_WEST_REGION, sparkExecutionEngineConfig.getRegion());
    Assertions.assertEquals(TEST_CLUSTER_NAME, sparkExecutionEngineConfig.getClusterName());
  }

  SparkExecutionEngineConfigClusterSetting getClusterSetting() {
    return SparkExecutionEngineConfigClusterSetting.builder()
        .applicationId(EMRS_APPLICATION_ID)
        .executionRoleARN(EMRS_EXECUTION_ROLE)
        .region(US_WEST_REGION)
        .sparkSubmitParameters(SPARK_SUBMIT_PARAMETERS)
        .build();
  }

  @Test
  void testGetSparkExecutionEngineConfigWithNullSetting() {
    SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier =
        new SparkExecutionEngineConfigSupplierImpl(
            settings, sparkExecutionEngineConfigClusterSettingLoader);
    when(settings.getSettingValue(Settings.Key.CLUSTER_NAME))
        .thenReturn(new ClusterName(TEST_CLUSTER_NAME));
    when(sparkExecutionEngineConfigClusterSettingLoader.load()).thenReturn(Optional.empty());

    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(asyncQueryRequestContext);

    Assertions.assertNull(sparkExecutionEngineConfig.getApplicationId());
    Assertions.assertNull(sparkExecutionEngineConfig.getExecutionRoleARN());
    Assertions.assertNull(sparkExecutionEngineConfig.getRegion());
    Assertions.assertNull(sparkExecutionEngineConfig.getSparkSubmitParameterModifier());
    Assertions.assertEquals(TEST_CLUSTER_NAME, sparkExecutionEngineConfig.getClusterName());
  }
}
