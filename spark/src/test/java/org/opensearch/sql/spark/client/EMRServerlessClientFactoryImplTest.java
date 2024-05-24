/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;
import org.opensearch.sql.spark.constants.TestConstants;

@ExtendWith(MockitoExtension.class)
public class EMRServerlessClientFactoryImplTest {

  @Mock private SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier;

  @Test
  public void testGetClient() {
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(any()))
        .thenReturn(createSparkExecutionEngineConfig());
    EMRServerlessClientFactory emrServerlessClientFactory =
        new EMRServerlessClientFactoryImpl(sparkExecutionEngineConfigSupplier);
    EMRServerlessClient emrserverlessClient = emrServerlessClientFactory.getClient();
    Assertions.assertNotNull(emrserverlessClient);
  }

  @Test
  public void testGetClientWithChangeInSetting() {
    SparkExecutionEngineConfig sparkExecutionEngineConfig = createSparkExecutionEngineConfig();
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(any()))
        .thenReturn(sparkExecutionEngineConfig);
    EMRServerlessClientFactory emrServerlessClientFactory =
        new EMRServerlessClientFactoryImpl(sparkExecutionEngineConfigSupplier);
    EMRServerlessClient emrserverlessClient = emrServerlessClientFactory.getClient();
    Assertions.assertNotNull(emrserverlessClient);

    EMRServerlessClient emrServerlessClient1 = emrServerlessClientFactory.getClient();
    Assertions.assertEquals(emrServerlessClient1, emrserverlessClient);

    sparkExecutionEngineConfig.setRegion(TestConstants.US_WEST_REGION);
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(any()))
        .thenReturn(sparkExecutionEngineConfig);
    EMRServerlessClient emrServerlessClient2 = emrServerlessClientFactory.getClient();
    Assertions.assertNotEquals(emrServerlessClient2, emrserverlessClient);
    Assertions.assertNotEquals(emrServerlessClient2, emrServerlessClient1);
  }

  @Test
  public void testGetClientWithException() {
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(any())).thenReturn(null);
    EMRServerlessClientFactory emrServerlessClientFactory =
        new EMRServerlessClientFactoryImpl(sparkExecutionEngineConfigSupplier);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class, emrServerlessClientFactory::getClient);
    Assertions.assertEquals(
        "Async Query APIs are disabled. Please configure plugins.query.executionengine.spark.config"
            + " in cluster settings to enable them.",
        illegalArgumentException.getMessage());
  }

  @Test
  public void testGetClientWithExceptionWithNullRegion() {
    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        SparkExecutionEngineConfig.builder().build();
    when(sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(any()))
        .thenReturn(sparkExecutionEngineConfig);
    EMRServerlessClientFactory emrServerlessClientFactory =
        new EMRServerlessClientFactoryImpl(sparkExecutionEngineConfigSupplier);
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class, emrServerlessClientFactory::getClient);
    Assertions.assertEquals(
        "Async Query APIs are disabled. Please configure plugins.query.executionengine.spark.config"
            + " in cluster settings to enable them.",
        illegalArgumentException.getMessage());
  }

  private SparkExecutionEngineConfig createSparkExecutionEngineConfig() {
    return SparkExecutionEngineConfig.builder()
        .region(TestConstants.US_EAST_REGION)
        .executionRoleARN(TestConstants.EMRS_EXECUTION_ROLE)
        .sparkSubmitParameterModifier((sparkSubmitParameters) -> {})
        .clusterName(TestConstants.TEST_CLUSTER_NAME)
        .applicationId(TestConstants.EMRS_APPLICATION_ID)
        .build();
  }
}
