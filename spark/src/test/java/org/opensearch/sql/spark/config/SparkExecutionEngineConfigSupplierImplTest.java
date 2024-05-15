package org.opensearch.sql.spark.config;

import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.TEST_CLUSTER_NAME;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.asyncquery.model.RequestContext;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;

@ExtendWith(MockitoExtension.class)
public class SparkExecutionEngineConfigSupplierImplTest {

  @Mock private Settings settings;
  @Mock private RequestContext requestContext;

  @Test
  void testGetSparkExecutionEngineConfig() {
    SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier =
        new SparkExecutionEngineConfigSupplierImpl(settings);
    when(settings.getSettingValue(Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG))
        .thenReturn(
            "{"
                + "\"applicationId\": \"00fd775baqpu4g0p\","
                + "\"executionRoleARN\": \"arn:aws:iam::270824043731:role/emr-job-execution-role\","
                + "\"region\": \"eu-west-1\","
                + "\"sparkSubmitParameters\": \"--conf spark.dynamicAllocation.enabled=false\""
                + "}");
    when(settings.getSettingValue(Settings.Key.CLUSTER_NAME))
        .thenReturn(new ClusterName(TEST_CLUSTER_NAME));

    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(requestContext);

    Assertions.assertEquals("00fd775baqpu4g0p", sparkExecutionEngineConfig.getApplicationId());
    Assertions.assertEquals(
        "arn:aws:iam::270824043731:role/emr-job-execution-role",
        sparkExecutionEngineConfig.getExecutionRoleARN());
    Assertions.assertEquals("eu-west-1", sparkExecutionEngineConfig.getRegion());
    Assertions.assertEquals(TEST_CLUSTER_NAME, sparkExecutionEngineConfig.getClusterName());
    SparkSubmitParameters parameters = SparkSubmitParameters.builder().build();
    sparkExecutionEngineConfig.getSparkSubmitParameterModifier().modifyParameters(parameters);
    Assertions.assertTrue(
        parameters.toString().contains("--conf spark.dynamicAllocation.enabled=false"));
  }

  @Test
  void testGetSparkExecutionEngineConfigWithNullSetting() {
    SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier =
        new SparkExecutionEngineConfigSupplierImpl(settings);
    when(settings.getSettingValue(Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG)).thenReturn(null);
    when(settings.getSettingValue(Settings.Key.CLUSTER_NAME))
        .thenReturn(new ClusterName(TEST_CLUSTER_NAME));

    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(requestContext);

    Assertions.assertNull(sparkExecutionEngineConfig.getApplicationId());
    Assertions.assertNull(sparkExecutionEngineConfig.getExecutionRoleARN());
    Assertions.assertNull(sparkExecutionEngineConfig.getRegion());
    Assertions.assertNull(sparkExecutionEngineConfig.getSparkSubmitParameterModifier());
    Assertions.assertEquals(TEST_CLUSTER_NAME, sparkExecutionEngineConfig.getClusterName());
  }
}
