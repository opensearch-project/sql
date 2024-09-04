/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JARS_KEY;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.config.SparkSubmitParameterModifier;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

@ExtendWith(MockitoExtension.class)
public class SparkSubmitParametersBuilderTest {

  @Mock SparkParameterComposerCollection sparkParameterComposerCollection;
  @Mock SparkSubmitParameterModifier sparkSubmitParameterModifier;
  @Mock AsyncQueryRequestContext asyncQueryRequestContext;
  @Mock DispatchQueryRequest dispatchQueryRequest;

  @InjectMocks SparkSubmitParametersBuilder sparkSubmitParametersBuilder;

  @Test
  public void testBuildWithoutExtraParameters() {
    String params = sparkSubmitParametersBuilder.toString();

    assertNotNull(params);
  }

  @Test
  public void testBuildWithExtraParameters() {
    String params = sparkSubmitParametersBuilder.extraParameters("--conf A=1").toString();

    // Assert the conf is included with a space
    assertTrue(params.endsWith(" --conf A=1"));
  }

  @Test
  public void testBuildQueryString() {
    String rawQuery = "SHOW tables LIKE \"%\";";
    String expectedQueryInParams = "\"SHOW tables LIKE \\\"%\\\";\"";
    String params = sparkSubmitParametersBuilder.query(rawQuery).toString();
    assertTrue(params.contains(expectedQueryInParams));
  }

  @Test
  public void testBuildQueryStringNestedQuote() {
    String rawQuery = "SELECT '\"1\"'";
    String expectedQueryInParams = "\"SELECT '\\\"1\\\"'\"";
    String params = sparkSubmitParametersBuilder.query(rawQuery).toString();
    assertTrue(params.contains(expectedQueryInParams));
  }

  @Test
  public void testBuildQueryStringSpecialCharacter() {
    String rawQuery = "SELECT '{\"test ,:+\\\"inner\\\"/\\|?#><\"}'";
    String expectedQueryInParams = "SELECT '{\\\"test ,:+\\\\\\\"inner\\\\\\\"/\\\\|?#><\\\"}'";
    String params = sparkSubmitParametersBuilder.query(rawQuery).toString();
    assertTrue(params.contains(expectedQueryInParams));
  }

  @Test
  public void testClassName() {
    String params = sparkSubmitParametersBuilder.className("CLASS_NAME").toString();
    assertTrue(params.contains("--class CLASS_NAME"));
  }

  @Test
  public void testClusterName() {
    String params = sparkSubmitParametersBuilder.clusterName("CLUSTER_NAME").toString();
    assertTrue(params.contains("spark.emr-serverless.driverEnv.FLINT_CLUSTER_NAME=CLUSTER_NAME"));
    assertTrue(params.contains("spark.executorEnv.FLINT_CLUSTER_NAME=CLUSTER_NAME"));
  }

  @Test
  public void testOverrideConfigItem() {
    SparkSubmitParameters params = sparkSubmitParametersBuilder.getSparkSubmitParameters();
    params.setConfigItem(SPARK_JARS_KEY, "Overridden");
    String result = params.toString();

    assertTrue(result.contains(String.format("%s=Overridden", SPARK_JARS_KEY)));
  }

  @Test
  public void testDeleteConfigItem() {
    SparkSubmitParameters params = sparkSubmitParametersBuilder.getSparkSubmitParameters();
    params.deleteConfigItem(HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY);
    String result = params.toString();

    assertFalse(result.contains(HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY));
  }

  @Test
  public void testAddConfigItem() {
    SparkSubmitParameters params = sparkSubmitParametersBuilder.getSparkSubmitParameters();
    params.setConfigItem("AdditionalKey", "Value");
    String result = params.toString();

    assertTrue(result.contains("AdditionalKey=Value"));
  }

  @Test
  public void testStructuredStreaming() {
    SparkSubmitParameters params =
        sparkSubmitParametersBuilder.structuredStreaming(true).getSparkSubmitParameters();
    String result = params.toString();

    assertTrue(result.contains("spark.flint.job.type=streaming"));
  }

  @Test
  public void testNonStructuredStreaming() {
    SparkSubmitParameters params =
        sparkSubmitParametersBuilder.structuredStreaming(false).getSparkSubmitParameters();
    String result = params.toString();

    assertFalse(result.contains("spark.flint.job.type=streaming"));
  }

  @Test
  public void testSessionExecution() {
    SparkSubmitParameters params =
        sparkSubmitParametersBuilder
            .sessionExecution("SESSION_ID", "DATASOURCE_NAME")
            .getSparkSubmitParameters();
    String result = params.toString();

    assertTrue(
        result.contains("spark.flint.job.requestIndex=.query_execution_request_datasource_name"));
    assertTrue(result.contains("spark.flint.job.sessionId=SESSION_ID"));
  }

  @Test
  public void testAcceptModifier() {
    sparkSubmitParametersBuilder.acceptModifier(sparkSubmitParameterModifier);

    verify(sparkSubmitParameterModifier).modifyParameters(sparkSubmitParametersBuilder);
  }

  @Test
  public void testAcceptNullModifier() {
    sparkSubmitParametersBuilder.acceptModifier(null);
  }

  @Test
  public void testDataSource() {
    when(sparkParameterComposerCollection.isComposerRegistered(DataSourceType.S3GLUE))
        .thenReturn(true);

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setConnector(DataSourceType.S3GLUE)
            .setName("name")
            .build();
    SparkSubmitParameters params =
        sparkSubmitParametersBuilder
            .dataSource(metadata, dispatchQueryRequest, asyncQueryRequestContext)
            .getSparkSubmitParameters();

    verify(sparkParameterComposerCollection)
        .composeByDataSource(metadata, params, dispatchQueryRequest, asyncQueryRequestContext);
  }

  @Test
  public void testUnsupportedDataSource() {
    when(sparkParameterComposerCollection.isComposerRegistered(DataSourceType.S3GLUE))
        .thenReturn(false);

    DataSourceMetadata metadata =
        new DataSourceMetadata.Builder()
            .setConnector(DataSourceType.S3GLUE)
            .setName("name")
            .build();
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            sparkSubmitParametersBuilder.dataSource(
                metadata, dispatchQueryRequest, asyncQueryRequestContext));
  }

  @Test
  public void testAcceptComposers() {
    SparkSubmitParameters params =
        sparkSubmitParametersBuilder
            .acceptComposers(dispatchQueryRequest, asyncQueryRequestContext)
            .getSparkSubmitParameters();

    verify(sparkParameterComposerCollection)
        .compose(params, dispatchQueryRequest, asyncQueryRequestContext);
  }
}
