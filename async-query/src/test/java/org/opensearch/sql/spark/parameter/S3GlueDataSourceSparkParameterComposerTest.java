/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.datasources.glue.GlueDataSourceFactory;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

@ExtendWith(MockitoExtension.class)
class S3GlueDataSourceSparkParameterComposerTest {

  public static final String VALID_URI = "https://test.host.com:9200";
  public static final String INVALID_URI = "http://test/\r\n";
  public static final String USERNAME = "USERNAME";
  public static final String PASSWORD = "PASSWORD";
  public static final String REGION = "REGION";
  public static final String TRUE = "true";
  public static final String ROLE_ARN = "ROLE_ARN";

  private static final String COMMON_EXPECTED_PARAMS =
      " --class org.apache.spark.sql.FlintJob "
          + getConfList(
              "spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=ROLE_ARN",
              "spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=ROLE_ARN",
              "spark.hive.metastore.glue.role.arn=ROLE_ARN",
              "spark.sql.catalog.DATASOURCE_NAME=org.opensearch.sql.FlintDelegatingSessionCatalog",
              "spark.flint.datasource.name=DATASOURCE_NAME",
              "spark.emr-serverless.lakeformation.enabled=true",
              "spark.flint.optimizer.covering.enabled=false",
              "spark.datasource.flint.host=test.host.com",
              "spark.datasource.flint.port=9200",
              "spark.datasource.flint.scheme=https");

  @Mock DispatchQueryRequest dispatchQueryRequest;

  @Test
  public void testBasicAuth() {
    DataSourceMetadata dataSourceMetadata =
        getDataSourceMetadata(AuthenticationType.BASICAUTH, VALID_URI);
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    new S3GlueDataSourceSparkParameterComposer()
        .compose(
            dataSourceMetadata,
            sparkSubmitParameters,
            dispatchQueryRequest,
            new NullAsyncQueryRequestContext());

    assertEquals(
        COMMON_EXPECTED_PARAMS
            + getConfList(
                "spark.datasource.flint.auth=basic",
                "spark.datasource.flint.auth.username=USERNAME",
                "spark.datasource.flint.auth.password=PASSWORD"),
        sparkSubmitParameters.toString());
  }

  @Test
  public void testComposeWithSigV4Auth() {
    DataSourceMetadata dataSourceMetadata =
        getDataSourceMetadata(AuthenticationType.AWSSIGV4AUTH, VALID_URI);
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    new S3GlueDataSourceSparkParameterComposer()
        .compose(
            dataSourceMetadata,
            sparkSubmitParameters,
            dispatchQueryRequest,
            new NullAsyncQueryRequestContext());

    assertEquals(
        COMMON_EXPECTED_PARAMS
            + getConfList(
                "spark.datasource.flint.auth=sigv4", "spark.datasource.flint.region=REGION"),
        sparkSubmitParameters.toString());
  }

  @Test
  public void testComposeWithNoAuth() {
    DataSourceMetadata dataSourceMetadata =
        getDataSourceMetadata(AuthenticationType.NOAUTH, VALID_URI);
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    new S3GlueDataSourceSparkParameterComposer()
        .compose(
            dataSourceMetadata,
            sparkSubmitParameters,
            dispatchQueryRequest,
            new NullAsyncQueryRequestContext());

    assertEquals(
        COMMON_EXPECTED_PARAMS + getConfList("spark.datasource.flint.auth=noauth"),
        sparkSubmitParameters.toString());
  }

  @Test
  public void testComposeWithBadUri() {
    DataSourceMetadata dataSourceMetadata =
        getDataSourceMetadata(AuthenticationType.NOAUTH, INVALID_URI);
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new S3GlueDataSourceSparkParameterComposer()
                .compose(
                    dataSourceMetadata,
                    sparkSubmitParameters,
                    dispatchQueryRequest,
                    new NullAsyncQueryRequestContext()));
  }

  private DataSourceMetadata getDataSourceMetadata(
      AuthenticationType authenticationType, String uri) {
    return new DataSourceMetadata.Builder()
        .setConnector(DataSourceType.S3GLUE)
        .setName("DATASOURCE_NAME")
        .setDescription("DESCRIPTION")
        .setResultIndex("RESULT_INDEX")
        .setDataSourceStatus(DataSourceStatus.ACTIVE)
        .setProperties(getProperties(authenticationType, uri))
        .build();
  }

  private Map<String, String> getProperties(AuthenticationType authType, String uri) {
    return ImmutableMap.<String, String>builder()
        .put(GlueDataSourceFactory.GLUE_ROLE_ARN, ROLE_ARN)
        .put(GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED, TRUE)
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_URI, uri)
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH, authType.getName())
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME, USERNAME)
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD, PASSWORD)
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_REGION, REGION)
        .build();
  }

  private static String getConfList(String... params) {
    return Arrays.stream(params)
        .map(param -> String.format(" --conf %s ", param))
        .collect(Collectors.joining());
  }
}
