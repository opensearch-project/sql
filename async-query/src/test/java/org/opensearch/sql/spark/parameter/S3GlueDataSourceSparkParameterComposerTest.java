/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_PACKAGES_KEY;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceStatus;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.datasources.glue.GlueDataSourceFactory;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigClusterSettingLoader;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

@ExtendWith(MockitoExtension.class)
class S3GlueDataSourceSparkParameterComposerTest {

  public static final String VALID_URI = "https://test.host.com:9200";
  public static final String INVALID_URI = "http://test/\r\n";
  public static final String USERNAME = "USERNAME";
  public static final String PASSWORD = "PASSWORD";
  public static final String REGION = "REGION";
  public static final String TRUE = "true";
  public static final String ROLE_ARN = "arn:aws:iam::123456789012:role/ROLE_NAME";
  public static final String APP_ID = "APP_ID";
  public static final String CLUSTER_NAME = "CLUSTER_NAME";
  public static final String ACCOUNT_ID = "123456789012";
  public static final String SESSION_TAG = "SESSION_TAG";

  private static final String COMMON_EXPECTED_PARAMS =
      " --class org.apache.spark.sql.FlintJob "
          + getConfList(
              "spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::123456789012:role/ROLE_NAME",
              "spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::123456789012:role/ROLE_NAME",
              "spark.hive.metastore.glue.role.arn=arn:aws:iam::123456789012:role/ROLE_NAME",
              "spark.sql.catalog.DATASOURCE_NAME=org.opensearch.sql.FlintDelegatingSessionCatalog",
              "spark.flint.datasource.name=DATASOURCE_NAME",
              "spark.datasource.flint.host=test.host.com",
              "spark.datasource.flint.port=9200",
              "spark.datasource.flint.scheme=https");

  @Mock DispatchQueryRequest dispatchQueryRequest;

  @Test
  public void testBasicAuth() {
    DataSourceMetadata dataSourceMetadata =
        getDataSourceMetadata(AuthenticationType.BASICAUTH, VALID_URI);
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    new S3GlueDataSourceSparkParameterComposer(getSparkExecutionEngineConfigClusterSettingLoader())
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

    new S3GlueDataSourceSparkParameterComposer(getSparkExecutionEngineConfigClusterSettingLoader())
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

    new S3GlueDataSourceSparkParameterComposer(getSparkExecutionEngineConfigClusterSettingLoader())
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
            new S3GlueDataSourceSparkParameterComposer(
                    getSparkExecutionEngineConfigClusterSettingLoader())
                .compose(
                    dataSourceMetadata,
                    sparkSubmitParameters,
                    dispatchQueryRequest,
                    new NullAsyncQueryRequestContext()));
  }

  @Test
  public void testIcebergEnabled() {
    final Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(GlueDataSourceFactory.GLUE_ROLE_ARN, ROLE_ARN)
            .put(GlueDataSourceFactory.GLUE_ICEBERG_ENABLED, TRUE)
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_URI, VALID_URI)
            .put(
                GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH,
                AuthenticationType.BASICAUTH.getName())
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME, USERNAME)
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD, PASSWORD)
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_REGION, REGION)
            .build();

    final String expectedParams =
        " --class org.apache.spark.sql.FlintJob "
            + getConfList(
                "spark.jars.packages=package,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.0,software.amazon.awssdk:bundle:2.26.30",
                "spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::123456789012:role/ROLE_NAME",
                "spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::123456789012:role/ROLE_NAME",
                "spark.hive.metastore.glue.role.arn=arn:aws:iam::123456789012:role/ROLE_NAME",
                "spark.sql.catalog.DATASOURCE_NAME=org.opensearch.sql.FlintDelegatingSessionCatalog",
                "spark.flint.datasource.name=DATASOURCE_NAME",
                "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.opensearch.flint.spark.FlintSparkExtensions,org.opensearch.flint.spark.FlintPPLSparkExtensions",
                "spark.sql.catalog.spark_catalog.client.region=REGION",
                "spark.sql.catalog.spark_catalog.glue.account-id=123456789012",
                "spark.sql.catalog.spark_catalog.client.assume-role.arn=arn:aws:iam::123456789012:role/ROLE_NAME",
                "spark.sql.catalog.spark_catalog.client.assume-role.region=REGION",
                "spark.sql.iceberg.handle-timestamp-without-timezone=true",
                "spark.sql.catalog.spark_catalog.client.factory=org.apache.iceberg.aws.AssumeRoleAwsClientFactory",
                "spark.datasource.flint.host=test.host.com",
                "spark.datasource.flint.port=9200",
                "spark.datasource.flint.scheme=https",
                "spark.datasource.flint.auth=basic",
                "spark.datasource.flint.auth.username=USERNAME",
                "spark.datasource.flint.auth.password=PASSWORD");

    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata(properties);
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();
    sparkSubmitParameters.setConfigItem(SPARK_JAR_PACKAGES_KEY, "package");

    new S3GlueDataSourceSparkParameterComposer(getSparkExecutionEngineConfigClusterSettingLoader())
        .compose(
            dataSourceMetadata,
            sparkSubmitParameters,
            dispatchQueryRequest,
            new NullAsyncQueryRequestContext());

    assertEquals(expectedParams, sparkSubmitParameters.toString());
  }

  @Test
  public void testIcebergWithLakeFormationEnabled() {
    final Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(GlueDataSourceFactory.GLUE_ROLE_ARN, ROLE_ARN)
            .put(GlueDataSourceFactory.GLUE_ICEBERG_ENABLED, TRUE)
            .put(GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED, TRUE)
            .put(GlueDataSourceFactory.GLUE_LAKEFORMATION_SESSION_TAG, SESSION_TAG)
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_URI, VALID_URI)
            .put(
                GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH,
                AuthenticationType.BASICAUTH.getName())
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME, USERNAME)
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD, PASSWORD)
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_REGION, REGION)
            .build();

    final String expectedParams =
        " --class org.apache.spark.sql.FlintJob "
            + getConfList(
                "spark.jars.packages=package,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.0,software.amazon.awssdk:bundle:2.26.30",
                "spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::123456789012:role/ROLE_NAME",
                "spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN=arn:aws:iam::123456789012:role/ROLE_NAME",
                "spark.hive.metastore.glue.role.arn=arn:aws:iam::123456789012:role/ROLE_NAME",
                "spark.sql.catalog.DATASOURCE_NAME=org.opensearch.sql.FlintDelegatingSessionCatalog",
                "spark.flint.datasource.name=DATASOURCE_NAME",
                "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.spark_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
                "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.opensearch.flint.spark.FlintSparkExtensions,org.opensearch.flint.spark.FlintPPLSparkExtensions",
                "spark.sql.catalog.spark_catalog.client.region=REGION",
                "spark.sql.catalog.spark_catalog.glue.account-id=123456789012",
                "spark.sql.catalog.spark_catalog.client.assume-role.arn=arn:aws:iam::123456789012:role/ROLE_NAME",
                "spark.sql.catalog.spark_catalog.client.assume-role.region=REGION",
                "spark.sql.iceberg.handle-timestamp-without-timezone=true",
                "spark.flint.optimizer.covering.enabled=false",
                "spark.sql.catalog.spark_catalog.glue.lakeformation-enabled=true",
                "spark.sql.catalog.spark_catalog.client.factory=org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory",
                "spark.sql.catalog.spark_catalog.client.assume-role.tags.LakeFormationAuthorizedCaller=SESSION_TAG",
                "spark.datasource.flint.host=test.host.com",
                "spark.datasource.flint.port=9200",
                "spark.datasource.flint.scheme=https",
                "spark.datasource.flint.auth=basic",
                "spark.datasource.flint.auth.username=USERNAME",
                "spark.datasource.flint.auth.password=PASSWORD");

    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata(properties);
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();
    sparkSubmitParameters.setConfigItem(SPARK_JAR_PACKAGES_KEY, "package");

    new S3GlueDataSourceSparkParameterComposer(getSparkExecutionEngineConfigClusterSettingLoader())
        .compose(
            dataSourceMetadata,
            sparkSubmitParameters,
            dispatchQueryRequest,
            new NullAsyncQueryRequestContext());

    assertEquals(expectedParams, sparkSubmitParameters.toString());
  }

  @Test
  public void testIcebergWithLakeFormationEnabledNoSessionTag() {
    final Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(GlueDataSourceFactory.GLUE_ROLE_ARN, ROLE_ARN)
            .put(GlueDataSourceFactory.GLUE_ICEBERG_ENABLED, TRUE)
            .put(GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED, TRUE)
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_URI, VALID_URI)
            .put(
                GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH,
                AuthenticationType.BASICAUTH.getName())
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME, USERNAME)
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD, PASSWORD)
            .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_REGION, REGION)
            .build();

    DataSourceMetadata dataSourceMetadata = getDataSourceMetadata(properties);
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    final S3GlueDataSourceSparkParameterComposer composer =
        new S3GlueDataSourceSparkParameterComposer(
            getSparkExecutionEngineConfigClusterSettingLoader());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            composer.compose(
                dataSourceMetadata,
                sparkSubmitParameters,
                dispatchQueryRequest,
                new NullAsyncQueryRequestContext()));
  }

  @Test
  public void testNoClusterConfigAvailable() {
    DataSourceMetadata dataSourceMetadata =
        getDataSourceMetadata(AuthenticationType.BASICAUTH, VALID_URI);
    SparkSubmitParameters sparkSubmitParameters = new SparkSubmitParameters();

    final OpenSearchSettings settings = Mockito.mock(OpenSearchSettings.class);
    Mockito.when(settings.getSettingValue(Key.SPARK_EXECUTION_ENGINE_CONFIG)).thenReturn(null);

    final S3GlueDataSourceSparkParameterComposer composer =
        new S3GlueDataSourceSparkParameterComposer(
            new SparkExecutionEngineConfigClusterSettingLoader(settings));

    assertThrows(
        RuntimeException.class,
        () ->
            composer.compose(
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

  private DataSourceMetadata getDataSourceMetadata(Map<String, String> properties) {
    return new DataSourceMetadata.Builder()
        .setConnector(DataSourceType.S3GLUE)
        .setName("DATASOURCE_NAME")
        .setDescription("DESCRIPTION")
        .setResultIndex("RESULT_INDEX")
        .setDataSourceStatus(DataSourceStatus.ACTIVE)
        .setProperties(properties)
        .build();
  }

  private Map<String, String> getProperties(AuthenticationType authType, String uri) {
    return ImmutableMap.<String, String>builder()
        .put(GlueDataSourceFactory.GLUE_ROLE_ARN, ROLE_ARN)
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_URI, uri)
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH, authType.getName())
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME, USERNAME)
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD, PASSWORD)
        .put(GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_REGION, REGION)
        .build();
  }

  private SparkExecutionEngineConfigClusterSettingLoader
      getSparkExecutionEngineConfigClusterSettingLoader() {
    Gson gson = new Gson();
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("accountId", ACCOUNT_ID);
    jsonObject.addProperty("applicationId", APP_ID);
    jsonObject.addProperty("region", REGION);
    jsonObject.addProperty("executionRoleARN", ROLE_ARN);
    jsonObject.addProperty("sparkSubmitParameters", "");

    // Convert JsonObject to JSON string
    final String jsonString = gson.toJson(jsonObject);

    final OpenSearchSettings settings = Mockito.mock(OpenSearchSettings.class);
    Mockito.when(settings.getSettingValue(Key.SPARK_EXECUTION_ENGINE_CONFIG))
        .thenReturn(jsonString);

    return new SparkExecutionEngineConfigClusterSettingLoader(settings);
  }

  private static String getConfList(String... params) {
    return Arrays.stream(params)
        .map(param -> String.format(" --conf %s ", param))
        .collect(Collectors.joining());
  }
}
