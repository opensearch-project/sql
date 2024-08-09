/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_ICEBERG_ENABLED;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_REGION;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_URI;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_LAKEFORMATION_SESSION_TAG;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_ROLE_ARN;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DRIVER_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_ACCELERATE_USING_COVERING_INDEX;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DATA_SOURCE_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DELEGATE_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_PASSWORD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_USERNAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AWSREGION_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_PPL_EXTENSION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SQL_EXTENSION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HIVE_METASTORE_GLUE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_ASSUME_ROLE_CLIENT_FACTORY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_GLUE_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_LF_CLIENT_FACTORY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_SESSION_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_SPARK_EXTENSION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_SPARK_JARS;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_TS_WO_TZ;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG_CATALOG_IMPL;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG_CLIENT_ASSUME_ROLE_ARN;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG_CLIENT_ASSUME_ROLE_REGION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG_CLIENT_FACTORY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG_CLIENT_REGION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG_GLUE_ACCOUNT_ID;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG_GLUE_LF_ENABLED;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG_LF_SESSION_TAG_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_PACKAGES_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_SQL_EXTENSIONS_KEY;

import com.amazonaws.arn.Arn;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigClusterSetting;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigClusterSettingLoader;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

@RequiredArgsConstructor
public class S3GlueDataSourceSparkParameterComposer implements DataSourceSparkParameterComposer {
  public static final String FLINT_BASIC_AUTH = "basic";
  public static final String FALSE = "false";
  public static final String TRUE = "true";

  private final SparkExecutionEngineConfigClusterSettingLoader settingLoader;

  @Override
  public void compose(
      DataSourceMetadata metadata,
      SparkSubmitParameters params,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context) {
    final Optional<SparkExecutionEngineConfigClusterSetting> maybeClusterSettings =
        settingLoader.load();
    if (!maybeClusterSettings.isPresent()) {
      throw new RuntimeException("No cluster settings present");
    }
    final SparkExecutionEngineConfigClusterSetting clusterSetting = maybeClusterSettings.get();
    final String region = clusterSetting.getRegion();

    final String roleArn = metadata.getProperties().get(GLUE_ROLE_ARN);
    final String accountId = Arn.fromString(roleArn).getAccountId();

    params.setConfigItem(DRIVER_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
    params.setConfigItem(EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
    params.setConfigItem(HIVE_METASTORE_GLUE_ARN_KEY, roleArn);
    params.setConfigItem("spark.sql.catalog." + metadata.getName(), FLINT_DELEGATE_CATALOG);
    params.setConfigItem(FLINT_DATA_SOURCE_KEY, metadata.getName());

    final boolean icebergEnabled =
        BooleanUtils.toBoolean(metadata.getProperties().get(GLUE_ICEBERG_ENABLED));
    if (icebergEnabled) {
      params.setConfigItem(
          SPARK_JAR_PACKAGES_KEY,
          params.getConfigItem(SPARK_JAR_PACKAGES_KEY) + "," + ICEBERG_SPARK_JARS);
      params.setConfigItem(SPARK_CATALOG, ICEBERG_SESSION_CATALOG);
      params.setConfigItem(SPARK_CATALOG_CATALOG_IMPL, ICEBERG_GLUE_CATALOG);
      params.setConfigItem(
          SPARK_SQL_EXTENSIONS_KEY,
          ICEBERG_SPARK_EXTENSION + "," + FLINT_SQL_EXTENSION + "," + FLINT_PPL_EXTENSION);

      params.setConfigItem(SPARK_CATALOG_CLIENT_REGION, region);
      params.setConfigItem(SPARK_CATALOG_GLUE_ACCOUNT_ID, accountId);
      params.setConfigItem(SPARK_CATALOG_CLIENT_ASSUME_ROLE_ARN, roleArn);
      params.setConfigItem(SPARK_CATALOG_CLIENT_ASSUME_ROLE_REGION, region);
      params.setConfigItem(ICEBERG_TS_WO_TZ, TRUE);

      final boolean lakeFormationEnabled =
          BooleanUtils.toBoolean(metadata.getProperties().get(GLUE_LAKEFORMATION_ENABLED));
      if (lakeFormationEnabled) {
        final String sessionTag = metadata.getProperties().get(GLUE_LAKEFORMATION_SESSION_TAG);
        if (StringUtils.isBlank(sessionTag)) {
          throw new IllegalArgumentException(GLUE_LAKEFORMATION_SESSION_TAG + " is required");
        }

        params.setConfigItem(FLINT_ACCELERATE_USING_COVERING_INDEX, FALSE);
        params.setConfigItem(SPARK_CATALOG_GLUE_LF_ENABLED, TRUE);
        params.setConfigItem(SPARK_CATALOG_CLIENT_FACTORY, ICEBERG_LF_CLIENT_FACTORY);
        params.setConfigItem(SPARK_CATALOG_LF_SESSION_TAG_KEY, sessionTag);
      } else {
        params.setConfigItem(SPARK_CATALOG_CLIENT_FACTORY, ICEBERG_ASSUME_ROLE_CLIENT_FACTORY);
      }
    }

    setFlintIndexStoreHost(
        params,
        parseUri(
            metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_URI), metadata.getName()));
    setFlintIndexStoreAuthProperties(
        params,
        metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH),
        () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME),
        () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD),
        () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_REGION));
    params.setConfigItem("spark.flint.datasource.name", metadata.getName());
  }

  private void setFlintIndexStoreHost(SparkSubmitParameters params, URI uri) {
    params.setConfigItem(FLINT_INDEX_STORE_HOST_KEY, uri.getHost());
    params.setConfigItem(FLINT_INDEX_STORE_PORT_KEY, String.valueOf(uri.getPort()));
    params.setConfigItem(FLINT_INDEX_STORE_SCHEME_KEY, uri.getScheme());
  }

  private void setFlintIndexStoreAuthProperties(
      SparkSubmitParameters params,
      String authType,
      Supplier<String> userName,
      Supplier<String> password,
      Supplier<String> region) {
    if (AuthenticationType.get(authType).equals(AuthenticationType.BASICAUTH)) {
      params.setConfigItem(FLINT_INDEX_STORE_AUTH_KEY, FLINT_BASIC_AUTH);
      params.setConfigItem(FLINT_INDEX_STORE_AUTH_USERNAME, userName.get());
      params.setConfigItem(FLINT_INDEX_STORE_AUTH_PASSWORD, password.get());
    } else if (AuthenticationType.get(authType).equals(AuthenticationType.AWSSIGV4AUTH)) {
      params.setConfigItem(FLINT_INDEX_STORE_AUTH_KEY, "sigv4");
      params.setConfigItem(FLINT_INDEX_STORE_AWSREGION_KEY, region.get());
    } else {
      params.setConfigItem(FLINT_INDEX_STORE_AUTH_KEY, authType);
    }
  }

  private URI parseUri(String opensearchUri, String datasourceName) {
    try {
      return new URI(opensearchUri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Bad URI in indexstore configuration of the : %s datasoure.", datasourceName));
    }
  }
}
