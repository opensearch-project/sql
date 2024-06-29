/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_REGION;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_URI;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_ROLE_ARN;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DRIVER_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EMR_LAKEFORMATION_OPTION;
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
import static org.opensearch.sql.spark.data.constants.SparkConstants.HIVE_METASTORE_GLUE_ARN_KEY;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Supplier;
import org.apache.commons.lang3.BooleanUtils;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

public class S3GlueDataSourceSparkParameterComposer implements DataSourceSparkParameterComposer {
  public static final String FLINT_BASIC_AUTH = "basic";

  @Override
  public void compose(
      DataSourceMetadata metadata,
      SparkSubmitParameters params,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context) {
    String roleArn = metadata.getProperties().get(GLUE_ROLE_ARN);

    params.setConfigItem(DRIVER_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
    params.setConfigItem(EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
    params.setConfigItem(HIVE_METASTORE_GLUE_ARN_KEY, roleArn);
    params.setConfigItem("spark.sql.catalog." + metadata.getName(), FLINT_DELEGATE_CATALOG);
    params.setConfigItem(FLINT_DATA_SOURCE_KEY, metadata.getName());

    final boolean lakeFormationEnabled =
        BooleanUtils.toBoolean(metadata.getProperties().get(GLUE_LAKEFORMATION_ENABLED));
    params.setConfigItem(EMR_LAKEFORMATION_OPTION, Boolean.toString(lakeFormationEnabled));
    params.setConfigItem(
        FLINT_ACCELERATE_USING_COVERING_INDEX, Boolean.toString(!lakeFormationEnabled));

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
