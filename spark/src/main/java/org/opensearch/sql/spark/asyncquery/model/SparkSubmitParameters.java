/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_REGION;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_INDEX_STORE_OPENSEARCH_URI;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_ROLE_ARN;
import static org.opensearch.sql.spark.data.constants.SparkConstants.*;
import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;

/** Defines the parameters required for Spark submit command construction. */
@AllArgsConstructor
@RequiredArgsConstructor
public class SparkSubmitParameters {
  private static final String SPACE = " ";
  private static final String EQUALS = "=";
  private static final String FLINT_BASIC_AUTH = "basic";

  private final String className;
  private final Map<String, String> config;

  /** Extra parameters to append finally */
  private String extraParameters;

  public static class Builder {
    private String className;
    private final Map<String, String> config = new LinkedHashMap<>();
    private String extraParameters;

    private Builder() {
      initializeDefaultConfigurations();
    }

    public static Builder builder() {
      return new Builder();
    }

    public Builder className(String className) {
      this.className = className;
      return this;
    }

    public Builder clusterName(String clusterName) {
      config.put(SPARK_DRIVER_ENV_FLINT_CLUSTER_NAME_KEY, clusterName);
      config.put(SPARK_EXECUTOR_ENV_FLINT_CLUSTER_NAME_KEY, clusterName);
      return this;
    }

    public Builder dataSource(DataSourceMetadata metadata) {
      if (!DataSourceType.S3GLUE.equals(metadata.getConnector())) {
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported datasource type for async queries: %s", metadata.getConnector()));
      }

      configureDataSource(metadata);
      return this;
    }

    public Builder extraParameters(String params) {
      this.extraParameters = params;
      return this;
    }

    public Builder query(String query) {
      config.put(FLINT_JOB_QUERY, query);
      return this;
    }

    public Builder sessionExecution(String sessionId, String datasourceName) {
      config.put(FLINT_JOB_REQUEST_INDEX, DATASOURCE_TO_REQUEST_INDEX.apply(datasourceName));
      config.put(FLINT_JOB_SESSION_ID, sessionId);
      return this;
    }

    public Builder structuredStreaming(Boolean isStructuredStreaming) {
      if (Boolean.TRUE.equals(isStructuredStreaming)) {
        config.put("spark.flint.job.type", "streaming");
      }
      return this;
    }

    public SparkSubmitParameters build() {
      return new SparkSubmitParameters(className, config, extraParameters);
    }

    private void configureDataSource(DataSourceMetadata metadata) {
      // DataSource specific configuration
      String roleArn = metadata.getProperties().get(GLUE_ROLE_ARN);

      config.put(DRIVER_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
      config.put(EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
      config.put(HIVE_METASTORE_GLUE_ARN_KEY, roleArn);
      config.put("spark.sql.catalog." + metadata.getName(), FLINT_DELEGATE_CATALOG);
      config.put(FLINT_DATA_SOURCE_KEY, metadata.getName());

      URI uri =
          parseUri(
              metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_URI), metadata.getName());
      config.put(FLINT_INDEX_STORE_HOST_KEY, uri.getHost());
      config.put(FLINT_INDEX_STORE_PORT_KEY, String.valueOf(uri.getPort()));
      config.put(FLINT_INDEX_STORE_SCHEME_KEY, uri.getScheme());

      setFlintIndexStoreAuthProperties(
          metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH),
          () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME),
          () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD),
          () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_REGION));
      config.put("spark.flint.datasource.name", metadata.getName());
    }

    private void initializeDefaultConfigurations() {
      className = DEFAULT_CLASS_NAME;
      // Default configurations initialization
      config.put(S3_AWS_CREDENTIALS_PROVIDER_KEY, DEFAULT_S3_AWS_CREDENTIALS_PROVIDER_VALUE);
      config.put(
          HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY,
          DEFAULT_GLUE_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY);
      config.put(
          SPARK_JAR_PACKAGES_KEY,
          SPARK_STANDALONE_PACKAGE + "," + SPARK_LAUNCHER_PACKAGE + "," + PPL_STANDALONE_PACKAGE);
      config.put(SPARK_JAR_REPOSITORIES_KEY, AWS_SNAPSHOT_REPOSITORY);
      config.put(SPARK_DRIVER_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
      config.put(SPARK_EXECUTOR_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
      config.put(SPARK_DRIVER_ENV_FLINT_CLUSTER_NAME_KEY, FLINT_DEFAULT_CLUSTER_NAME);
      config.put(SPARK_EXECUTOR_ENV_FLINT_CLUSTER_NAME_KEY, FLINT_DEFAULT_CLUSTER_NAME);
      config.put(FLINT_INDEX_STORE_HOST_KEY, FLINT_DEFAULT_HOST);
      config.put(FLINT_INDEX_STORE_PORT_KEY, FLINT_DEFAULT_PORT);
      config.put(FLINT_INDEX_STORE_SCHEME_KEY, FLINT_DEFAULT_SCHEME);
      config.put(FLINT_INDEX_STORE_AUTH_KEY, FLINT_DEFAULT_AUTH);
      config.put(FLINT_CREDENTIALS_PROVIDER_KEY, EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER);
      config.put(SPARK_SQL_EXTENSIONS_KEY, FLINT_SQL_EXTENSION + "," + FLINT_PPL_EXTENSION);
      config.put(HIVE_METASTORE_CLASS_KEY, GLUE_HIVE_CATALOG_FACTORY_CLASS);
    }

    private URI parseUri(String opensearchUri, String datasourceName) {
      try {
        return new URI(opensearchUri);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(
            String.format(
                "Bad URI in indexstore configuration for datasource: %s.", datasourceName),
            e);
      }
    }

    private void setFlintIndexStoreAuthProperties(
        String authType,
        Supplier<String> userName,
        Supplier<String> password,
        Supplier<String> region) {
      if (AuthenticationType.get(authType).equals(AuthenticationType.BASICAUTH)) {
        config.put(FLINT_INDEX_STORE_AUTH_KEY, FLINT_BASIC_AUTH);
        config.put(FLINT_INDEX_STORE_AUTH_USERNAME, userName.get());
        config.put(FLINT_INDEX_STORE_AUTH_PASSWORD, password.get());
      } else if (AuthenticationType.get(authType).equals(AuthenticationType.AWSSIGV4AUTH)) {
        config.put(FLINT_INDEX_STORE_AUTH_KEY, "sigv4");
        config.put(FLINT_INDEX_STORE_AWSREGION_KEY, region.get());
      } else {
        config.put(FLINT_INDEX_STORE_AUTH_KEY, authType);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder(" --class ").append(className).append(SPACE);
    config.forEach(
        (key, value) ->
            stringBuilder
                .append(" --conf ")
                .append(key)
                .append(EQUALS)
                .append(value)
                .append(SPACE));
    if (extraParameters != null) stringBuilder.append(extraParameters);
    return stringBuilder.toString();
  }
}
