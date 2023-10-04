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
import static org.opensearch.sql.spark.data.constants.SparkConstants.AWS_SNAPSHOT_REPOSITORY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_GLUE_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_S3_AWS_CREDENTIALS_PROVIDER_VALUE;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DRIVER_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_CATALOG_JAR;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_CREDENTIALS_PROVIDER_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_AUTH;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_HOST;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_PORT;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_SCHEME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DELEGATE_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_PASSWORD;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_USERNAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AWSREGION_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SQL_EXTENSION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.GLUE_CATALOG_HIVE_JAR;
import static org.opensearch.sql.spark.data.constants.SparkConstants.GLUE_HIVE_CATALOG_FACTORY_CLASS;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HIVE_METASTORE_CLASS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HIVE_METASTORE_GLUE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.JAVA_HOME_LOCATION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.S3_AWS_CREDENTIALS_PROVIDER_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_DRIVER_ENV_JAVA_HOME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_EXECUTOR_ENV_JAVA_HOME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JARS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_PACKAGES_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_REPOSITORIES_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_SQL_EXTENSIONS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_STANDALONE_PACKAGE;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;

/** Define Spark Submit Parameters. */
@RequiredArgsConstructor
public class SparkSubmitParameters {
  public static final String SPACE = " ";
  public static final String EQUALS = "=";

  private final String className;
  private final Map<String, String> config;

  public static class Builder {

    private final String className;
    private final Map<String, String> config;

    private Builder() {
      className = DEFAULT_CLASS_NAME;
      config = new LinkedHashMap<>();

      config.put(S3_AWS_CREDENTIALS_PROVIDER_KEY, DEFAULT_S3_AWS_CREDENTIALS_PROVIDER_VALUE);
      config.put(
          HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY,
          DEFAULT_GLUE_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY);
      config.put(SPARK_JARS_KEY, GLUE_CATALOG_HIVE_JAR + "," + FLINT_CATALOG_JAR);
      config.put(SPARK_JAR_PACKAGES_KEY, SPARK_STANDALONE_PACKAGE);
      config.put(SPARK_JAR_REPOSITORIES_KEY, AWS_SNAPSHOT_REPOSITORY);
      config.put(SPARK_DRIVER_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
      config.put(SPARK_EXECUTOR_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
      config.put(FLINT_INDEX_STORE_HOST_KEY, FLINT_DEFAULT_HOST);
      config.put(FLINT_INDEX_STORE_PORT_KEY, FLINT_DEFAULT_PORT);
      config.put(FLINT_INDEX_STORE_SCHEME_KEY, FLINT_DEFAULT_SCHEME);
      config.put(FLINT_INDEX_STORE_AUTH_KEY, FLINT_DEFAULT_AUTH);
      config.put(FLINT_CREDENTIALS_PROVIDER_KEY, EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER);
      config.put(SPARK_SQL_EXTENSIONS_KEY, FLINT_SQL_EXTENSION);
      config.put(HIVE_METASTORE_CLASS_KEY, GLUE_HIVE_CATALOG_FACTORY_CLASS);
    }

    public static Builder builder() {
      return new Builder();
    }

    public Builder dataSource(DataSourceMetadata metadata) {
      if (DataSourceType.S3GLUE.equals(metadata.getConnector())) {
        String roleArn = metadata.getProperties().get(GLUE_ROLE_ARN);

        config.put(DRIVER_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
        config.put(EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
        config.put(HIVE_METASTORE_GLUE_ARN_KEY, roleArn);
        config.put("spark.sql.catalog." + metadata.getName(), FLINT_DELEGATE_CATALOG);

        setFlintIndexStoreHost(
            parseUri(
                metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_URI), metadata.getName()));
        setFlintIndexStoreAuthProperties(
            metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH),
            () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME),
            () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD),
            () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_REGION));
        return this;
      }
      throw new UnsupportedOperationException(
          String.format(
              "UnSupported datasource type for async queries:: %s", metadata.getConnector()));
    }

    private void setFlintIndexStoreHost(URI uri) {
      config.put(FLINT_INDEX_STORE_HOST_KEY, uri.getHost());
      config.put(FLINT_INDEX_STORE_PORT_KEY, String.valueOf(uri.getPort()));
      config.put(FLINT_INDEX_STORE_SCHEME_KEY, uri.getScheme());
    }

    private void setFlintIndexStoreAuthProperties(
        String authType,
        Supplier<String> userName,
        Supplier<String> password,
        Supplier<String> region) {
      if (AuthenticationType.get(authType).equals(AuthenticationType.BASICAUTH)) {
        config.put(FLINT_INDEX_STORE_AUTH_KEY, authType);
        config.put(FLINT_INDEX_STORE_AUTH_USERNAME, userName.get());
        config.put(FLINT_INDEX_STORE_AUTH_PASSWORD, password.get());
      } else if (AuthenticationType.get(authType).equals(AuthenticationType.AWSSIGV4AUTH)) {
        config.put(FLINT_INDEX_STORE_AUTH_KEY, "sigv4");
        config.put(FLINT_INDEX_STORE_AWSREGION_KEY, region.get());
      } else {
        config.put(FLINT_INDEX_STORE_AUTH_KEY, authType);
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

    public Builder structuredStreaming(Boolean isStructuredStreaming) {
      if (isStructuredStreaming) {
        config.put("spark.flint.job.type", "streaming");
      }
      return this;
    }

    public SparkSubmitParameters build() {
      return new SparkSubmitParameters(className, config);
    }
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(" --class ");
    stringBuilder.append(this.className);
    stringBuilder.append(SPACE);
    for (String key : config.keySet()) {
      stringBuilder.append(" --conf ");
      stringBuilder.append(key);
      stringBuilder.append(EQUALS);
      stringBuilder.append(config.get(key));
      stringBuilder.append(SPACE);
    }
    return stringBuilder.toString();
  }
}
