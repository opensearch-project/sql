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
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_LAKEFORMATION_ENABLED;
import static org.opensearch.sql.datasources.glue.GlueDataSourceFactory.GLUE_ROLE_ARN;
import static org.opensearch.sql.spark.data.constants.SparkConstants.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.spark.config.SparkSubmitParameterModifier;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil;

/** Define Spark Submit Parameters. */
@AllArgsConstructor
@RequiredArgsConstructor
public class SparkSubmitParameters {
  public static final String SPACE = " ";
  public static final String EQUALS = "=";
  public static final String FLINT_BASIC_AUTH = "basic";

  private final String className;
  private final Map<String, String> config;

  /** Extra parameters to append finally */
  @Setter private String extraParameters;

  public void setConfigItem(String key, String value) {
    config.put(key, value);
  }

  public void deleteConfigItem(String key) {
    config.remove(key);
  }

  public static Builder builder() {
    return Builder.builder();
  }

  public SparkSubmitParameters acceptModifier(SparkSubmitParameterModifier modifier) {
    modifier.modifyParameters(this);
    return this;
  }

  public static class Builder {

    private String className;
    private final Map<String, String> config;
    private String extraParameters;

    private Builder() {
      className = DEFAULT_CLASS_NAME;
      config = new LinkedHashMap<>();

      config.put(S3_AWS_CREDENTIALS_PROVIDER_KEY, DEFAULT_S3_AWS_CREDENTIALS_PROVIDER_VALUE);
      config.put(
          HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY,
          DEFAULT_GLUE_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY);
      config.put(SPARK_JARS_KEY, ICEBERG_SPARK_RUNTIME_PACKAGE);
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
      config.put(
          SPARK_SQL_EXTENSIONS_KEY,
          ICEBERG_SPARK_EXTENSION + "," + FLINT_SQL_EXTENSION + "," + FLINT_PPL_EXTENSION);
      config.put(HIVE_METASTORE_CLASS_KEY, GLUE_HIVE_CATALOG_FACTORY_CLASS);
      config.put(SPARK_CATALOG, ICEBERG_SESSION_CATALOG);
      config.put(SPARK_CATALOG_CATALOG_IMPL, ICEBERG_GLUE_CATALOG);
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

    /**
     * For query in spark submit parameters to be parsed correctly, escape the characters in the
     * query, then wrap the query with double quotes.
     */
    public Builder query(String query) {
      String escapedQuery = StringEscapeUtils.escapeJava(query);
      String wrappedQuery = "\"" + escapedQuery + "\"";
      config.put(FLINT_JOB_QUERY, wrappedQuery);
      return this;
    }

    public Builder dataSource(DataSourceMetadata metadata) {
      if (DataSourceType.S3GLUE.equals(metadata.getConnector())) {
        String roleArn = metadata.getProperties().get(GLUE_ROLE_ARN);

        config.put(DRIVER_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
        config.put(EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY, roleArn);
        config.put(HIVE_METASTORE_GLUE_ARN_KEY, roleArn);
        config.put("spark.sql.catalog." + metadata.getName(), FLINT_DELEGATE_CATALOG);
        config.put(FLINT_DATA_SOURCE_KEY, metadata.getName());

        final boolean lakeFormationEnabled =
            BooleanUtils.toBoolean(metadata.getProperties().get(GLUE_LAKEFORMATION_ENABLED));
        config.put(EMR_LAKEFORMATION_OPTION, Boolean.toString(lakeFormationEnabled));
        config.put(FLINT_ACCELERATE_USING_COVERING_INDEX, Boolean.toString(!lakeFormationEnabled));

        setFlintIndexStoreHost(
            parseUri(
                metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_URI), metadata.getName()));
        setFlintIndexStoreAuthProperties(
            metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH),
            () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_USERNAME),
            () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_AUTH_PASSWORD),
            () -> metadata.getProperties().get(GLUE_INDEX_STORE_OPENSEARCH_REGION));
        config.put("spark.flint.datasource.name", metadata.getName());
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

    public Builder extraParameters(String params) {
      extraParameters = params;
      return this;
    }

    public SparkSubmitParameters build() {
      return new SparkSubmitParameters(className, config, extraParameters);
    }
  }

  public void sessionExecution(String sessionId, String datasourceName) {
    config.put(FLINT_JOB_REQUEST_INDEX, OpenSearchStateStoreUtil.getIndexName(datasourceName));
    config.put(FLINT_JOB_SESSION_ID, sessionId);
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

    if (extraParameters != null) {
      stringBuilder.append(extraParameters);
    }
    return stringBuilder.toString();
  }
}
