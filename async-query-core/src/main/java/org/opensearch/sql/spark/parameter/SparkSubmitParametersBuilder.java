/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import static org.opensearch.sql.spark.data.constants.SparkConstants.AWS_SNAPSHOT_REPOSITORY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_GLUE_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_S3_AWS_CREDENTIALS_PROVIDER_VALUE;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_CREDENTIALS_PROVIDER_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_AUTH;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_CLUSTER_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_HOST;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_PORT;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_SCHEME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_JOB_QUERY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_JOB_REQUEST_INDEX;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_JOB_SESSION_ID;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_PPL_EXTENSION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SQL_EXTENSION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.GLUE_HIVE_CATALOG_FACTORY_CLASS;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HIVE_METASTORE_CLASS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_GLUE_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_SESSION_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_SPARK_EXTENSION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.ICEBERG_SPARK_RUNTIME_PACKAGE;
import static org.opensearch.sql.spark.data.constants.SparkConstants.JAVA_HOME_LOCATION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.PPL_STANDALONE_PACKAGE;
import static org.opensearch.sql.spark.data.constants.SparkConstants.S3_AWS_CREDENTIALS_PROVIDER_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_CATALOG_CATALOG_IMPL;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_DRIVER_ENV_FLINT_CLUSTER_NAME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_DRIVER_ENV_JAVA_HOME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_EXECUTOR_ENV_FLINT_CLUSTER_NAME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_EXECUTOR_ENV_JAVA_HOME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JARS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_PACKAGES_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_REPOSITORIES_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_LAUNCHER_PACKAGE;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_SQL_EXTENSIONS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_STANDALONE_PACKAGE;

import lombok.Getter;
import org.apache.commons.text.StringEscapeUtils;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.config.SparkSubmitParameterModifier;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil;

public class SparkSubmitParametersBuilder {
  private final SparkParameterComposerCollection sparkParameterComposerCollection;
  @Getter private final SparkSubmitParameters sparkSubmitParameters;

  public SparkSubmitParametersBuilder(
      SparkParameterComposerCollection sparkParameterComposerCollection) {
    this.sparkParameterComposerCollection = sparkParameterComposerCollection;
    sparkSubmitParameters = new SparkSubmitParameters();
    setDefaultConfigs();
  }

  private void setDefaultConfigs() {
    setConfigItem(S3_AWS_CREDENTIALS_PROVIDER_KEY, DEFAULT_S3_AWS_CREDENTIALS_PROVIDER_VALUE);
    setConfigItem(
        HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY,
        DEFAULT_GLUE_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY);
    setConfigItem(SPARK_JARS_KEY, ICEBERG_SPARK_RUNTIME_PACKAGE);
    setConfigItem(
        SPARK_JAR_PACKAGES_KEY,
        SPARK_STANDALONE_PACKAGE + "," + SPARK_LAUNCHER_PACKAGE + "," + PPL_STANDALONE_PACKAGE);
    setConfigItem(SPARK_JAR_REPOSITORIES_KEY, AWS_SNAPSHOT_REPOSITORY);
    setConfigItem(SPARK_DRIVER_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
    setConfigItem(SPARK_EXECUTOR_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
    setConfigItem(SPARK_DRIVER_ENV_FLINT_CLUSTER_NAME_KEY, FLINT_DEFAULT_CLUSTER_NAME);
    setConfigItem(SPARK_EXECUTOR_ENV_FLINT_CLUSTER_NAME_KEY, FLINT_DEFAULT_CLUSTER_NAME);
    setConfigItem(FLINT_INDEX_STORE_HOST_KEY, FLINT_DEFAULT_HOST);
    setConfigItem(FLINT_INDEX_STORE_PORT_KEY, FLINT_DEFAULT_PORT);
    setConfigItem(FLINT_INDEX_STORE_SCHEME_KEY, FLINT_DEFAULT_SCHEME);
    setConfigItem(FLINT_INDEX_STORE_AUTH_KEY, FLINT_DEFAULT_AUTH);
    setConfigItem(FLINT_CREDENTIALS_PROVIDER_KEY, EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER);
    setConfigItem(
        SPARK_SQL_EXTENSIONS_KEY,
        ICEBERG_SPARK_EXTENSION + "," + FLINT_SQL_EXTENSION + "," + FLINT_PPL_EXTENSION);
    setConfigItem(HIVE_METASTORE_CLASS_KEY, GLUE_HIVE_CATALOG_FACTORY_CLASS);
    setConfigItem(SPARK_CATALOG, ICEBERG_SESSION_CATALOG);
    setConfigItem(SPARK_CATALOG_CATALOG_IMPL, ICEBERG_GLUE_CATALOG);
  }

  private void setConfigItem(String key, String value) {
    sparkSubmitParameters.setConfigItem(key, value);
  }

  public SparkSubmitParametersBuilder className(String className) {
    sparkSubmitParameters.setClassName(className);
    return this;
  }

  /** clusterName will be used for logging and metrics in Spark */
  public SparkSubmitParametersBuilder clusterName(String clusterName) {
    setConfigItem(SPARK_DRIVER_ENV_FLINT_CLUSTER_NAME_KEY, clusterName);
    setConfigItem(SPARK_EXECUTOR_ENV_FLINT_CLUSTER_NAME_KEY, clusterName);
    return this;
  }

  /**
   * For query in spark submit parameters to be parsed correctly, escape the characters in the
   * query, then wrap the query with double quotes.
   */
  public SparkSubmitParametersBuilder query(String query) {
    String escapedQuery = StringEscapeUtils.escapeJava(query);
    String wrappedQuery = "\"" + escapedQuery + "\"";
    setConfigItem(FLINT_JOB_QUERY, wrappedQuery);
    return this;
  }

  public SparkSubmitParametersBuilder dataSource(
      DataSourceMetadata metadata,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context) {
    if (sparkParameterComposerCollection.isComposerRegistered(metadata.getConnector())) {
      sparkParameterComposerCollection.composeByDataSource(
          metadata, sparkSubmitParameters, dispatchQueryRequest, context);
      return this;
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "UnSupported datasource type for async queries:: %s", metadata.getConnector()));
    }
  }

  public SparkSubmitParametersBuilder structuredStreaming(Boolean isStructuredStreaming) {
    if (isStructuredStreaming) {
      setConfigItem("spark.flint.job.type", "streaming");
    }
    return this;
  }

  public SparkSubmitParametersBuilder extraParameters(String params) {
    sparkSubmitParameters.setExtraParameters(params);
    return this;
  }

  public SparkSubmitParametersBuilder sessionExecution(String sessionId, String datasourceName) {
    setConfigItem(FLINT_JOB_REQUEST_INDEX, OpenSearchStateStoreUtil.getIndexName(datasourceName));
    setConfigItem(FLINT_JOB_SESSION_ID, sessionId);
    return this;
  }

  public SparkSubmitParametersBuilder acceptModifier(SparkSubmitParameterModifier modifier) {
    if (modifier != null) {
      modifier.modifyParameters(this);
    }
    return this;
  }

  public SparkSubmitParametersBuilder acceptComposers(
      DispatchQueryRequest dispatchQueryRequest, AsyncQueryRequestContext context) {
    sparkParameterComposerCollection.compose(sparkSubmitParameters, dispatchQueryRequest, context);
    return this;
  }

  @Override
  public String toString() {
    return sparkSubmitParameters.toString();
  }
}
