/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.opensearch.sql.spark.data.constants.SparkConstants.AWS_SNAPSHOT_REPOSITORY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_CLASS_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_GLUE_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_S3_AWS_CREDENTIALS_PROVIDER_VALUE;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_CATALOG_JAR;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_CREDENTIALS_PROVIDER_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_AUTH;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_HOST;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_PORT;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DEFAULT_SCHEME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_SQL_EXTENSION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.GLUE_CATALOG_HIVE_JAR;
import static org.opensearch.sql.spark.data.constants.SparkConstants.GLUE_HIVE_CATALOG_FACTORY_CLASS;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HIVE_METASTORE_CLASS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.JAVA_HOME_LOCATION;
import static org.opensearch.sql.spark.data.constants.SparkConstants.S3_AWS_CREDENTIALS_PROVIDER_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_DRIVER_ENV_JAVA_HOME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_EXECUTOR_ENV_JAVA_HOME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JARS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_PACKAGES_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_JAR_REPOSITORIES_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_SQL_EXTENSIONS_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_STANDALONE_PACKAGE;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class S3GlueSparkSubmitParameters {

  private String className;
  private Map<String, String> config;
  public static final String SPACE = " ";
  public static final String EQUALS = "=";

  public S3GlueSparkSubmitParameters() {
    this.className = DEFAULT_CLASS_NAME;
    this.config = new LinkedHashMap<>();
    this.config.put(S3_AWS_CREDENTIALS_PROVIDER_KEY, DEFAULT_S3_AWS_CREDENTIALS_PROVIDER_VALUE);
    this.config.put(
        HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY,
        DEFAULT_GLUE_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY);
    this.config.put(SPARK_JARS_KEY, GLUE_CATALOG_HIVE_JAR + "," + FLINT_CATALOG_JAR);
    this.config.put(SPARK_JAR_PACKAGES_KEY, SPARK_STANDALONE_PACKAGE);
    this.config.put(SPARK_JAR_REPOSITORIES_KEY, AWS_SNAPSHOT_REPOSITORY);
    this.config.put(SPARK_DRIVER_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
    this.config.put(SPARK_EXECUTOR_ENV_JAVA_HOME_KEY, JAVA_HOME_LOCATION);
    this.config.put(FLINT_INDEX_STORE_HOST_KEY, FLINT_DEFAULT_HOST);
    this.config.put(FLINT_INDEX_STORE_PORT_KEY, FLINT_DEFAULT_PORT);
    this.config.put(FLINT_INDEX_STORE_SCHEME_KEY, FLINT_DEFAULT_SCHEME);
    this.config.put(FLINT_INDEX_STORE_AUTH_KEY, FLINT_DEFAULT_AUTH);
    this.config.put(FLINT_CREDENTIALS_PROVIDER_KEY, EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER);
    this.config.put(SPARK_SQL_EXTENSIONS_KEY, FLINT_SQL_EXTENSION);
    this.config.put(HIVE_METASTORE_CLASS_KEY, GLUE_HIVE_CATALOG_FACTORY_CLASS);
  }

  public void addParameter(String key, String value) {
    this.config.put(key, value);
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
