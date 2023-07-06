/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.data.constants;

public class SparkConstants {
  public static final String EMR = "emr";
  public static final String STEP_ID_FIELD = "stepId.keyword";
  public static String SPARK_SQL_APPLICATION_JAR = "s3://spark-datasource/sql-job.jar";
  public static final String SPARK_INDEX_NAME = ".query_execution_result";
  public static String FLINT_INTEGRATION_JAR =
      "s3://spark-datasource/flint-spark-integration-assembly-0.1.0-SNAPSHOT.jar";
  public static final String FLINT_DEFAULT_HOST = "localhost";
  public static final String FLINT_DEFAULT_PORT = "9200";
  public static final String FLINT_DEFAULT_SCHEME = "http";
  public static final String FLINT_DEFAULT_AUTH = "-1";
  public static final String FLINT_DEFAULT_REGION = "us-west-2";
}
