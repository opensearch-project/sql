/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.data.constants;

public class SparkConstants {
  public static final String EMR = "emr";
  public static final String STEP_ID_FIELD = "stepId.keyword";

  public static final String JOB_ID_FIELD = "jobRunId";

  public static final String STATUS_FIELD = "status";

  public static final String DATA_FIELD = "data";

  public static final String ERROR_FIELD = "error";

  // EMR-S will download JAR to local maven
  public static final String SPARK_SQL_APPLICATION_JAR =
      "file:///home/hadoop/.ivy2/jars/org.opensearch_opensearch-spark-sql-application_2.12-0.3.0-SNAPSHOT.jar";
  public static final String SPARK_REQUEST_BUFFER_INDEX_NAME = ".query_execution_request";
  // TODO should be replaced with mvn jar.
  public static final String FLINT_INTEGRATION_JAR =
      "s3://spark-datasource/flint-spark-integration-assembly-0.3.0-SNAPSHOT.jar";
  // TODO should be replaced with mvn jar.
  public static final String FLINT_DEFAULT_CLUSTER_NAME = "opensearch-cluster";
  public static final String FLINT_DEFAULT_HOST = "localhost";
  public static final String FLINT_DEFAULT_PORT = "9200";
  public static final String FLINT_DEFAULT_SCHEME = "http";
  public static final String FLINT_DEFAULT_AUTH = "noauth";
  public static final String FLINT_DEFAULT_REGION = "us-west-2";
  public static final String DEFAULT_CLASS_NAME = "org.apache.spark.sql.FlintJob";
  public static final String S3_AWS_CREDENTIALS_PROVIDER_KEY =
      "spark.hadoop.fs.s3.customAWSCredentialsProvider";
  public static final String DRIVER_ENV_ASSUME_ROLE_ARN_KEY =
      "spark.emr-serverless.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN";
  public static final String EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY =
      "spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN";
  public static final String HADOOP_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY =
      "spark.hadoop.aws.catalog.credentials.provider.factory.class";
  public static final String HIVE_METASTORE_GLUE_ARN_KEY = "spark.hive.metastore.glue.role.arn";
  public static final String SPARK_JARS_KEY = "spark.jars";
  public static final String SPARK_JAR_PACKAGES_KEY = "spark.jars.packages";
  public static final String SPARK_JAR_REPOSITORIES_KEY = "spark.jars.repositories";
  public static final String SPARK_DRIVER_ENV_JAVA_HOME_KEY =
      "spark.emr-serverless.driverEnv.JAVA_HOME";
  public static final String SPARK_EXECUTOR_ENV_JAVA_HOME_KEY = "spark.executorEnv.JAVA_HOME";
  // Used for logging/metrics in Spark (driver)
  public static final String SPARK_DRIVER_ENV_FLINT_CLUSTER_NAME_KEY =
      "spark.emr-serverless.driverEnv.FLINT_CLUSTER_NAME";
  // Used for logging/metrics in Spark (executor)
  public static final String SPARK_EXECUTOR_ENV_FLINT_CLUSTER_NAME_KEY =
      "spark.executorEnv.FLINT_CLUSTER_NAME";
  public static final String FLINT_INDEX_STORE_HOST_KEY = "spark.datasource.flint.host";
  public static final String FLINT_INDEX_STORE_PORT_KEY = "spark.datasource.flint.port";
  public static final String FLINT_INDEX_STORE_SCHEME_KEY = "spark.datasource.flint.scheme";
  public static final String FLINT_INDEX_STORE_AUTH_KEY = "spark.datasource.flint.auth";
  public static final String FLINT_INDEX_STORE_AUTH_USERNAME =
      "spark.datasource.flint.auth.username";
  public static final String FLINT_INDEX_STORE_AUTH_PASSWORD =
      "spark.datasource.flint.auth.password";
  public static final String FLINT_INDEX_STORE_AWSREGION_KEY = "spark.datasource.flint.region";
  public static final String FLINT_CREDENTIALS_PROVIDER_KEY =
      "spark.datasource.flint.customAWSCredentialsProvider";
  public static final String FLINT_DATA_SOURCE_KEY = "spark.flint.datasource.name";
  public static final String SPARK_SQL_EXTENSIONS_KEY = "spark.sql.extensions";
  public static final String HIVE_METASTORE_CLASS_KEY =
      "spark.hadoop.hive.metastore.client.factory.class";
  public static final String DEFAULT_S3_AWS_CREDENTIALS_PROVIDER_VALUE =
      "com.amazonaws.emr.AssumeRoleAWSCredentialsProvider";
  public static final String DEFAULT_GLUE_CATALOG_CREDENTIALS_PROVIDER_FACTORY_KEY =
      "com.amazonaws.glue.catalog.metastore.STSAssumeRoleSessionCredentialsProviderFactory";
  public static final String SPARK_STANDALONE_PACKAGE =
      "org.opensearch:opensearch-spark-standalone_2.12:0.3.0-SNAPSHOT";
  public static final String SPARK_LAUNCHER_PACKAGE =
      "org.opensearch:opensearch-spark-sql-application_2.12:0.3.0-SNAPSHOT";
  public static final String PPL_STANDALONE_PACKAGE =
      "org.opensearch:opensearch-spark-ppl_2.12:0.3.0-SNAPSHOT";
  public static final String AWS_SNAPSHOT_REPOSITORY =
      "https://aws.oss.sonatype.org/content/repositories/snapshots";
  public static final String GLUE_HIVE_CATALOG_FACTORY_CLASS =
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory";
  public static final String FLINT_DELEGATE_CATALOG =
      "org.opensearch.sql.FlintDelegatingSessionCatalog";
  public static final String FLINT_SQL_EXTENSION =
      "org.opensearch.flint.spark.FlintSparkExtensions";
  public static final String FLINT_PPL_EXTENSION =
      "org.opensearch.flint.spark.FlintPPLSparkExtensions";

  public static final String EMR_ASSUME_ROLE_CREDENTIALS_PROVIDER =
      "com.amazonaws.emr.AssumeRoleAWSCredentialsProvider";
  public static final String JAVA_HOME_LOCATION = "/usr/lib/jvm/java-17-amazon-corretto.x86_64/";
  public static final String FLINT_JOB_QUERY = "spark.flint.job.query";
  public static final String FLINT_JOB_REQUEST_INDEX = "spark.flint.job.requestIndex";
  public static final String FLINT_JOB_SESSION_ID = "spark.flint.job.sessionId";

  public static final String FLINT_SESSION_CLASS_NAME = "org.apache.spark.sql.FlintREPL";

  public static final String SPARK_CATALOG = "spark.sql.catalog.spark_catalog";
  public static final String ICEBERG_SESSION_CATALOG =
      "org.apache.iceberg.spark.SparkSessionCatalog";
  public static final String ICEBERG_SPARK_EXTENSION =
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions";
  public static final String ICEBERG_SPARK_RUNTIME_PACKAGE =
      "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar";
  public static final String SPARK_CATALOG_CATALOG_IMPL =
      "spark.sql.catalog.spark_catalog.catalog-impl";
  public static final String ICEBERG_GLUE_CATALOG = "org.apache.iceberg.aws.glue.GlueCatalog";

  public static final String EMR_LAKEFORMATION_OPTION =
      "spark.emr-serverless.lakeformation.enabled";
  public static final String FLINT_ACCELERATE_USING_COVERING_INDEX =
      "spark.flint.optimizer.covering.enabled";
}
