/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.constants;

import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;

public class TestConstants {
  public static final String QUERY = "select 1";
  public static final String TEST_DATASOURCE_NAME = "test_datasource_name";
  public static final String EMR_CLUSTER_ID = "j-123456789";
  public static final String EMR_JOB_ID = "job-123xxx";
  public static final String EMRS_APPLICATION_ID = "app-xxxxx";
  public static final String EMRS_EXECUTION_ROLE = "execution_role";
  public static final String EMRS_DATASOURCE_ROLE = "datasource_role";
  public static final String EMRS_JOB_NAME = "job_name";
  public static final String SPARK_SUBMIT_PARAMETERS = "--conf org.flint.sql.SQLJob";
  public static final String TEST_CLUSTER_NAME = "TEST_CLUSTER";
  public static final String MOCK_SESSION_ID = "s-0123456";
  public static final String ENTRY_POINT_START_JAR =
      "file:///home/hadoop/.ivy2/jars/org.opensearch_opensearch-spark-sql-application_2.12-0.1.0-SNAPSHOT.jar";
  public static final String DEFAULT_RESULT_INDEX = "query_execution_result_ds1";
  public static final String MOCK_STATEMENT_ID =
      AsyncQueryId.newAsyncQueryId(TEST_DATASOURCE_NAME).getId();
}
