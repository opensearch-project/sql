/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.xcontent;

import lombok.experimental.UtilityClass;

@UtilityClass
public class XContentCommonAttributes {
  public static final String VERSION = "version";
  public static final String VERSION_1_0 = "1.0";
  public static final String TYPE = "type";
  public static final String QUERY_ID = "queryId";
  public static final String STATE = "state";
  public static final String LAST_UPDATE_TIME = "lastUpdateTime";
  public static final String APPLICATION_ID = "applicationId";
  public static final String DATASOURCE_NAME = "dataSourceName";
  public static final String JOB_ID = "jobId";
  public static final String ERROR = "error";
}
