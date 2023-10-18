/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.rest.model.LangType;

@AllArgsConstructor
@Data
@RequiredArgsConstructor // required explicitly
public class DispatchQueryRequest {
  private final String applicationId;
  private final String query;
  private final String datasource;
  private final LangType langType;
  private final String executionRoleARN;
  private final String clusterName;

  /** Optional extra Spark submit parameters to include in final request */
  private String extraSparkSubmitParams;

  /** Optional sessionId. */
  private String sessionId;
}
