/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.opensearch.sql.spark.config.SparkSubmitParameterModifier;
import org.opensearch.sql.spark.rest.model.LangType;

@AllArgsConstructor
@Data
@Builder
public class DispatchQueryRequest {
  private final String accountId;
  private final String applicationId;
  private final String query;
  private final String datasource;
  private final LangType langType;
  private final String executionRoleARN;
  private final String clusterName;

  /* extension point to modify or add spark submit parameter */
  private final SparkSubmitParameterModifier sparkSubmitParameterModifier;

  /** Optional sessionId. */
  private String sessionId;
}
