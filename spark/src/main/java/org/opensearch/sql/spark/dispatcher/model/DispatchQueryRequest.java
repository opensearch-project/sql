/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import lombok.Data;
import org.opensearch.sql.spark.rest.model.LangType;

@Data
public class DispatchQueryRequest {
  private final String applicationId;
  private final String query;
  private final LangType langType;
  private final String executionRoleARN;
  private final String clusterName;
}
