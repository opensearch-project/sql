/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import lombok.Data;
import org.opensearch.sql.spark.client.StartJobRequest;

@Data
public class CreateSessionRequest {
  private final StartJobRequest startJobRequest;
  private final String datasourceName;
}
