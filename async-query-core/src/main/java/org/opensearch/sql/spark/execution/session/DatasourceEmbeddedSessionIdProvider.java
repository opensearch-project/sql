/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import org.opensearch.sql.spark.utils.IDUtils;

public class DatasourceEmbeddedSessionIdProvider implements SessionIdProvider {

  @Override
  public String getSessionId(CreateSessionRequest createSessionRequest) {
    return IDUtils.encode(createSessionRequest.getDatasourceName());
  }
}
