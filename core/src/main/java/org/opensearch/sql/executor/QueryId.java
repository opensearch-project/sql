/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor;

import lombok.Getter;
import org.apache.commons.lang3.RandomStringUtils;
import org.opensearch.sql.executor.execution.AbstractPlan;

/**
 * Query id of {@link AbstractPlan}.
 */
public class QueryId {
  /**
   * Query id.
   */
  @Getter
  private final String queryId;

  /**
   * Generate {@link QueryId}.
   * @return {@link QueryId}.
   */
  public static QueryId queryId() {
    return new QueryId(RandomStringUtils.random(10, true, true));
  }

  private QueryId(String queryId) {
    this.queryId = queryId;
  }
}
