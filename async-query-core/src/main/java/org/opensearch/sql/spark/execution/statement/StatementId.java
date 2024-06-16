/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import lombok.Data;

@Data
public class StatementId {
  private final String id;

  // construct statementId from queryId.
  public static StatementId newStatementId(String qid) {
    return new StatementId(qid);
  }

  @Override
  public String toString() {
    return "statementId=" + id;
  }
}
