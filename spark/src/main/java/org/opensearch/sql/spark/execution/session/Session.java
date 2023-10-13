/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.util.Optional;
import org.opensearch.sql.spark.execution.statement.QueryRequest;
import org.opensearch.sql.spark.execution.statement.Statement;
import org.opensearch.sql.spark.execution.statement.StatementId;

/** Session define the statement execution context. Each session is binding to one Spark Job. */
public interface Session {
  /** open session. */
  void open();

  /** close session. */
  void close();

  /**
   * submit {@link QueryRequest}.
   *
   * @param request {@link QueryRequest}
   * @return {@link StatementId}
   */
  StatementId submit(QueryRequest request);

  /**
   * get {@link Statement}.
   *
   * @param stID {@link StatementId}
   * @return {@link Statement}
   */
  Optional<Statement> get(StatementId stID);

  SessionModel getSessionModel();

  SessionId getSessionId();
}
