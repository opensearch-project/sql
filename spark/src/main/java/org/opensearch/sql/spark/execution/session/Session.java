/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

/** Session define the statement execution context. Each session is binding to one Spark Job. */
public interface Session {
  /** open session. */
  void open();

  /** close session. */
  void close();

  SessionModel getSessionModel();

  SessionId getSessionId();
}
