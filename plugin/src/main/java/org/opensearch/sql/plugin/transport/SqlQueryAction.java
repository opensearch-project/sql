/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

/**
 * Internal action used to create a coordinator task for a SQL query. Not exposed as a public REST
 * API; it is dispatched locally from {@code RestSqlAction} via {@code NodeClient.executeLocally} so
 * the transport framework registers a {@link SqlQueryTask} for the duration of the query.
 */
public class SqlQueryAction extends ActionType<TransportSqlQueryResponse> {
  public static final String NAME = "cluster:admin/opensearch/sql";
  public static final SqlQueryAction INSTANCE = new SqlQueryAction();

  private SqlQueryAction() {
    super(NAME, TransportSqlQueryResponse::new);
  }
}
