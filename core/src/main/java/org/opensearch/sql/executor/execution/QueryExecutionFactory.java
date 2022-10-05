/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.statement.CreateTable;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.statement.WriteToStream;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;

/** QueryExecution Factory. */
@RequiredArgsConstructor
public class QueryExecutionFactory extends AbstractNodeVisitor<QueryExecution, Void> {
  /** Query Service. */
  private final QueryService queryService;

  /** Create QueryExecution from Statement. */
  public QueryExecution create(Statement statement) {
    return statement.accept(this, null);
  }

  @Override
  public QueryExecution visitQuery(Query node, Void context) {
    return new DMLQueryExecution(QueryId.queryId(), node.getPlan(), false, queryService);
  }

  @Override
  public QueryExecution visitExplain(Explain node, Void context) {
    return new DMLQueryExecution(QueryId.queryId(), node.getPlan(), true, queryService);
  }

  @Override
  public QueryExecution visitCreateTable(CreateTable node, Void context) {
    return new DDLQueryExecution(
        QueryId.queryId(), node.getDataDefinitionTask(), false, queryService);
  }

  @Override
  public QueryExecution visitWriteToStream(WriteToStream node, Void context) {
    return new StreamQueryExecution(
        QueryId.queryId(), node.getPlan(), false, queryService);
  }
}
