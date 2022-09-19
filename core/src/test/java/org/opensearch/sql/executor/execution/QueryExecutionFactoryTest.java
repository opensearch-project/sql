/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.executor.QueryService;

@ExtendWith(MockitoExtension.class)
class QueryExecutionFactoryTest {

  @Mock
  private UnresolvedPlan plan;

  @Mock
  private QueryService queryService;

  @Test
  public void testCreateFromQuery() {
    Query query = new Query(plan);
    QueryExecution queryExecution = new QueryExecutionFactory(queryService).create(query);
    assertTrue(queryExecution instanceof DMLQueryExecution);
  }

  @Test
  public void testCreateFromExplain() {
    Explain query = new Explain(plan);
    QueryExecution queryExecution = new QueryExecutionFactory(queryService).create(query);
    assertTrue(queryExecution instanceof DMLQueryExecution);
  }
}
