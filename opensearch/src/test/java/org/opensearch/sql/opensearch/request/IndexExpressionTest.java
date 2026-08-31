/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.request.IndexPruner.IndexExpression;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;

class IndexExpressionTest {

  private static IndexExpression expression(String indexName) {
    // Null node client: every predicate under test reads only the expression text.
    return new IndexExpression(new IndexName(indexName), null);
  }

  @Test
  void concreteNameIsNotWildcard() {
    assertFalse(expression("logs-2024").hasWildcard());
  }

  @Test
  void starIsWildcard() {
    assertTrue(expression("logs-*").hasWildcard());
  }

  @Test
  void questionMarkIsNotWildcard() {
    // OpenSearch resolves index expressions with Glob.globMatch, which honours only '*', so
    // "logs-202?" names one literal index. Treating it as a pattern would prune a concrete name.
    assertFalse(expression("logs-202?").hasWildcard());
  }

  @Test
  void oneWildcardAmongConcreteNamesIsWildcard() {
    assertTrue(expression("logs-2024,logs-*").hasWildcard());
  }
}
