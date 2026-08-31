/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.SearchPredicateCompiler;

/**
 * A scan that can defer a {@code query_string} predicate until its correlated input is available.
 */
public interface DynamicQueryStringPushDown {

  /**
   * Pushes a dynamic {@code query_string} filter into this scan.
   *
   * @param condition filter condition containing a correlated query argument
   * @param runtimePredicates correlated PPL predicate values that require runtime compilation
   * @param compiler compiler from PPL search syntax to OpenSearch query-string syntax
   * @return a scan that builds the OpenSearch request after the correlated value is available
   */
  RelNode pushDownDynamicQueryString(
      RexNode condition, List<RexNode> runtimePredicates, SearchPredicateCompiler compiler);
}
