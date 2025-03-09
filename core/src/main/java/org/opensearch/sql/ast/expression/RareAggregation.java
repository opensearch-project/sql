/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.tree.Aggregation;

/**
 * Logical plan node of Rare (Aggregation) command, the interface for building aggregation actions
 * in queries.
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = true)
public class RareAggregation extends Aggregation implements CountedAggregation {
  private final Optional<Literal> results;

  /** Aggregation Constructor without span and argument. */
  public RareAggregation(
      Optional<Literal> results,
      List<UnresolvedExpression> aggExprList,
      List<UnresolvedExpression> sortExprList,
      List<UnresolvedExpression> groupExprList) {
    super(aggExprList, sortExprList, groupExprList, null, Collections.emptyList());
    this.results = results;
  }
}
