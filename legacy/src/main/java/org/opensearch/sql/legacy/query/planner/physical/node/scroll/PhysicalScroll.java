/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical.node.scroll;

import java.util.Iterator;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.query.AggregationQueryAction;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.planner.core.ExecuteParams;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.Row;
import org.opensearch.sql.legacy.query.planner.physical.estimation.Cost;

/** The definition of Scroll Operator. */
@RequiredArgsConstructor
public class PhysicalScroll implements PhysicalOperator<BindingTuple> {
  private final QueryAction queryAction;

  private Iterator<BindingTupleRow> rowIterator;

  @Override
  public Cost estimate() {
    return null;
  }

  @Override
  public PlanNode[] children() {
    return new PlanNode[0];
  }

  @Override
  public boolean hasNext() {
    return rowIterator.hasNext();
  }

  @Override
  public Row<BindingTuple> next() {
    return rowIterator.next();
  }

  @Override
  public void open(ExecuteParams params) {
    try {
      ActionResponse response = queryAction.explain().get();
      if (queryAction instanceof AggregationQueryAction) {
        rowIterator =
            SearchAggregationResponseHelper.populateSearchAggregationResponse(
                    ((SearchResponse) response).getAggregations())
                .iterator();
      } else {
        throw new IllegalStateException("Not support QueryAction type: " + queryAction.getClass());
      }
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
  }

  @SneakyThrows
  @Override
  public String toString() {
    return queryAction.explain().toString();
  }
}
