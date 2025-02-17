/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.core;

import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.opensearch.sql.legacy.domain.ColumnTypeProvider;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.query.planner.converter.SQLToOperatorConverter;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.node.scroll.PhysicalScroll;
import org.opensearch.transport.client.Client;

/** The definition of QueryPlanner which return the {@link BindingTuple} as result. */
public class BindingTupleQueryPlanner {
  private final PhysicalOperator<BindingTuple> physicalOperator;
  @Getter private final List<ColumnNode> columnNodes;

  public BindingTupleQueryPlanner(
      Client client, SQLQueryExpr sqlExpr, ColumnTypeProvider columnTypeProvider) {
    SQLToOperatorConverter converter = new SQLToOperatorConverter(client, columnTypeProvider);
    sqlExpr.accept(converter);
    this.physicalOperator = converter.getPhysicalOperator();
    this.columnNodes = converter.getColumnNodes();
  }

  /**
   * Execute the QueryPlanner.
   *
   * @return list of {@link BindingTuple}.
   */
  public List<BindingTuple> execute() {
    PhysicalOperator<BindingTuple> op = physicalOperator;
    List<BindingTuple> tuples = new ArrayList<>();
    try {
      op.open(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    while (op.hasNext()) {
      tuples.add(op.next().data());
    }
    return tuples;
  }

  /**
   * Explain the physical execution plan.
   *
   * @return execution plan.
   */
  public String explain() {
    Explanation explanation = new Explanation();
    physicalOperator.accept(explanation);
    return explanation.explain();
  }

  private static class Explanation implements PlanNode.Visitor {
    private String explain;

    public String explain() {
      return explain;
    }

    @Override
    public boolean visit(PlanNode planNode) {
      if (planNode instanceof PhysicalScroll) {
        explain = planNode.toString();
      }
      return true;
    }
  }
}
