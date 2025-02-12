package org.opensearch.sql.planner.logical;

import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;

/** Logical Field Summary. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalFieldSummary extends LogicalAggregation {

  Map<String, Map.Entry<String, ExprType>> aggregationToFieldNameMap;

  public LogicalFieldSummary(
      LogicalPlan child,
      List<NamedAggregator> aggregatorList,
      List<NamedExpression> groupByList,
      Map<String, Map.Entry<String, ExprType>> aggregationToFieldNameMap) {
    super(child, aggregatorList, groupByList);
    this.aggregationToFieldNameMap = aggregationToFieldNameMap;
  }
}
