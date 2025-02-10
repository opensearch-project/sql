package org.opensearch.sql.planner.logical;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Logical Field Summary. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalFieldSummary extends LogicalAggregation {

    Map<String, String> aggregationToFieldNameMap;

    public LogicalFieldSummary(
            LogicalPlan child, List<NamedAggregator> aggregatorList, List<NamedExpression> groupByList, Map<String, String> aggregationToFieldNameMap) {
        super(child, aggregatorList, groupByList);
        this.aggregationToFieldNameMap = aggregationToFieldNameMap;
    }
}