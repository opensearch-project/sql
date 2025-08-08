/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;

import com.google.common.collect.ImmutableList;

/**
 * DynamicPivotOperator is responsible for implementing the two-phase approach for the timechart command.
 * It first executes a query to get distinct values for the "by" field, then uses those values to build the pivot table.
 */
public class DynamicPivotOperator {

    /**
     * Execute the pivot operation in two phases:
     * 1. Execute a query to get distinct values for the "by" field
     * 2. Use those distinct values to build the pivot table
     *
     * @param relBuilder The RelBuilder to use for building the pivot table
     * @param timeField The name of the time field
     * @param byField The name of the "by" field
     * @param valueField The name of the value field
     * @param distinctValues The distinct values for the "by" field
     * @return The RelNode representing the pivot table
     */
    public static RelNode executePivot(
            RelBuilder relBuilder,
            String timeField,
            String byField,
            String valueField,
            List<String> distinctValues) {
        
        // Save the current state
        RelNode currentState = relBuilder.peek();
        
        // Group by the time field
        List<RexNode> timeGroupBy = List.of(relBuilder.field(timeField));
        
        // Create a list of aggregation calls for the pivot
        List<AggCall> pivotAggCalls = new ArrayList<>();
        
        // For each distinct value, create a CASE expression and an aggregation call
        for (String distinctValue : distinctValues) {
            // Create a CASE expression:
            // CASE WHEN by_field = 'value' THEN value_field ELSE NULL END
            RexNode caseExpr = relBuilder.call(
                    SqlStdOperatorTable.CASE,
                    relBuilder.equals(relBuilder.field(byField), relBuilder.literal(distinctValue)),
                    relBuilder.field(valueField),
                    relBuilder.literal(null)
            );
            
            // Add this as a new column
            relBuilder.projectPlus(
                    relBuilder.alias(caseExpr, "pivot_" + distinctValue)
            );
            
            // Create an aggregation call to get the maximum value for this column
            // (MAX works here because there will be at most one non-null value per group)
            pivotAggCalls.add(
                    relBuilder.aggregateCall(SqlStdOperatorTable.MAX, relBuilder.field("pivot_" + distinctValue))
                            .as(distinctValue)
            );
        }
        
        // Group by the time field and aggregate the pivot columns
        relBuilder.aggregate(relBuilder.groupKey(timeGroupBy), pivotAggCalls);
        
        // Sort by time
        relBuilder.sort(relBuilder.field(0));
        
        return relBuilder.peek();
    }
    
    /**
     * Get the distinct values for the "by" field from the result of a query.
     * This method would be called during execution, not during planning.
     *
     * @param result The result of the query
     * @param byField The name of the "by" field
     * @return The distinct values for the "by" field
     */
    public static List<String> getDistinctValues(List<Map<String, Object>> result, String byField) {
        return result.stream()
                .map(row -> row.get(byField))
                .filter(value -> value != null)
                .map(Object::toString)
                .distinct()
                .collect(Collectors.toList());
    }
}
