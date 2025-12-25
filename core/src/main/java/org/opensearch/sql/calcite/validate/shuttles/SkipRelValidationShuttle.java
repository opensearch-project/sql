/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate.shuttles;

import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;

/**
 * A RelShuttle that detects if validation should be skipped for certain operations. Currently, it
 * detects the following patterns:
 *
 * <ul>
 *   <li>Binning on datetime types, which is only executable after pushdown.
 *   <li>Aggregates with multiple complex CASE statements, which cause field reference issues during
 *       the SQL-to-Rel conversion.
 *   <li>LogicalValues is used to populate empty row values
 * </ul>
 *
 * <b>Group by multiple CASE statements</b>
 *
 * <p>When grouping by multiple CASE expressions, a Calcite 1.41 bug causes field references to
 * become invalid during SQL-to-Rel conversion. This affects queries in {@code
 * testCaseCanBePushedDownAsCompositeRangeQuery} 2.4 and {@code testCaseCanBePushedDownAsRangeQuery}
 * 1.3. E.g. for the following query:
 *
 * <pre>{@code
 * source=opensearch-sql_test_index_bank
 * | eval age_range = case(age < 30, 'u30', age < 40, 'u40' else 'u100'),
 *        balance_range = case(balance < 20000, 'medium' else 'high')
 * | stats avg(balance) as avg_balance by age_range, balance_range
 * }</pre>
 *
 * <p>During validation, this PPL query is converted to SQL:
 *
 * <pre>{@code
 * SELECT AVG(`balance`) AS `avg_balance`,
 *        CASE WHEN `age` < 30 THEN 'u30' WHEN `age` < 40 THEN 'u40' ELSE 'u100' END AS `age_range`,
 *        CASE WHEN `balance` < 20000 THEN 'medium' ELSE 'high' END AS `balance_range`
 * FROM `OpenSearch`.`opensearch-sql_test_index_bank`
 * GROUP BY CASE WHEN `age` < 30 THEN 'u30' WHEN `age` < 40 THEN 'u40' ELSE 'u100' END,
 *          CASE WHEN `balance` < 20000 THEN 'medium' ELSE 'high' END
 * }</pre>
 *
 * <p>When Calcite converts this SQL back to RelNode, it processes GROUP BY expressions
 * sequentially, making field references in the second CASE expression invalid.
 *
 * <p><b>Generate empty row with LogicalValues</b>
 *
 * <p>Types in the rows generated with {@code VALUES} will not be preserved, causing validation
 * issues when converting SQL back to a logical plan.
 *
 * <p>For example, in {@code CalcitePPLAggregationIT.testSumEmpty}, the query {@code
 * source=opensearch-sql_test_index_bank_with_null_values | where 1=2 | stats sum(balance)} will be
 * converted to the following SQL:
 *
 * <pre>{@code
 * SELECT SUM(CAST(`balance` AS DECIMAL(38, 19))) AS `sum(balance)`
 * FROM (VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) AS `t` (`account_number`, `firstname`, `address`, `balance`, `gender`, `age`, `lastname`, `_id`, `_index`, `_score`, `_maxscore`, `_sort`, `_routing`)
 * WHERE 1 = 0
 * }</pre>
 *
 * When converted back to logical plan, {@code CAST(`balance` AS DECIMAL(38, 19))} will fail because
 * the type of balance is lost.
 *
 * <p><b>Note for developers</b>: when validations fail during developing new features, please try
 * to solve the root cause instead of adding skipping rules here. Under rare cases when you have to
 * skip validation, please document the exact reason.
 *
 * <p><b>WARNING</b>: When a skip pattern is detected, we bypass the entire validation pipeline,
 * skipping potentially useful transformation relying on rewriting SQL node
 */
public class SkipRelValidationShuttle extends RelShuttleImpl {
  private boolean shouldSkip = false;
  private final RexShuttle rexShuttle;

  /** Predicates about patterns of calls that should not be validated. */
  public static final List<Predicate<RexCall>> SKIP_CALLS;

  /** Predicates about logical aggregates that should not be validated */
  public static final List<Predicate<LogicalAggregate>> SKIP_AGGREGATES;

  /** Predicates about logical values that should not be validated */
  public static final List<Predicate<LogicalValues>> SKIP_VALUES;

  static {
    // TODO: Make incompatible operations like bin-on-timestamp a validatable UDFs so that they can
    //  be still be converted to SqlNode and back to RelNode
    Predicate<RexCall> binOnTimestamp =
        call -> {
          if ("WIDTH_BUCKET".equalsIgnoreCase(call.getOperator().getName())) {
            if (!call.getOperands().isEmpty()) {
              RexNode firstOperand = call.getOperands().get(0);
              return OpenSearchTypeUtil.isDatetime(firstOperand.getType());
            }
          }
          return false;
        };
    Predicate<LogicalAggregate> groupByMultipleCases =
        aggregate -> {
          if (aggregate.getGroupCount() > 1
              && aggregate.getInput() instanceof LogicalProject project) {
            long nGroupByCase =
                project.getProjects().stream().filter(p -> p.isA(SqlKind.CASE)).count();
            return nGroupByCase > 1;
          }
          return false;
        };
    Predicate<LogicalValues> createEmptyRow = values -> values.getTuples().isEmpty();
    SKIP_CALLS = List.of(binOnTimestamp);
    SKIP_AGGREGATES = List.of(groupByMultipleCases);
    SKIP_VALUES = List.of(createEmptyRow);
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    for (Predicate<LogicalAggregate> skipAgg : SKIP_AGGREGATES) {
      if (skipAgg.test(aggregate)) {
        shouldSkip = true;
        return aggregate;
      }
    }
    return super.visit(aggregate);
  }

  @Override
  public RelNode visit(LogicalValues values) {
    for (Predicate<LogicalValues> skipValues : SKIP_VALUES) {
      if (skipValues.test(values)) {
        shouldSkip = true;
        return values;
      }
    }
    return super.visit(values);
  }

  public SkipRelValidationShuttle() {
    this.rexShuttle =
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            for (Predicate<RexCall> skipCall : SKIP_CALLS) {
              if (skipCall.test(call)) {
                shouldSkip = true;
                return call;
              }
            }
            return super.visitCall(call);
          }
        };
  }

  /** Returns true if validation should be skipped based on detected conditions. */
  public boolean shouldSkipValidation() {
    return shouldSkip;
  }

  @Override
  protected RelNode visitChild(RelNode parent, int i, RelNode child) {
    RelNode newChild = super.visitChild(parent, i, child);
    return newChild.accept(rexShuttle);
  }
}
