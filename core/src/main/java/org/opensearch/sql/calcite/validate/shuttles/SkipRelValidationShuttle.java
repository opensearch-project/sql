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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

/**
 * A RelShuttle that detects if validation should be skipped for certain operations. Currently, it
 * detects the following patterns:
 *
 * <ul>
 *   <li>Binning on datetime types, which is only executable after pushdown.
 *   <li>Aggregates with multiple complex CASE statements, which cause field reference issues during
 *       the SQL-to-Rel conversion.
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
 */
public class SkipRelValidationShuttle extends RelShuttleImpl {
  private boolean shouldSkip = false;
  private final RexShuttle rexShuttle;

  /** Predicates about patterns of calls that should not be validated. */
  public static final List<Predicate<RexCall>> SKIP_CALLS;

  /** Predicates about logical aggregates that should not be validated */
  public static final List<Predicate<LogicalAggregate>> SKIP_AGGREGATES;

  static {
    Predicate<RexCall> binOnTimestamp =
        call -> {
          if ("WIDTH_BUCKET".equalsIgnoreCase(call.getOperator().getName())) {
            if (!call.getOperands().isEmpty()) {
              RexNode firstOperand = call.getOperands().get(0);
              return OpenSearchTypeFactory.isDatetime(firstOperand.getType());
            }
          }
          return false;
        };
    Predicate<LogicalAggregate> groupByMultipleCases =
        aggregate -> {
          if (aggregate.getGroupCount() >= 2
              && aggregate.getInput() instanceof LogicalProject project) {
            long nGroupByCase =
                project.getProjects().stream().filter(p -> p.isA(SqlKind.CASE)).count();
            return nGroupByCase >= 2;
          }
          return false;
        };
    SKIP_CALLS = List.of(binOnTimestamp);
    SKIP_AGGREGATES = List.of(groupByMultipleCases);
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
