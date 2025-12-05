/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import java.math.BigDecimal;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlIntervalQualifier;

/**
 * A RelShuttle that recursively visits all RelNodes and their RexNode expressions to fix interval
 * literals before/after SQL conversion.
 *
 * <p>This shuttle extends RelShuttleImpl to ensure it visits the entire RelNode tree recursively,
 * applying the interval literal fixes at each node.
 */
public class PplRelToSqlRelShuttle extends RelShuttleImpl {
  private final RexShuttle rexShuttle;

  public PplRelToSqlRelShuttle(RexBuilder rexBuilder, boolean forward) {
    this.rexShuttle =
        new RexShuttle() {
          @Override
          public RexNode visitLiteral(RexLiteral literal) {
            SqlIntervalQualifier qualifier = literal.getType().getIntervalQualifier();
            if (qualifier == null) {
              return literal;
            }
            BigDecimal value = literal.getValueAs(BigDecimal.class);
            if (value == null) {
              return literal;
            }
            TimeUnit unit = qualifier.getUnit();
            // An ad-hoc fix to a Calcite bug in RexLiteral#intervalString -- quarter type does not
            // exist in SqlTypeName, rendering it return number of months instead of number of
            // quarters.
            BigDecimal forwardMultiplier =
                TimeUnit.QUARTER.equals(unit) ? BigDecimal.valueOf(1) : unit.multiplier;

            // QUARTER intervals are stored as INTERVAL_MONTH in Calcite's type system
            // but the qualifier preserves the actual unit (QUARTER vs MONTH).
            // The multiplier for QUARTER is 3 (months), for MONTH is 1.
            BigDecimal newValue =
                forward
                    ? value.multiply(forwardMultiplier)
                    : value.divideToIntegralValue(unit.multiplier);
            return rexBuilder.makeIntervalLiteral(newValue, qualifier);
          }
        };
  }

  @Override
  protected RelNode visitChild(RelNode parent, int i, RelNode child) {
    RelNode newChild = super.visitChild(parent, i, child);
    return newChild.accept(rexShuttle);
  }
}
