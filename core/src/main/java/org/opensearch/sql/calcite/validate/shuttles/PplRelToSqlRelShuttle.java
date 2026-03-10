/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate.shuttles;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A RelShuttle that recursively visits all RelNodes and their RexNode expressions to fix interval
 * literals and float literal before/after SQL conversion.
 *
 * <p>This shuttle extends RelShuttleImpl to ensure it visits the entire RelNode tree recursively,
 * applying the interval literal fixes at each node.
 */
public class PplRelToSqlRelShuttle extends RelShuttleImpl {
  private final RexShuttle rexShuttle;

  public PplRelToSqlRelShuttle(RexBuilder rexBuilder, boolean forward) {
    this.rexShuttle =
        new RexShuttle() {
          /**
           * This visitor fixes: 1. float literal: when converting logical plan to sql node, float
           * information is missing. All floats will be treated as double. A compulsory cast is
           * inserted here to ensure a cast presents in the generated SQL 2. interval literal: we
           * create and read the interval literal in a different way that how Calcite originally
           * expected it to be.
           */
          @Override
          public RexNode visitLiteral(RexLiteral literal) {
            // 1. Fix float literal
            SqlTypeName literalType = literal.getType().getSqlTypeName();
            if (SqlTypeName.REAL.equals(literalType) || SqlTypeName.FLOAT.equals(literalType)) {
              return rexBuilder.makeCall(
                  literal.getType(), SqlLibraryOperators.SAFE_CAST, List.of(literal));
            }

            // 2. Fix interval literal
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
