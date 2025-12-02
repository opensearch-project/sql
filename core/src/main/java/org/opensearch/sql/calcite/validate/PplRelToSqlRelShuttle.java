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
            BigDecimal newValue =
                forward
                    ? value.multiply(unit.multiplier)
                    : value.divideToIntegralValue(unit.multiplier);
            return rexBuilder.makeIntervalLiteral(newValue, qualifier);
          }
        };
  }

  @Override
  protected RelNode visitChild(RelNode parent, int i, RelNode child) {
    // First visit the child recursively
    RelNode newChild = super.visitChild(parent, i, child);
    // Then apply the RexShuttle to the child's expressions
    return newChild.accept(rexShuttle);
  }
}
