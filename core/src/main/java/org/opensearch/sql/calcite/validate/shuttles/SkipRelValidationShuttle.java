/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate.shuttles;

import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

/**
 * A RelShuttle that detects if validation should be skipped for certain operations. Currently, it
 * detects the following patterns:
 *
 * <ul>
 *   <li>binning on datetime types, which is only executable after pushdown.
 * </ul>
 */
public class SkipRelValidationShuttle extends RelShuttleImpl {
  private boolean shouldSkip = false;
  private final RexShuttle rexShuttle;

  /** Predicates about patterns of calls that should not be validated. */
  public static final List<Predicate<RexCall>> SKIP_CALLS;

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
    SKIP_CALLS = List.of(binOnTimestamp);
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
