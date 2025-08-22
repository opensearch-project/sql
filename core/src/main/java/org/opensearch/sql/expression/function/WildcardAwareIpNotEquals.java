/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

/** Wildcard-aware IP address not-equals function. */
public class WildcardAwareIpNotEquals extends WildcardAwareIpEquals {

  @Override
  public RexNode resolve(RexBuilder builder, RexNode arg1, RexNode arg2) {
    // Check if this is an IP comparison with wildcards
    if (isIpType(arg1) && arg2.isA(SqlKind.LITERAL)) {
      String value = ((RexLiteral) arg2).getValueAs(String.class);
      if (value != null && containsWildcards(value)) {
        // Create negated range check
        return createIpRangeCheck(builder, arg1, value, true);
      }
    } else if (arg1.isA(SqlKind.LITERAL) && isIpType(arg2)) {
      String value = ((RexLiteral) arg1).getValueAs(String.class);
      if (value != null && containsWildcards(value)) {
        // Handle reverse order (literal first)
        return createIpRangeCheck(builder, arg2, value, true);
      }
    }

    // Fall back to standard IP not-equals
    return builder.makeCall(PPLBuiltinOperators.NOT_EQUALS_IP, arg1, arg2);
  }
}
