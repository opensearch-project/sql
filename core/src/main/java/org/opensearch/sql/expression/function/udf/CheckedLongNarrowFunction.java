/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Narrows a numeric SUM accumulator (widened to DECIMAL/double so it does not wrap) back to BIGINT,
 * raising an error when the value overflowed the BIGINT range instead of wrapping to a negative
 * value.
 *
 * <p>SUM over a BIGINT column is accumulated as a wider type to avoid the silent {@code long}
 * two's-complement wrap near 2^63. This function restores the declared BIGINT output type. A value
 * whose magnitude clearly exceeds {@code 2^63} is treated as overflow and surfaces as a client
 * error. Because OpenSearch computes pushed-down sums in {@code double}, {@code (double)
 * Long.MAX_VALUE} rounds to exactly {@code 2^63} and cannot be distinguished from a small overflow;
 * to avoid erroring on a legitimate near-{@code Long.MAX_VALUE} sum, only magnitudes strictly
 * beyond {@code 2^63} are rejected, and in-range values saturate on narrowing rather than wrap.
 * This makes the pushdown and in-memory paths behave identically.
 */
public class CheckedLongNarrowFunction extends ImplementorUDF {
  public CheckedLongNarrowFunction() {
    super(new CheckedLongNarrowImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BIGINT_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC;
  }

  /** {@code 2^63}, the first magnitude beyond the signed BIGINT range. */
  private static final double TWO_POW_63 = 0x1p63;

  /** Narrow a widened sum value to a long, erroring when it overflowed the BIGINT range. */
  public static long narrow(Object value) {
    double magnitude = ((Number) value).doubleValue();
    if (magnitude > TWO_POW_63 || magnitude < -TWO_POW_63) {
      throw new ArithmeticException("BIGINT overflow in SUM");
    }
    if (value instanceof BigDecimal decimal) {
      // Exact for the in-memory (DECIMAL) path; clamps a value that rounds to exactly 2^63.
      return decimal
          .min(BigDecimal.valueOf(Long.MAX_VALUE))
          .max(BigDecimal.valueOf(Long.MIN_VALUE))
          .longValue();
    }
    // double path (pushed-down sum): saturating narrow (JLS 5.1.3) clamps 2^63 to Long.MAX_VALUE.
    return (long) magnitude;
  }

  public static class CheckedLongNarrowImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(CheckedLongNarrowFunction.class, "narrow", translatedOperands.get(0));
    }
  }
}
