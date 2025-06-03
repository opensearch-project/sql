/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Convert number x from base a to base b<br>
 * The supported signature of floor function is<br>
 * (STRING, INTEGER, INTEGER) -> STRING<br>
 * (INTEGER, INTEGER, INTEGER) -> STRING
 */
public class ConvFunction extends ImplementorUDF {
  public ConvFunction() {
    super(new ConvImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.STRING_FORCE_NULLABLE;
  }

  public static class ConvImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      RelDataType numberType = call.getOperands().get(0).getType();
      Expression number = translatedOperands.get(0);
      Expression fromBase = translatedOperands.get(1);
      Expression toBase = translatedOperands.get(2);
      if (numberType.getFamily() == SqlTypeFamily.NUMERIC) {
        // Convert the first operand to String
        number = Expressions.call(Object.class, "toString", number);
      }
      return Expressions.call(ConvImplementor.class, "conv", number, fromBase, toBase);
    }

    /**
     * Convert numStr from fromBase to toBase
     *
     * @param numStr the number to convert (case-insensitive for alphanumeric digits, may have a
     *     leading '-')
     * @param fromBase base of the input number (2 to 36)
     * @param toBase target base (2 to 36)
     * @return the converted number in the target base (uppercase), "0" if the input is invalid, or
     *     null if bases are out of range.
     */
    @Strict
    public static String conv(String numStr, int fromBase, int toBase) {
      return Long.toString(Long.parseLong(numStr, fromBase), toBase);
    }
  }
}
