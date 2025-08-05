/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * A custom implementation of FormatNumber in replace of SqlLibraryOperators.FORMAT_NUMBER
 *
 * <p>The operators SqlLibraryOperators.FORMAT_NUMBER will convert a float to double with code
 * equivalent to {@code ((Number) float).doubleValue()}, which will lead to a loss in precision.
 * E.g. 6.2 becomes 6.199999809265137. This operator fix the problem by converting the number to a
 * BigDecimal before formatting it.
 */
public class FormatNumberFunction extends ImplementorUDF {

  /**
   * Formats double values in PPL using a non-scientific notation, displaying up to 16 digits after
   * the decimal point. This formatting is consistent with PPL V2.
   */
  private static final String DOUBLE_FORMAT = "0.0###############";

  public FormatNumberFunction() {
    super(new FormatNumberImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.STRING_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC;
  }

  public static class FormatNumberImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression operand = translatedOperands.get(0);
      Expression decimal =
          Expressions.call(
              FormatNumberImplementor.class, "convertNumber", Expressions.box(operand));
      return Expressions.call(
          SqlFunctions.class, "formatNumber", decimal, Expressions.constant(DOUBLE_FORMAT));
    }

    public static BigDecimal convertNumber(Number number) {
      return new BigDecimal(number.toString());
    }
  }
}
