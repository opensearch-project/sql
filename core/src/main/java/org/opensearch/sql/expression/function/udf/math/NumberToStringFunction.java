/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * A custom implementation of number to string cast.
 *
 * <p>This operator is necessary because Calcite's built-in CAST converts floating point 0.0 to 0E0,
 * and converts decimal 0.123 to .123 when casting them to string.
 */
public class NumberToStringFunction extends ImplementorUDF {
  public NumberToStringFunction() {
    super(new NumberToStringImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.STRING_FORCE_NULLABLE;
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC;
  }

  public static class NumberToStringImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression operand = translatedOperands.get(0);
      return Expressions.call(Expressions.box(operand), "toString");
    }
  }
}
