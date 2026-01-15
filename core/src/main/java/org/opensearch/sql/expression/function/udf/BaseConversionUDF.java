/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Base class for PPL conversion functions (auto, num, rmcomma, rmunit). */
public abstract class BaseConversionUDF extends ImplementorUDF {

  protected BaseConversionUDF(String conversionMethodName) {
    super(new ConversionImplementor(conversionMethodName), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(
        factory ->
            factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DOUBLE), true));
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.OPTIONAL_ANY;
  }

  public static class ConversionImplementor implements NotNullImplementor {
    private final String methodName;

    public ConversionImplementor(String methodName) {
      this.methodName = methodName;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression fieldValue = translatedOperands.get(0);
      Expression result =
          Expressions.call(ConversionUtils.class, methodName, Expressions.box(fieldValue));
      return Expressions.call(ConversionImplementor.class, "toDoubleOrNull", result);
    }

    public static Double toDoubleOrNull(Object value) {
      if (value instanceof Number) {
        return ((Number) value).doubleValue();
      }
      return null;
    }
  }
}
