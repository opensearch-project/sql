/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Extracts an internal foreach pair slot while preserving the slot's inferred Calcite type. */
public class ForeachPairItemFunctionImpl extends ImplementorUDF {
  public ForeachPairItemFunctionImpl() {
    super(new ForeachPairItemImplementor(), NullPolicy.NONE);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      SqlTypeName typeName = SqlTypeName.valueOf(opBinding.getOperandLiteralValue(2, String.class));
      RelDataType type;
      if (typeName == SqlTypeName.ARRAY) {
        SqlTypeName componentTypeName =
            SqlTypeName.valueOf(opBinding.getOperandLiteralValue(3, String.class));
        type =
            SqlTypeUtil.createArrayType(
                opBinding.getTypeFactory(),
                opBinding
                    .getTypeFactory()
                    .createTypeWithNullability(
                        opBinding.getTypeFactory().createSqlType(componentTypeName), true),
                true);
      } else {
        type = opBinding.getTypeFactory().createSqlType(typeName);
      }
      return opBinding.getTypeFactory().createTypeWithNullability(type, true);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class ForeachPairItemImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          Types.lookupMethod(ForeachPairItemFunctionImpl.class, "eval", Object[].class),
          translatedOperands);
    }
  }

  public static Object eval(Object... args) {
    if (args.length < 2 || args[0] == null || args[1] == null) {
      return null;
    }
    int index = ((Number) args[1]).intValue();
    if (args[0] instanceof Object[] pair) {
      return index < 0 || index >= pair.length ? null : pair[index];
    }
    List<?> pair = (List<?>) args[0];
    if (index < 0 || index >= pair.size()) {
      return null;
    }
    return pair.get(index);
  }
}
