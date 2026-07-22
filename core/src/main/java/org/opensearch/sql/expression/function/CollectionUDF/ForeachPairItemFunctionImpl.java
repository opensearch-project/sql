/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Extracts one slot of an internal foreach pair: {@code foreach_pair_item(pair, index)}. Returns
 * OTHER by default; the foreach planner assigns every call its inferred slot type explicitly.
 */
public class ForeachPairItemFunctionImpl extends ImplementorUDF {
  public ForeachPairItemFunctionImpl() {
    super(new ForeachPairItemImplementor(), NullPolicy.NONE);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding ->
        opBinding
            .getTypeFactory()
            .createTypeWithNullability(
                opBinding.getTypeFactory().createSqlType(SqlTypeName.OTHER), true);
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
    List<?> pair = args[0] instanceof Object[] array ? Arrays.asList(array) : (List<?>) args[0];
    return index < 0 || index >= pair.size() ? null : pair.get(index);
  }
}
