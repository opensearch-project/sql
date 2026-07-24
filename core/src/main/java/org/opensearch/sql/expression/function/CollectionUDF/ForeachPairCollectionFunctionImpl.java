/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.ArrayList;
import java.util.Arrays;
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

/** Builds the internal foreach pair array: [item, iter, captured-field...]. */
public class ForeachPairCollectionFunctionImpl extends ImplementorUDF {
  public ForeachPairCollectionFunctionImpl() {
    super(new ForeachPairCollectionImplementor(), NullPolicy.NONE);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding -> {
      RelDataType pair =
          opBinding
              .getTypeFactory()
              .createTypeWithNullability(
                  opBinding.getTypeFactory().createSqlType(SqlTypeName.OTHER), true);
      return SqlTypeUtil.createArrayType(opBinding.getTypeFactory(), pair, true);
    };
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class ForeachPairCollectionImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          Types.lookupMethod(ForeachPairCollectionFunctionImpl.class, "eval", Object[].class),
          translatedOperands);
    }
  }

  public static Object eval(Object... args) {
    if (args.length == 0 || args[0] == null) {
      return null;
    }
    List<?> source = toList(args[0]);
    List<Object[]> pairs = new ArrayList<>();
    for (int i = 0; i < source.size(); i++) {
      Object[] pair = new Object[args.length + 1];
      pair[0] = source.get(i);
      pair[1] = i;
      for (int j = 1; j < args.length; j++) {
        pair[j + 1] = args[j];
      }
      pairs.add(pair);
    }
    return pairs;
  }

  private static List<?> toList(Object value) {
    if (value instanceof List<?> list) {
      return list;
    }
    if (value instanceof Object[] array) {
      return Arrays.asList(array);
    }
    throw new IllegalArgumentException("foreach pair collection requires an array input");
  }
}
