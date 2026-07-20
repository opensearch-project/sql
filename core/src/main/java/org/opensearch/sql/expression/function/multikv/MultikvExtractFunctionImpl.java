/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.multikv;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Internal UDF backing the {@code multikv} command. Extracts a single named cell value out of one
 * serialized per-row record produced by {@link MultikvSplitFunctionImpl}.
 *
 * <p>Signature: {@code MULTIKV_EXTRACT(recordString, columnName)} returns {@code varchar} (null
 * when the column is absent from the record).
 */
public class MultikvExtractFunctionImpl extends ImplementorUDF {

  public MultikvExtractFunctionImpl() {
    super(new MultikvExtractImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.explicit(
        TYPE_FACTORY.createTypeWithNullability(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), true));
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static Object eval(Object... args) {
    if (args.length < 2 || args[0] == null || args[1] == null) {
      return null;
    }
    return MultikvParser.extract((String) args[0], (String) args[1]);
  }

  public static class MultikvExtractImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(MultikvExtractFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }
}
