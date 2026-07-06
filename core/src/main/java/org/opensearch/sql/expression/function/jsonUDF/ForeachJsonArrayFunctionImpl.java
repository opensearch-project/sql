/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import static org.opensearch.sql.expression.function.jsonUDF.JsonUtils.gson;

import com.google.gson.JsonSyntaxException;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Converts a JSON array string to a Calcite enumerable array for foreach collection modes. */
public class ForeachJsonArrayFunctionImpl extends ImplementorUDF {
  public ForeachJsonArrayFunctionImpl() {
    super(new ForeachJsonArrayImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return opBinding ->
        SqlTypeUtil.createArrayType(
            opBinding.getTypeFactory(),
            opBinding
                .getTypeFactory()
                .createTypeWithNullability(
                    opBinding.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true),
            true);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.ANY));
  }

  public static class ForeachJsonArrayImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(ForeachJsonArrayFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  public static Object eval(Object... args) {
    if (args.length != 1 || args[0] == null) {
      return null;
    }
    if (args[0] instanceof List<?>) {
      return args[0];
    }
    try {
      return gson.fromJson(String.valueOf(args[0]), List.class);
    } catch (JsonSyntaxException e) {
      return null;
    }
  }
}
