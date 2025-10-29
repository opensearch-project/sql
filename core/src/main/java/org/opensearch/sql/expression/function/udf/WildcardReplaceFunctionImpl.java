/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

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
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.calcite.utils.WildcardReplaceUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * UDF for wildcard-based string replacement in PPL replace command.
 *
 * <p>This function wraps WildcardReplaceUtils.replaceWithWildcard to provide SPL-compatible
 * wildcard matching where '*' matches zero or more characters.
 */
public class WildcardReplaceFunctionImpl extends ImplementorUDF {

  public WildcardReplaceFunctionImpl() {
    super(new WildcardReplaceImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING));
  }

  /** Implementor for wildcard replace function. */
  public static class WildcardReplaceImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(
                      WildcardReplaceUtils.class,
                      "replaceWithWildcard",
                      String.class,
                      String.class,
                      String.class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }
}
