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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

public class RelevanceQueryFunction extends ImplementorUDF {

  public RelevanceQueryFunction() {
    super(new RelevanceQueryImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        (CompositeOperandTypeChecker)
            OperandTypes.STRING_STRING
                .or(
                    OperandTypes.family(
                        SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.MAP))
                .or(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.STRING))
                .or(
                    OperandTypes.family(
                        SqlTypeFamily.MAP, SqlTypeFamily.STRING, SqlTypeFamily.MAP)));
  }

  public static class RelevanceQueryImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      throw new UnsupportedOperationException(
          "Relevance search query functions are only supported when they are pushed down");
    }
  }
}
