/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.conversion;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/** Placeholder UDF that wraps a VARCHAR literal cast to VARBINARY for ip/binary fields. */
public class BinaryFunction extends ImplementorUDF {

  private static final SqlReturnTypeInference VARBINARY_FORCE_NULLABLE =
      ReturnTypes.explicit(
          TYPE_FACTORY.createTypeWithNullability(
              TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY), true));

  public BinaryFunction() {
    super(new PassThroughImplementor(), NullPolicy.STRICT);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return VARBINARY_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.CHARACTER);
  }

  public static class PassThroughImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return translatedOperands.get(0);
    }
  }
}
