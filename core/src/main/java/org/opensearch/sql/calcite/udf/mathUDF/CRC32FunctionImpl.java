/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.mathUDF;

import java.util.List;
import java.util.zip.CRC32;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class CRC32FunctionImpl extends ImplementorUDF {
  public CRC32FunctionImpl() {
    super(new Crc32Implementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BIGINT_FORCE_NULLABLE;
  }

  public static class Crc32Implementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(Crc32Implementor.class, "crc32", translatedOperands.getFirst());
    }

    public static long crc32(String value) {
      CRC32 crc = new CRC32();
      crc.update(value.getBytes());
      return crc.getValue();
    }
  }
}
