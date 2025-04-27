/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.text;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * <code>LOCATE(substr, str[, start])</code> returns the position of the first occurrence of
 * `substr` in `str`, optionally starting the search at a specified position.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>(STRING, STRING) -> INTEGER
 *   <li>(STRING, STRING, INTEGER) -> INTEGER
 * </ul>
 */
public class LocateFunction extends ImplementorUDF {
  public LocateFunction() {
    super(new LocateImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.INTEGER_FORCE_NULLABLE;
  }

  public static class LocateImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(LocateImplementor.class, "locate", translatedOperands);
    }

    public static int locate(String substr, String str) {
      return str.indexOf(substr) + 1;
    }

    public static int locate(String substr, String str, int start) {
      return str.indexOf(substr, start - 1) + 1;
    }
  }
}
