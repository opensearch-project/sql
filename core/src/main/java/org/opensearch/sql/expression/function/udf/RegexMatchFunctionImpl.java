/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import java.util.List;
import java.util.regex.PatternSyntaxException;
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
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * REGEX_MATCH UDF implementation for Calcite engine. This function provides Java regex matching via
 * script query pushdown.
 */
public class RegexMatchFunctionImpl extends ImplementorUDF {

  public RegexMatchFunctionImpl() {
    super(new RegexMatchImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return null;
  }

  public static class RegexMatchImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(
                      RegexMatchFunctionImpl.class, "eval", String.class, String.class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  /**
   * Evaluation method for REGEX_MATCH function. This method is called by Calcite's generated code
   * during execution.
   *
   * @param field The field value to match against
   * @param pattern The Java regex pattern
   * @return Boolean result of regex match
   */
  public static Boolean eval(String field, String pattern) {
    if (field == null || pattern == null) {
      return null;
    }

    // Use shared utility for consistent regex matching
    try {
      return org.opensearch.sql.expression.parse.RegexCommonUtils.matchesPartial(field, pattern);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regex pattern: " + e.getMessage());
    }
  }
}
