/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

import java.util.List;

/**
 * Custom REX_EXTRACT function for extracting regex named capture groups.
 */
public final class RexExtractFunction extends ImplementorUDF {

  public RexExtractFunction() {
    super(new RexExtractImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.STRING_STRING_INTEGER;
  }

  private static class RexExtractImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression field = translatedOperands.get(0);
      Expression pattern = translatedOperands.get(1);
      Expression groupIndex = translatedOperands.get(2);
      
      return Expressions.call(
          RexExtractFunction.class,
          "extractGroup",
          field,
          pattern,
          groupIndex
      );
    }
  }
  public static String extractGroup(String text, String pattern, int groupIndex) {
    if (text == null || pattern == null) {
      return null;
    }
    
    try {
      java.util.regex.Pattern compiledPattern = java.util.regex.Pattern.compile(pattern);
      java.util.regex.Matcher matcher = compiledPattern.matcher(text);
      
      if (matcher.find() && groupIndex > 0 && groupIndex <= matcher.groupCount()) {
        return matcher.group(groupIndex);
      }
      
      return null;
    } catch (Exception e) {
      return null;
    }
  }
}