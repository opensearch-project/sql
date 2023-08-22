/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.visitor;

import com.google.common.collect.Sets;
import java.util.Set;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.MathOperatorContext;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.RegexpPredicateContext;
import org.opensearch.sql.legacy.antlr.parser.OpenSearchLegacySqlParser.ScalarFunctionCallContext;
import org.opensearch.sql.legacy.exception.SqlFeatureNotImplementedException;
import org.opensearch.sql.legacy.utils.StringUtils;

public class UnsupportedSemanticVerifier {

  private static final Set<String> mathConstants = Sets.newHashSet("e", "pi");

  private static final Set<String> supportedNestedFunctions =
      Sets.newHashSet("nested", "reverse_nested", "score", "match_query", "matchquery");

  /**
   * The following two sets include the functions and operators that have requested or issued by
   * users but the plugin does not support yet.
   */
  private static final Set<String> unsupportedFunctions =
      Sets.newHashSet("adddate", "addtime", "datetime", "greatest", "least");

  private static final Set<String> unsupportedOperators = Sets.newHashSet("div");

  /**
   * The scalar function calls are separated into (a)typical function calls; (b)nested function
   * calls with functions as arguments, like abs(log(...)); (c)aggregations with functions as
   * aggregators, like max(abs(....)). Currently, we do not support nested functions or nested
   * aggregations, aka (b) and (c). However, for the special EsFunctions included in the
   * [supportedNestedFunctions] set, we have supported them in nested function calls and
   * aggregations (b&c). Besides, the math constants included in the [mathConstants] set are
   * regraded as scalar functions, but they are working well in the painless script.
   *
   * <p>Thus, the types of functions to throw exceptions: (I)case (b) except that the arguments are
   * from the [mathConstants] set; (II) case (b) except that the arguments are from the
   * [supportedNestedFunctions] set; (III) case (c) except that the aggregators are from thet
   * [supportedNestedFunctions] set.
   */
  public static void verify(ScalarFunctionCallContext ctx) {
    String funcName = StringUtils.toLower(ctx.scalarFunctionName().getText());

    // type (III)
    if (ctx.parent.parent instanceof OpenSearchLegacySqlParser.FunctionAsAggregatorFunctionContext
        && !(supportedNestedFunctions.contains(StringUtils.toLower(funcName)))) {
      throw new SqlFeatureNotImplementedException(
          StringUtils.format(
              "Aggregation calls with function aggregator like [%s] are not supported yet",
              ctx.parent.parent.getText()));

      // type (I) and (II)
    } else if (ctx.parent.parent instanceof OpenSearchLegacySqlParser.NestedFunctionArgsContext
        && !(mathConstants.contains(funcName) || supportedNestedFunctions.contains(funcName))) {
      throw new SqlFeatureNotImplementedException(
          StringUtils.format(
              "Nested function calls like [%s] are not supported yet",
              ctx.parent.parent.parent.getText()));

      // unsupported functions
    } else if (unsupportedFunctions.contains(funcName)) {
      throw new SqlFeatureNotImplementedException(
          StringUtils.format("Function [%s] is not supported yet", funcName));
    }
  }

  public static void verify(MathOperatorContext ctx) {
    if (unsupportedOperators.contains(StringUtils.toLower(ctx.getText()))) {
      throw new SqlFeatureNotImplementedException(
          StringUtils.format("Operator [%s] is not supported yet", ctx.getText()));
    }
  }

  public static void verify(RegexpPredicateContext ctx) {
    throw new SqlFeatureNotImplementedException("Regexp predicate is not supported yet");
  }
}
