/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;

public class OpenSearchFunctionsTest extends ExpressionTestBase {
  private final NamedArgumentExpression field =
      new NamedArgumentExpression("field", DSL.literal("message"));
  private final NamedArgumentExpression fields =
      new NamedArgumentExpression(
          "fields",
          DSL.literal(
              new ExprTupleValue(
                  new LinkedHashMap<>(
                      Map.of(
                          "title", ExprValueUtils.floatValue(1.F),
                          "body", ExprValueUtils.floatValue(.3F))))));
  private final NamedArgumentExpression query =
      new NamedArgumentExpression("query", DSL.literal("search query"));
  private final NamedArgumentExpression analyzer =
      new NamedArgumentExpression("analyzer", DSL.literal("keyword"));
  private final NamedArgumentExpression autoGenerateSynonymsPhrase =
      new NamedArgumentExpression("auto_generate_synonyms_phrase", DSL.literal("true"));
  private final NamedArgumentExpression fuzziness =
      new NamedArgumentExpression("fuzziness", DSL.literal("AUTO"));
  private final NamedArgumentExpression maxExpansions =
      new NamedArgumentExpression("max_expansions", DSL.literal("10"));
  private final NamedArgumentExpression prefixLength =
      new NamedArgumentExpression("prefix_length", DSL.literal("1"));
  private final NamedArgumentExpression fuzzyTranspositions =
      new NamedArgumentExpression("fuzzy_transpositions", DSL.literal("false"));
  private final NamedArgumentExpression fuzzyRewrite =
      new NamedArgumentExpression("fuzzy_rewrite", DSL.literal("rewrite method"));
  private final NamedArgumentExpression lenient =
      new NamedArgumentExpression("lenient", DSL.literal("true"));
  private final NamedArgumentExpression operator =
      new NamedArgumentExpression("operator", DSL.literal("OR"));
  private final NamedArgumentExpression minimumShouldMatch =
      new NamedArgumentExpression("minimum_should_match", DSL.literal("1"));
  private final NamedArgumentExpression zeroTermsQueryAll =
      new NamedArgumentExpression("zero_terms_query", DSL.literal("ALL"));
  private final NamedArgumentExpression zeroTermsQueryNone =
      new NamedArgumentExpression("zero_terms_query", DSL.literal("None"));
  private final NamedArgumentExpression boost =
      new NamedArgumentExpression("boost", DSL.literal("2.0"));
  private final NamedArgumentExpression slop =
      new NamedArgumentExpression("slop", DSL.literal("3"));
}
