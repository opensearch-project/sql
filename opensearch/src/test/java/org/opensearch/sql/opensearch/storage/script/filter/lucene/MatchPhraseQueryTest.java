/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;


import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.planner.physical.SessionContext;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhraseQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class MatchPhraseQueryTest {

  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());
  private final MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery();
  private final FunctionName matchPhrase = FunctionName.of("match_phrase");

  @Test
  public void test_SyntaxCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(SyntaxCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument() {
    List<Expression> arguments = List.of(dsl.namedArgument("field", "test"));
    assertThrows(SyntaxCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        dsl.namedArgument("field", "test"),
        dsl.namedArgument("query", "test2"),
        dsl.namedArgument("unsupported", "3"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_analyzer_parameter() {
    List<Expression> arguments = List.of(
        dsl.namedArgument("field", "t1"),
        dsl.namedArgument("query", "t2"),
        dsl.namedArgument("analyzer", "standard")
    );
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void build_succeeds_with_two_arguments() {
    List<Expression> arguments = List.of(
        dsl.namedArgument("field", "test"),
        dsl.namedArgument("query", "test2"));
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_slop_parameter() {
    List<Expression> arguments = List.of(
        dsl.namedArgument("field", "t1"),
        dsl.namedArgument("query", "t2"),
        dsl.namedArgument("slop", "2")
    );
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_zero_terms_query_parameter() {
    List<Expression> arguments = List.of(
        dsl.namedArgument("field", "t1"),
        dsl.namedArgument("query", "t2"),
        dsl.namedArgument("zero_terms_query", "ALL")
    );
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_zero_terms_query_parameter_lower_case() {
    List<Expression> arguments = List.of(
        dsl.namedArgument("field", "t1"),
        dsl.namedArgument("query", "t2"),
        dsl.namedArgument("zero_terms_query", "all")
    );
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  private class MatchPhraseExpression extends FunctionExpression {
    public MatchPhraseExpression(List<Expression> arguments) {
      super(MatchPhraseQueryTest.this.matchPhrase, arguments);
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv,
                             SessionContext sessionContext) {
      return null;
    }

    @Override
    public ExprType type() {
      return null;
    }
  }
}
