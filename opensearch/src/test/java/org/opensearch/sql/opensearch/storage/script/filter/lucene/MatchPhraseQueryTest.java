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
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;
import org.opensearch.sql.opensearch.expression.OpenSearchDSL;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhraseQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class MatchPhraseQueryTest {

  private final MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery();
  private final FunctionName matchPhraseName = FunctionName.of("matchphrase");
  private final FunctionName matchPhraseWithUnderscoreName = FunctionName.of("match_phrase");
  private final FunctionName matchPhraseQueryName = FunctionName.of("matchphrasequery");

  @Test
  public void test_SyntaxCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("test", OpenSearchTextType.of())));
    assertThrows(
        SyntaxCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_invalid_parameter() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("test", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "test2"),
            OpenSearchDSL.namedArgument("unsupported", "3"));
    Assertions.assertThrows(
        SemanticCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_analyzer_parameter() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("analyzer", "standard"));
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void build_succeeds_with_two_arguments() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("test", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "test2"));
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_slop_parameter() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("slop", "2"));
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_zero_terms_query_parameter() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("zero_terms_query", "ALL"));
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_zero_terms_query_parameter_lower_case() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("zero_terms_query", "all"));
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments_match_phrase_syntax() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class,
        () ->
            matchPhraseQuery.build(
                new MatchPhraseExpression(arguments, matchPhraseWithUnderscoreName)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument_match_phrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("test", OpenSearchTextType.of())));
    assertThrows(
        SyntaxCheckException.class,
        () ->
            matchPhraseQuery.build(
                new MatchPhraseExpression(arguments, matchPhraseWithUnderscoreName)));
  }

  @Test
  public void test_SyntaxCheckException_when_invalid_parameter_match_phrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("test", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "test2"),
            OpenSearchDSL.namedArgument("unsupported", "3"));
    Assertions.assertThrows(
        SemanticCheckException.class,
        () ->
            matchPhraseQuery.build(
                new MatchPhraseExpression(arguments, matchPhraseWithUnderscoreName)));
  }

  @Test
  public void test_analyzer_parameter_match_phrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("analyzer", "standard"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(
            new MatchPhraseExpression(arguments, matchPhraseWithUnderscoreName)));
  }

  @Test
  public void build_succeeds_with_two_arguments_match_phrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("test", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "test2"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(
            new MatchPhraseExpression(arguments, matchPhraseWithUnderscoreName)));
  }

  @Test
  public void test_slop_parameter_match_phrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("slop", "2"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(
            new MatchPhraseExpression(arguments, matchPhraseWithUnderscoreName)));
  }

  @Test
  public void test_zero_terms_query_parameter_match_phrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("zero_terms_query", "ALL"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(
            new MatchPhraseExpression(arguments, matchPhraseWithUnderscoreName)));
  }

  @Test
  public void test_zero_terms_query_parameter_lower_case_match_phrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("zero_terms_query", "all"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(
            new MatchPhraseExpression(arguments, matchPhraseWithUnderscoreName)));
  }

  @Test
  public void test_SyntaxCheckException_when_no_arguments_matchphrase_syntax() {
    List<Expression> arguments = List.of();
    assertThrows(
        SyntaxCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments, matchPhraseQueryName)));
  }

  @Test
  public void test_SyntaxCheckException_when_one_argument_matchphrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("test", OpenSearchTextType.of())));
    assertThrows(
        SyntaxCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments, matchPhraseQueryName)));
  }

  @Test
  public void test_SyntaxCheckException_when_invalid_parameter_matchphrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("test", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "test2"),
            OpenSearchDSL.namedArgument("unsupported", "3"));
    Assertions.assertThrows(
        SemanticCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments, matchPhraseQueryName)));
  }

  @Test
  public void test_analyzer_parameter_matchphrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("analyzer", "standard"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(new MatchPhraseExpression(arguments, matchPhraseQueryName)));
  }

  @Test
  public void build_succeeds_with_two_arguments_matchphrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("test", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "test2"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(new MatchPhraseExpression(arguments, matchPhraseQueryName)));
  }

  @Test
  public void test_slop_parameter_matchphrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("slop", "2"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(new MatchPhraseExpression(arguments, matchPhraseQueryName)));
  }

  @Test
  public void test_zero_terms_query_parameter_matchphrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("zero_terms_query", "ALL"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(new MatchPhraseExpression(arguments, matchPhraseQueryName)));
  }

  @Test
  public void test_zero_terms_query_parameter_lower_case_matchphrase_syntax() {
    List<Expression> arguments =
        List.of(
            DSL.namedArgument("field", new ReferenceExpression("t1", OpenSearchTextType.of())),
            OpenSearchDSL.namedArgument("query", "t2"),
            OpenSearchDSL.namedArgument("zero_terms_query", "all"));
    Assertions.assertNotNull(
        matchPhraseQuery.build(new MatchPhraseExpression(arguments, matchPhraseQueryName)));
  }

  private class MatchPhraseExpression extends FunctionExpression {
    public MatchPhraseExpression(List<Expression> arguments) {
      super(matchPhraseName, arguments);
    }

    public MatchPhraseExpression(List<Expression> arguments, FunctionName funcName) {
      super(funcName, arguments);
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      return null;
    }

    @Override
    public ExprType type() {
      return null;
    }
  }
}
