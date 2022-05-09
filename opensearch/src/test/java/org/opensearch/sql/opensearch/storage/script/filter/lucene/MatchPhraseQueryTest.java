package org.opensearch.sql.opensearch.storage.script.filter.lucene;


import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchPhraseQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class MatchPhraseQueryTest {

  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());
  private final MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery();
  private final FunctionName matchPhrase = FunctionName.of("match_phrase");

  private NamedArgumentExpression namedArgument(String name, String value) {
    return dsl.namedArgument(name, DSL.literal(value));
  }

  @Test
  public void test_SemanticCheckException_when_no_arguments() {

    List<Expression> arguments = List.of();
    assertThrows(SemanticCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_one_argument() {
    List<Expression> arguments = List.of(namedArgument("field", "test"));
    assertThrows(SemanticCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "test"),
        namedArgument("query", "test2"),
        namedArgument("unsupported", "3"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_analyzer_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "t1"),
        namedArgument("query", "t2"),
        namedArgument("analyzer", "standard")
    );
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void build_succeeds_with_two_arguments() {
    List<Expression> arguments = List.of(
        namedArgument("field", "test"),
        namedArgument("query", "test2"));
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_slop_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "t1"),
        namedArgument("query", "t2"),
        namedArgument("slop", "2")
    );
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  @Test
  public void test_zero_terms_query_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "t1"),
        namedArgument("query", "t2"),
        namedArgument("zero_terms_query", "ALL")
    );
    Assertions.assertNotNull(matchPhraseQuery.build(new MatchPhraseExpression(arguments)));
  }

  private class MatchPhraseExpression extends FunctionExpression {
    public MatchPhraseExpression(List<Expression> arguments) {
      super(MatchPhraseQueryTest.this.matchPhrase, arguments);
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
