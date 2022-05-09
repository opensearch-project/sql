package org.opensearch.sql.opensearch.storage.script.filter.lucene;


import static org.junit.Assert.assertNotNull;
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
  public void test_SemanticCheckException_when_no_arguments() {

    List<Expression> arguments = List.of();
    FunctionExpression fe = new MatchPhraseExpression(arguments);
    assertThrows(SemanticCheckException.class, () -> matchPhraseQuery.build(fe));
  }

  @Test
  public void test_SemanticCheckException_when_one_argument() {
    List<Expression> arguments = List.of(dsl.namedArgument("field",
        DSL.literal("test")));
    FunctionExpression fe = new MatchPhraseExpression(arguments);
    assertThrows(SemanticCheckException.class, () -> matchPhraseQuery.build(fe));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        dsl.namedArgument("field", DSL.literal("test")),
        dsl.namedArgument("query", DSL.literal("test2")),
        dsl.namedArgument("unsupported", DSL.literal(3)));
    FunctionExpression fe = new MatchPhraseExpression(arguments);
    Assertions.assertThrows(SemanticCheckException.class, () -> matchPhraseQuery.build(fe));
  }

  @Test
  public void build_succeeds_with_two_arguments() {

    List<Expression> arguments = List.of(
        dsl.namedArgument("field", DSL.literal("test")),
        dsl.namedArgument("query", DSL.literal("test2")));
    FunctionExpression fe = new MatchPhraseExpression(arguments);
    Assertions.assertNotNull(matchPhraseQuery.build(fe));
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
