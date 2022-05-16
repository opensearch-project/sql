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
import org.opensearch.sql.opensearch.storage.script.filter.lucene.relevance.MatchQuery;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class MatchQueryTest {
  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());
  private final MatchQuery matchQuery = new MatchQuery();
  private final FunctionName match = FunctionName.of("match");

  private NamedArgumentExpression namedArgument(String name, String value) {
    return dsl.namedArgument(name, DSL.literal(value));
  }

  @Test
  public void test_SemanticCheckException_when_no_arguments() {
    List<Expression> arguments = List.of();
    assertThrows(SemanticCheckException.class,
        () -> matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_one_argument() {
    List<Expression> arguments = List.of(namedArgument("field", "field_value"));
    assertThrows(SemanticCheckException.class,
        () -> matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_SemanticCheckException_when_invalid_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("unsupported", "unsupported_value"));
    Assertions.assertThrows(SemanticCheckException.class,
        () -> matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_analyzer_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("analyzer", "standard")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void build_succeeds_with_two_arguments() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"));
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_auto_generate_synonyms_phrase_query_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("auto_generate_synonyms_phrase_query", "true")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_fuzziness_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("fuzziness", "AUTO")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_max_expansions_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("max_expansions", "50")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_prefix_length_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("prefix_length", "0")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_fuzzy_transpositions_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("fuzzy_transpositions", "true")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_fuzzy_rewrite_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("fuzzy_rewrite", "constant_score")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_lenient_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("lenient", "false")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_operator_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("operator", "OR")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_minimum_should_match_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("minimum_should_match", "3")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_zero_terms_query_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("zero_terms_query", "NONE")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  @Test
  public void test_boost_parameter() {
    List<Expression> arguments = List.of(
        namedArgument("field", "field_value"),
        namedArgument("query", "query_value"),
        namedArgument("boost", "1")
    );
    Assertions.assertNotNull(matchQuery.build(new MatchExpression(arguments)));
  }

  private class MatchExpression extends FunctionExpression {
    public MatchExpression(List<Expression> arguments) {
      super(MatchQueryTest.this.match, arguments);
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
