/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.opensearch.storage.serialization.ExpressionSerializer;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class FilterQueryBuilderTest {

  private final DSL dsl = new ExpressionConfig().dsl(new ExpressionConfig().functionRepository());

  private static Stream<LiteralExpression> numericCastSource() {
    return Stream.of(literal((byte) 1), literal((short) -1), literal(
        1), literal(21L), literal(3.14F), literal(3.1415D), literal(true), literal("1"));
  }

  private static Stream<LiteralExpression> booleanTrueCastSource() {
    return Stream.of(literal((byte) 1), literal((short) -1), literal(
        1), literal(42L), literal(3.14F), literal(3.1415D), literal(true), literal("true"));
  }

  private static Stream<LiteralExpression> booleanFalseCastSource() {
    return Stream.of(literal((byte) 0), literal((short) 0), literal(
        0), literal(0L), literal(0.0F), literal(0.0D), literal(false), literal("false"));
  }

  @Mock
  private ExpressionSerializer serializer;

  private FilterQueryBuilder filterQueryBuilder;

  @BeforeEach
  void set_up() {
    filterQueryBuilder = new FilterQueryBuilder(serializer);
  }

  @Test
  void should_build_term_query_for_equality_expression() {
    assertJsonEquals(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"name\" : {\n"
            + "      \"value\" : \"John\",\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.equal(
                ref("name", STRING), literal("John"))));
  }

  @Test
  void should_build_range_query_for_comparison_expression() {
    Expression[] params = {ref("age", INTEGER), literal(30)};
    Map<Expression, Object[]> ranges = ImmutableMap.of(
        dsl.less(params), new Object[]{null, 30, true, false},
        dsl.greater(params), new Object[]{30, null, false, true},
        dsl.lte(params), new Object[]{null, 30, true, true},
        dsl.gte(params), new Object[]{30, null, true, true});

    ranges.forEach((expr, range) ->
        assertJsonEquals(
            "{\n"
                + "  \"range\" : {\n"
                + "    \"age\" : {\n"
                + "      \"from\" : " + range[0] + ",\n"
                + "      \"to\" : " + range[1] + ",\n"
                + "      \"include_lower\" : " + range[2] + ",\n"
                + "      \"include_upper\" : " + range[3] + ",\n"
                + "      \"boost\" : 1.0\n"
                + "    }\n"
                + "  }\n"
                + "}",
            buildQuery(expr)));
  }

  @Test
  void should_build_wildcard_query_for_like_expression() {
    assertJsonEquals(
        "{\n"
            + "  \"wildcard\" : {\n"
            + "    \"name\" : {\n"
            + "      \"wildcard\" : \"*John?\",\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.like(
                ref("name", STRING), literal("%John_"))));
  }

  @Test
  void should_build_script_query_for_unsupported_lucene_query() {
    mockToStringSerializer();
    assertJsonEquals(
        "{\n"
            + "  \"script\" : {\n"
            + "    \"script\" : {\n"
            + "      \"source\" : \"is not null(age)\",\n"
            + "      \"lang\" : \"opensearch_query_expression\"\n"
            + "    },\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.isnotnull(ref("age", INTEGER))));
  }

  @Test
  void should_build_script_query_for_function_expression() {
    mockToStringSerializer();
    assertJsonEquals(
        "{\n"
            + "  \"script\" : {\n"
            + "    \"script\" : {\n"
            + "      \"source\" : \"=(abs(age), 30)\",\n"
            + "      \"lang\" : \"opensearch_query_expression\"\n"
            + "    },\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.equal(
                dsl.abs(ref("age", INTEGER)), literal(30))));
  }

  @Test
  void should_build_script_query_for_comparison_between_fields() {
    mockToStringSerializer();
    assertJsonEquals(
        "{\n"
            + "  \"script\" : {\n"
            + "    \"script\" : {\n"
            + "      \"source\" : \"=(age1, age2)\",\n"
            + "      \"lang\" : \"opensearch_query_expression\"\n"
            + "    },\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.equal(
                ref("age1", INTEGER), ref("age2", INTEGER))));
  }

  @Test
  void should_build_bool_query_for_and_or_expression() {
    String[] names = { "filter", "should" };
    FunctionExpression expr1 = dsl.equal(ref("name", STRING), literal("John"));
    FunctionExpression expr2 = dsl.equal(ref("age", INTEGER), literal(30));
    Expression[] exprs = {
        dsl.and(expr1, expr2),
        dsl.or(expr1, expr2)
    };

    for (int i = 0; i < names.length; i++) {
      assertJsonEquals(
          "{\n"
              + "  \"bool\" : {\n"
              + "    \"" + names[i] + "\" : [\n"
              + "      {\n"
              + "        \"term\" : {\n"
              + "          \"name\" : {\n"
              + "            \"value\" : \"John\",\n"
              + "            \"boost\" : 1.0\n"
              + "          }\n"
              + "        }\n"
              + "      },\n"
              + "      {\n"
              + "        \"term\" : {\n"
              + "          \"age\" : {\n"
              + "            \"value\" : 30,\n"
              + "            \"boost\" : 1.0\n"
              + "          }\n"
              + "        }\n"
              + "      }\n"
              + "    ],\n"
              + "    \"adjust_pure_negative\" : true,\n"
              + "    \"boost\" : 1.0\n"
              + "  }\n"
              + "}",
          buildQuery(exprs[i]));
    }
  }

  @Test
  void should_build_bool_query_for_not_expression() {
    assertJsonEquals(
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must_not\" : [\n"
            + "      {\n"
            + "        \"term\" : {\n"
            + "          \"age\" : {\n"
            + "            \"value\" : 30,\n"
            + "            \"boost\" : 1.0\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ],\n"
            + "    \"adjust_pure_negative\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.not(
                dsl.equal(
                    ref("age", INTEGER), literal(30)))));
  }

  @Test
  void should_use_keyword_for_multi_field_in_equality_expression() {
    assertJsonEquals(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"name.keyword\" : {\n"
            + "      \"value\" : \"John\",\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.equal(
                ref("name", OPENSEARCH_TEXT_KEYWORD), literal("John"))));
  }

  @Test
  void should_use_keyword_for_multi_field_in_like_expression() {
    assertJsonEquals(
        "{\n"
            + "  \"wildcard\" : {\n"
            + "    \"name.keyword\" : {\n"
            + "      \"wildcard\" : \"John*\",\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.like(
                ref("name", OPENSEARCH_TEXT_KEYWORD), literal("John%"))));
  }

  @Test
  void should_build_match_query_with_default_parameters() {
    assertJsonEquals(
        "{\n"
            + "  \"match\" : {\n"
            + "    \"message\" : {\n"
            + "      \"query\" : \"search query\",\n"
            + "      \"operator\" : \"OR\",\n"
            + "      \"prefix_length\" : 0,\n"
            + "      \"max_expansions\" : 50,\n"
            + "      \"fuzzy_transpositions\" : true,\n"
            + "      \"lenient\" : false,\n"
            + "      \"zero_terms_query\" : \"NONE\",\n"
            + "      \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.match(
                dsl.namedArgument("field", literal("message")),
                dsl.namedArgument("query", literal("search query")))));
  }

  @Test
  void should_build_match_query_with_custom_parameters() {
    assertJsonEquals(
        "{\n"
            + "  \"match\" : {\n"
            + "    \"message\" : {\n"
            + "      \"query\" : \"search query\",\n"
            + "      \"operator\" : \"AND\",\n"
            + "      \"analyzer\" : \"keyword\","
            + "      \"fuzziness\" : \"AUTO\","
            + "      \"prefix_length\" : 0,\n"
            + "      \"max_expansions\" : 50,\n"
            + "      \"minimum_should_match\" : \"3\","
            + "      \"fuzzy_rewrite\" : \"top_terms_1\","
            + "      \"fuzzy_transpositions\" : false,\n"
            + "      \"lenient\" : false,\n"
            + "      \"zero_terms_query\" : \"ALL\",\n"
            + "      \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "      \"boost\" : 2.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.match(
                dsl.namedArgument("field", literal("message")),
                dsl.namedArgument("query", literal("search query")),
                dsl.namedArgument("operator", literal("AND")),
                dsl.namedArgument("analyzer", literal("keyword")),
                dsl.namedArgument("auto_generate_synonyms_phrase_query", literal("true")),
                dsl.namedArgument("fuzziness", literal("AUTO")),
                dsl.namedArgument("max_expansions", literal("50")),
                dsl.namedArgument("prefix_length", literal("0")),
                dsl.namedArgument("fuzzy_transpositions", literal("false")),
                dsl.namedArgument("fuzzy_rewrite", literal("top_terms_1")),
                dsl.namedArgument("lenient", literal("false")),
                dsl.namedArgument("minimum_should_match", literal("3")),
                dsl.namedArgument("zero_terms_query", literal("ALL")),
                dsl.namedArgument("boost", literal("2.0")))));
  }

  @Test
  void match_invalid_parameter() {
    FunctionExpression expr = dsl.match(
        dsl.namedArgument("field", literal("message")),
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("invalid_parameter", literal("invalid_value")));
    var msg = assertThrows(SemanticCheckException.class, () -> buildQuery(expr)).getMessage();
    assertTrue(msg.startsWith("Parameter invalid_parameter is invalid for match function."));
  }

  @Test
  void match_disallow_duplicate_parameter() {
    FunctionExpression expr = dsl.match(
        dsl.namedArgument("field", literal("message")),
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("analyzer", literal("keyword")),
        dsl.namedArgument("AnalYzer", literal("english")));
    var msg = assertThrows(SemanticCheckException.class, () -> buildQuery(expr)).getMessage();
    assertEquals("Parameter 'analyzer' can only be specified once.", msg);
  }

  @Test
  void match_disallow_duplicate_query() {
    FunctionExpression expr = dsl.match(
        dsl.namedArgument("field", literal("message")),
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("analyzer", literal("keyword")),
        dsl.namedArgument("QUERY", literal("something")));
    var msg = assertThrows(SemanticCheckException.class, () -> buildQuery(expr)).getMessage();
    assertEquals("Parameter 'query' can only be specified once.", msg);
  }

  @Test
  void match_disallow_duplicate_field() {
    FunctionExpression expr = dsl.match(
        dsl.namedArgument("field", literal("message")),
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("analyzer", literal("keyword")),
        dsl.namedArgument("Field", literal("something")));
    var msg = assertThrows(SemanticCheckException.class, () -> buildQuery(expr)).getMessage();
    assertEquals("Parameter 'field' can only be specified once.", msg);
  }

  @Test
  void match_missing_field() {
    FunctionExpression expr = dsl.match(
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("analyzer", literal("keyword")));
    var msg = assertThrows(SemanticCheckException.class, () -> buildQuery(expr)).getMessage();
    assertEquals("'field' parameter is missing.", msg);
  }

  @Test
  void match_missing_query() {
    FunctionExpression expr = dsl.match(
            dsl.namedArgument("field", literal("field1")),
            dsl.namedArgument("analyzer", literal("keyword")));
    var msg = assertThrows(SemanticCheckException.class, () -> buildQuery(expr)).getMessage();
    assertEquals("'query' parameter is missing", msg);
  }

  @Test
  void should_build_match_phrase_query_with_default_parameters() {
    assertJsonEquals(
            "{\n"
            + "  \"match_phrase\" : {\n"
            + "    \"message\" : {\n"
            + "      \"query\" : \"search query\",\n"
            + "      \"slop\" : 0,\n"
            + "      \"zero_terms_query\" : \"NONE\",\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.match_phrase(
                dsl.namedArgument("field", literal("message")),
                dsl.namedArgument("query", literal("search query")))));
  }

  @Test
  void should_build_multi_match_query_with_default_parameters_single_field() {
    assertJsonEquals("{\n"
            + "  \"multi_match\" : {\n"
            + "    \"query\" : \"search query\",\n"
            + "    \"fields\" : [\n"
            + "      \"field1^1.0\"\n"
            + "    ],\n"
            + "    \"type\" : \"best_fields\",\n"
            + "    \"operator\" : \"OR\",\n"
            + "    \"slop\" : 0,\n"
            + "    \"prefix_length\" : 0,\n"
            + "    \"max_expansions\" : 50,\n"
            + "    \"zero_terms_query\" : \"NONE\",\n"
            + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "    \"fuzzy_transpositions\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.multi_match(
            dsl.namedArgument("fields", DSL.literal(new ExprTupleValue(
                new LinkedHashMap<>(ImmutableMap.of(
                    "field1", ExprValueUtils.floatValue(1.F)))))),
            dsl.namedArgument("query", literal("search query")))));
  }

  @Test
  void should_build_multi_match_query_with_default_parameters_all_fields() {
    assertJsonEquals("{\n"
            + "  \"multi_match\" : {\n"
            + "    \"query\" : \"search query\",\n"
            + "    \"fields\" : [\n"
            + "      \"*^1.0\"\n"
            + "    ],\n"
            + "    \"type\" : \"best_fields\",\n"
            + "    \"operator\" : \"OR\",\n"
            + "    \"slop\" : 0,\n"
            + "    \"prefix_length\" : 0,\n"
            + "    \"max_expansions\" : 50,\n"
            + "    \"zero_terms_query\" : \"NONE\",\n"
            + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "    \"fuzzy_transpositions\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.multi_match(
            dsl.namedArgument("fields", DSL.literal(new ExprTupleValue(
                new LinkedHashMap<>(ImmutableMap.of(
                    "*", ExprValueUtils.floatValue(1.F)))))),
            dsl.namedArgument("query", literal("search query")))));
  }

  @Test
  void should_build_multi_match_query_with_default_parameters_no_fields() {
    assertJsonEquals("{\n"
            + "  \"multi_match\" : {\n"
            + "    \"query\" : \"search query\",\n"
            + "    \"fields\" : [],\n"
            + "    \"type\" : \"best_fields\",\n"
            + "    \"operator\" : \"OR\",\n"
            + "    \"slop\" : 0,\n"
            + "    \"prefix_length\" : 0,\n"
            + "    \"max_expansions\" : 50,\n"
            + "    \"zero_terms_query\" : \"NONE\",\n"
            + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "    \"fuzzy_transpositions\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.multi_match(
            dsl.namedArgument("fields", DSL.literal(new ExprTupleValue(
                new LinkedHashMap<>(ImmutableMap.of())))),
            dsl.namedArgument("query", literal("search query")))));
  }

  // Note: we can't test `multi_match` and `simple_query_string` without weight(s)

  @Test
  void should_build_multi_match_query_with_default_parameters_multiple_fields() {
    var expected = "{\n"
            + "  \"multi_match\" : {\n"
            + "    \"query\" : \"search query\",\n"
            + "    \"fields\" : [%s],\n"
            + "    \"type\" : \"best_fields\",\n"
            + "    \"operator\" : \"OR\",\n"
            + "    \"slop\" : 0,\n"
            + "    \"max_expansions\" : 50,\n"
            + "    \"prefix_length\" : 0,\n"
            + "    \"zero_terms_query\" : \"NONE\",\n"
            + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "    \"fuzzy_transpositions\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "  }\n"
            + "}";
    var actual = buildQuery(dsl.multi_match(
        dsl.namedArgument("fields", DSL.literal(new ExprTupleValue(
            new LinkedHashMap<>(ImmutableMap.of(
                "field1", ExprValueUtils.floatValue(1.F),
                "field2", ExprValueUtils.floatValue(.3F)))))),
        dsl.namedArgument("query", literal("search query"))));

    var ex1 = String.format(expected, "\"field1^1.0\", \"field2^0.3\"");
    var ex2 = String.format(expected, "\"field2^0.3\", \"field1^1.0\"");
    assertTrue(new JSONObject(ex1).similar(new JSONObject(actual))
        || new JSONObject(ex2).similar(new JSONObject(actual)),
        StringUtils.format("Actual %s doesn't match neither expected %s nor %s", actual, ex1, ex2));
  }

  @Test
  void should_build_multi_match_query_with_custom_parameters() {
    var expected = "{\n"
            + "  \"multi_match\" : {\n"
            + "    \"query\" : \"search query\",\n"
            + "    \"fields\" : [%s],\n"
            + "    \"type\" : \"phrase_prefix\",\n"
            + "    \"operator\" : \"AND\",\n"
            + "    \"analyzer\" : \"keyword\",\n"
            + "    \"slop\" : 1,\n"
            + "    \"fuzziness\" : \"AUTO:2,4\",\n"
            + "    \"prefix_length\" : 1,\n"
            + "    \"max_expansions\" : 3,\n"
            + "    \"minimum_should_match\" : \"3\",\n"
            + "    \"tie_breaker\" : 1.0,\n"
            + "    \"lenient\" : false,\n"
            + "    \"cutoff_frequency\" : 4.3,\n"
            + "    \"zero_terms_query\" : \"ALL\",\n"
            + "    \"auto_generate_synonyms_phrase_query\" : false,\n"
            + "    \"fuzzy_transpositions\" : false,\n"
            + "    \"boost\" : 2.0\n"
            + "  }\n"
            + "}";
    var actual = buildQuery(
            dsl.multi_match(
                dsl.namedArgument("fields", DSL.literal(
                    ExprValueUtils.tupleValue(ImmutableMap.of("field1", 1.F, "field2", .3F)))),
                dsl.namedArgument("query", literal("search query")),
                dsl.namedArgument("analyzer", literal("keyword")),
                dsl.namedArgument("auto_generate_synonyms_phrase_query", literal("false")),
                dsl.namedArgument("cutoff_frequency", literal("4.3")),
                dsl.namedArgument("fuzziness", literal("AUTO:2,4")),
                dsl.namedArgument("fuzzy_transpositions", literal("false")),
                dsl.namedArgument("lenient", literal("false")),
                dsl.namedArgument("max_expansions", literal("3")),
                dsl.namedArgument("minimum_should_match", literal("3")),
                dsl.namedArgument("operator", literal("AND")),
                dsl.namedArgument("prefix_length", literal("1")),
                dsl.namedArgument("slop", literal("1")),
                dsl.namedArgument("tie_breaker", literal("1")),
                dsl.namedArgument("type", literal("phrase_prefix")),
                dsl.namedArgument("zero_terms_query", literal("ALL")),
                dsl.namedArgument("boost", literal("2.0"))));

    var ex1 = String.format(expected, "\"field1^1.0\", \"field2^0.3\"");
    var ex2 = String.format(expected, "\"field2^0.3\", \"field1^1.0\"");
    assertTrue(new JSONObject(ex1).similar(new JSONObject(actual))
        || new JSONObject(ex2).similar(new JSONObject(actual)),
        StringUtils.format("Actual %s doesn't match neither expected %s nor %s", actual, ex1, ex2));
  }

  @Test
  void multi_match_invalid_parameter() {
    FunctionExpression expr = dsl.multi_match(
        dsl.namedArgument("fields", DSL.literal(
            new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
                "field1", ExprValueUtils.floatValue(1.F),
                "field2", ExprValueUtils.floatValue(.3F)))))),
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("invalid_parameter", literal("invalid_value")));
    assertThrows(SemanticCheckException.class, () -> buildQuery(expr),
        "Parameter invalid_parameter is invalid for match function.");
  }

  @Test
  void should_build_match_phrase_query_with_custom_parameters() {
    assertJsonEquals(
            "{\n"
            + "  \"match_phrase\" : {\n"
            + "    \"message\" : {\n"
            + "      \"query\" : \"search query\",\n"
            + "      \"analyzer\" : \"keyword\","
            + "      \"slop\" : 2,\n"
            + "      \"zero_terms_query\" : \"ALL\",\n"
            + "      \"boost\" : 1.2\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.match_phrase(
                dsl.namedArgument("boost", literal("1.2")),
                dsl.namedArgument("field", literal("message")),
                dsl.namedArgument("query", literal("search query")),
                dsl.namedArgument("analyzer", literal("keyword")),
                dsl.namedArgument("slop", literal("2")),
                dsl.namedArgument("zero_terms_query", literal("ALL")))));
  }

  @Test
  void query_invalid_parameter() {
    FunctionExpression expr = dsl.query(
            dsl.namedArgument("invalid_parameter", literal("invalid_value")));
    assertThrows(SemanticCheckException.class, () -> buildQuery(expr),
            "Parameter invalid_parameter is invalid for query function.");
  }

  @Test
  void query_invalid_fields_parameter_exception_message() {
    FunctionExpression expr = dsl.query(
        dsl.namedArgument("fields", literal("field1")),
        dsl.namedArgument("query", literal("search query")));

    var exception = assertThrows(SemanticCheckException.class, () -> buildQuery(expr));
    assertEquals("Parameter fields is invalid for query function.", exception.getMessage());
  }

  @Test
  void should_build_query_query_with_default_parameters() {
    var expected = "{\n"
            + "  \"query_string\" : {\n"
            + "    \"query\" : \"field1:query_value\",\n"
            + "    \"fields\" : [],\n"
            + "    \"type\" : \"best_fields\",\n"
            + "    \"default_operator\" : \"or\",\n"
            + "    \"max_determinized_states\" : 10000,\n"
            + "    \"enable_position_increments\" : true,\n"
            + "    \"fuzziness\" : \"AUTO\",\n"
            + "    \"fuzzy_prefix_length\" : 0,\n"
            + "    \"fuzzy_max_expansions\" : 50,\n"
            + "    \"phrase_slop\" : 0,\n"
            + "    \"escape\" : false,\n"
            + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "    \"fuzzy_transpositions\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";

    assertJsonEquals(expected, buildQuery(dsl.query(
            dsl.namedArgument("query", literal("field1:query_value")))));
  }

  @Test
  void should_build_query_query_with_custom_parameters() {
    var expected = "{\n"
            + "  \"query_string\" : {\n"
            + "    \"query\" : \"field1:query_value\",\n"
            + "    \"fields\" : [],\n"
            + "    \"type\" : \"cross_fields\",\n"
            + "    \"tie_breaker\" : 1.3,\n"
            + "    \"default_operator\" : \"and\",\n"
            + "    \"analyzer\" : \"keyword\",\n"
            + "    \"max_determinized_states\" : 10000,\n"
            + "    \"enable_position_increments\" : true,\n"
            + "    \"fuzziness\" : \"AUTO\",\n"
            + "    \"fuzzy_prefix_length\" : 2,\n"
            + "    \"fuzzy_max_expansions\" : 10,\n"
            + "    \"phrase_slop\" : 0,\n"
            + "    \"analyze_wildcard\" : true,\n"
            + "    \"minimum_should_match\" : \"3\",\n"
            + "    \"lenient\" : false,\n"
            + "    \"escape\" : false,\n"
            + "    \"auto_generate_synonyms_phrase_query\" : false,\n"
            + "    \"fuzzy_transpositions\" : false,\n"
            + "    \"boost\" : 2.0,\n"
            + "  }\n"
            + "}";
    var actual = buildQuery(
            dsl.query(
                    dsl.namedArgument("query", literal("field1:query_value")),
                    dsl.namedArgument("analyze_wildcard", literal("true")),
                    dsl.namedArgument("analyzer", literal("keyword")),
                    dsl.namedArgument("auto_generate_synonyms_phrase_query", literal("false")),
                    dsl.namedArgument("default_operator", literal("AND")),
                    dsl.namedArgument("fuzzy_max_expansions", literal("10")),
                    dsl.namedArgument("fuzzy_prefix_length", literal("2")),
                    dsl.namedArgument("fuzzy_transpositions", literal("false")),
                    dsl.namedArgument("lenient", literal("false")),
                    dsl.namedArgument("minimum_should_match", literal("3")),
                    dsl.namedArgument("tie_breaker", literal("1.3")),
                    dsl.namedArgument("type", literal("cross_fields")),
                    dsl.namedArgument("boost", literal("2.0"))));

    assertJsonEquals(expected, actual);
  }

  @Test
  void query_string_invalid_parameter() {
    FunctionExpression expr = dsl.query_string(
        dsl.namedArgument("fields", DSL.literal(
            new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
                "field1", ExprValueUtils.floatValue(1.F),
                "field2", ExprValueUtils.floatValue(.3F)))))),
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("invalid_parameter", literal("invalid_value")));
    assertThrows(SemanticCheckException.class, () -> buildQuery(expr),
        "Parameter invalid_parameter is invalid for match function.");
  }

  @Test
  void should_build_query_string_query_with_default_parameters_multiple_fields() {
    var expected = "{\n"
        + "  \"query_string\" : {\n"
        + "    \"query\" : \"query_value\",\n"
        + "    \"fields\" : [%s],\n"
        + "    \"type\" : \"best_fields\",\n"
        + "    \"default_operator\" : \"or\",\n"
        + "    \"max_determinized_states\" : 10000,\n"
        + "    \"enable_position_increments\" : true,\n"
        + "    \"fuzziness\" : \"AUTO\",\n"
        + "    \"fuzzy_prefix_length\" : 0,\n"
        + "    \"fuzzy_max_expansions\" : 50,\n"
        + "    \"phrase_slop\" : 0,\n"
        + "    \"escape\" : false,\n"
        + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
        + "    \"fuzzy_transpositions\" : true,\n"
        + "    \"boost\" : 1.0\n"
        + "  }\n"
        + "}";
    var actual = buildQuery(dsl.query_string(
        dsl.namedArgument("fields", DSL.literal(new ExprTupleValue(
            new LinkedHashMap<>(ImmutableMap.of(
                "field1", ExprValueUtils.floatValue(1.F),
                "field2", ExprValueUtils.floatValue(.3F)))))),
        dsl.namedArgument("query", literal("query_value"))));

    var ex1 = String.format(expected, "\"field1^1.0\", \"field2^0.3\"");
    var ex2 = String.format(expected, "\"field2^0.3\", \"field1^1.0\"");
    assertTrue(new JSONObject(ex1).similar(new JSONObject(actual))
            || new JSONObject(ex2).similar(new JSONObject(actual)),
        StringUtils.format("Actual %s doesn't match neither expected %s nor %s", actual, ex1, ex2));
  }

  @Test
  void should_build_query_string_query_with_custom_parameters() {
    var expected = "{\n"
        + "  \"query_string\" : {\n"
        + "    \"query\" : \"query_value\",\n"
        + "    \"fields\" : [%s],\n"
        + "    \"type\" : \"cross_fields\",\n"
        + "    \"tie_breaker\" : 1.3,\n"
        + "    \"default_operator\" : \"and\",\n"
        + "    \"analyzer\" : \"keyword\",\n"
        + "    \"max_determinized_states\" : 10000,\n"
        + "    \"enable_position_increments\" : true,\n"
        + "    \"fuzziness\" : \"AUTO\",\n"
        + "    \"fuzzy_prefix_length\" : 2,\n"
        + "    \"fuzzy_max_expansions\" : 10,\n"
        + "    \"phrase_slop\" : 0,\n"
        + "    \"analyze_wildcard\" : true,\n"
        + "    \"minimum_should_match\" : \"3\",\n"
        + "    \"lenient\" : false,\n"
        + "    \"escape\" : false,\n"
        + "    \"auto_generate_synonyms_phrase_query\" : false,\n"
        + "    \"fuzzy_transpositions\" : false,\n"
        + "    \"boost\" : 2.0,\n"
        + "  }\n"
        + "}";
    var actual = buildQuery(
        dsl.query_string(
            dsl.namedArgument("fields", DSL.literal(
                ExprValueUtils.tupleValue(ImmutableMap.of("field1", 1.F, "field2", .3F)))),
            dsl.namedArgument("query", literal("query_value")),
            dsl.namedArgument("analyze_wildcard", literal("true")),
            dsl.namedArgument("analyzer", literal("keyword")),
            dsl.namedArgument("auto_generate_synonyms_phrase_query", literal("false")),
            dsl.namedArgument("default_operator", literal("AND")),
            dsl.namedArgument("fuzzy_max_expansions", literal("10")),
            dsl.namedArgument("fuzzy_prefix_length", literal("2")),
            dsl.namedArgument("fuzzy_transpositions", literal("false")),
            dsl.namedArgument("lenient", literal("false")),
            dsl.namedArgument("minimum_should_match", literal("3")),
            dsl.namedArgument("tie_breaker", literal("1.3")),
            dsl.namedArgument("type", literal("cross_fields")),
            dsl.namedArgument("boost", literal("2.0"))));

    var ex1 = String.format(expected, "\"field1^1.0\", \"field2^0.3\"");
    var ex2 = String.format(expected, "\"field2^0.3\", \"field1^1.0\"");
    assertTrue(new JSONObject(ex1).similar(new JSONObject(actual))
            || new JSONObject(ex2).similar(new JSONObject(actual)),
        StringUtils.format("Actual %s doesn't match neither expected %s nor %s", actual, ex1, ex2));
  }

  @Test
  void should_build_query_string_query_with_default_parameters_single_field() {
    assertJsonEquals("{\n"
            + "  \"query_string\" : {\n"
            + "    \"query\" : \"query_value\",\n"
            + "    \"fields\" : [\n"
            + "      \"field1^1.0\"\n"
            + "    ],\n"
            + "    \"type\" : best_fields,\n"
            + "    \"default_operator\" : or,\n"
            + "    \"max_determinized_states\" : 10000,\n"
            + "    \"enable_position_increments\" : true,\n"
            + "    \"fuzziness\" : \"AUTO\",\n"
            + "    \"fuzzy_prefix_length\" : 0,\n"
            + "    \"fuzzy_max_expansions\" : 50,\n"
            + "    \"phrase_slop\" : 0,\n"
            + "    \"escape\" : false,\n"
            + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "    \"fuzzy_transpositions\" : true,\n"
            + "    \"boost\" : 1.0,\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.query_string(
            dsl.namedArgument("fields", DSL.literal(new ExprTupleValue(
                new LinkedHashMap<>(ImmutableMap.of(
                    "field1", ExprValueUtils.floatValue(1.F)))))),
            dsl.namedArgument("query", literal("query_value")))));
  }

  @Test
  // Notes for following three tests:
  // 1) OpenSearch (not the plugin) might change order of fields
  // 2) `flags` are printed by OpenSearch as an integer
  // 3) `minimum_should_match` printed as a string
  void should_build_simple_query_string_query_with_default_parameters_single_field() {
    assertJsonEquals("{\n"
            + "  \"simple_query_string\" : {\n"
            + "    \"query\" : \"search query\",\n"
            + "    \"fields\" : [\n"
            + "      \"field1^1.0\"\n"
            + "    ],\n"
            + "    \"default_operator\" : \"or\",\n"
            + "    \"analyze_wildcard\" : false,\n"
            + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "    \"flags\" : -1,\n"
            + "    \"fuzzy_max_expansions\" : 50,\n"
            + "    \"fuzzy_prefix_length\" : 0,\n"
            + "    \"fuzzy_transpositions\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.simple_query_string(
            dsl.namedArgument("fields", DSL.literal(new ExprTupleValue(
                new LinkedHashMap<>(ImmutableMap.of(
                    "field1", ExprValueUtils.floatValue(1.F)))))),
            dsl.namedArgument("query", literal("search query")))));
  }

  @Test
  void should_build_simple_query_string_query_with_default_parameters_multiple_fields() {
    var expected = "{\n"
            + "  \"simple_query_string\" : {\n"
            + "    \"query\" : \"search query\",\n"
            + "    \"fields\" : [%s],\n"
            + "    \"default_operator\" : \"or\",\n"
            + "    \"analyze_wildcard\" : false,\n"
            + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "    \"flags\" : -1,\n"
            + "    \"fuzzy_max_expansions\" : 50,\n"
            + "    \"fuzzy_prefix_length\" : 0,\n"
            + "    \"fuzzy_transpositions\" : true,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";
    var actual = buildQuery(dsl.simple_query_string(
        dsl.namedArgument("fields", DSL.literal(new ExprTupleValue(
            new LinkedHashMap<>(ImmutableMap.of(
                "field1", ExprValueUtils.floatValue(1.F),
                "field2", ExprValueUtils.floatValue(.3F)))))),
        dsl.namedArgument("query", literal("search query"))));

    var ex1 = String.format(expected, "\"field1^1.0\", \"field2^0.3\"");
    var ex2 = String.format(expected, "\"field2^0.3\", \"field1^1.0\"");
    assertTrue(new JSONObject(ex1).similar(new JSONObject(actual))
        || new JSONObject(ex2).similar(new JSONObject(actual)),
        StringUtils.format("Actual %s doesn't match neither expected %s nor %s", actual, ex1, ex2));
  }

  @Test
  void should_build_simple_query_string_query_with_custom_parameters() {
    var expected = "{\n"
            + "  \"simple_query_string\" : {\n"
            + "    \"query\" : \"search query\",\n"
            + "    \"fields\" : [%s],\n"
            + "    \"analyze_wildcard\" : true,\n"
            + "    \"analyzer\" : \"keyword\",\n"
            + "    \"auto_generate_synonyms_phrase_query\" : false,\n"
            + "    \"default_operator\" : \"and\",\n"
            + "    \"flags\" : 1,\n"
            + "    \"fuzzy_max_expansions\" : 10,\n"
            + "    \"fuzzy_prefix_length\" : 2,\n"
            + "    \"fuzzy_transpositions\" : false,\n"
            + "    \"lenient\" : false,\n"
            + "    \"minimum_should_match\" : \"3\",\n"
            + "    \"boost\" : 2.0\n"
            + "  }\n"
            + "}";
    var actual = buildQuery(
            dsl.simple_query_string(
                dsl.namedArgument("fields", DSL.literal(
                    ExprValueUtils.tupleValue(ImmutableMap.of("field1", 1.F, "field2", .3F)))),
                dsl.namedArgument("query", literal("search query")),
                dsl.namedArgument("analyze_wildcard", literal("true")),
                dsl.namedArgument("analyzer", literal("keyword")),
                dsl.namedArgument("auto_generate_synonyms_phrase_query", literal("false")),
                dsl.namedArgument("default_operator", literal("AND")),
                dsl.namedArgument("flags", literal("AND")),
                dsl.namedArgument("fuzzy_max_expansions", literal("10")),
                dsl.namedArgument("fuzzy_prefix_length", literal("2")),
                dsl.namedArgument("fuzzy_transpositions", literal("false")),
                dsl.namedArgument("lenient", literal("false")),
                dsl.namedArgument("minimum_should_match", literal("3")),
                dsl.namedArgument("boost", literal("2.0"))));

    var ex1 = String.format(expected, "\"field1^1.0\", \"field2^0.3\"");
    var ex2 = String.format(expected, "\"field2^0.3\", \"field1^1.0\"");
    assertTrue(new JSONObject(ex1).similar(new JSONObject(actual))
        || new JSONObject(ex2).similar(new JSONObject(actual)),
        StringUtils.format("Actual %s doesn't match neither expected %s nor %s", actual, ex1, ex2));
  }

  @Test
  void simple_query_string_invalid_parameter() {
    FunctionExpression expr = dsl.simple_query_string(
        dsl.namedArgument("fields", DSL.literal(
            new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
                "field1", ExprValueUtils.floatValue(1.F),
                "field2", ExprValueUtils.floatValue(.3F)))))),
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("invalid_parameter", literal("invalid_value")));
    assertThrows(SemanticCheckException.class, () -> buildQuery(expr),
        "Parameter invalid_parameter is invalid for match function.");
  }

  @Test
  void match_phrase_invalid_parameter() {
    FunctionExpression expr = dsl.match_phrase(
        dsl.namedArgument("field", literal("message")),
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("invalid_parameter", literal("invalid_value")));
    var msg = assertThrows(SemanticCheckException.class, () -> buildQuery(expr)).getMessage();
    assertTrue(msg.startsWith("Parameter invalid_parameter is invalid for match_phrase function."));
  }

  @Test
  void relevancy_func_invalid_arg_values() {
    final var field = dsl.namedArgument("field", literal("message"));
    final var fields = dsl.namedArgument("fields", DSL.literal(
        new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
            "field1", ExprValueUtils.floatValue(1.F),
            "field2", ExprValueUtils.floatValue(.3F))))));
    final var query = dsl.namedArgument("query", literal("search query"));

    var slopTest = dsl.match_phrase(field, query,
        dsl.namedArgument("slop", literal("1.5")));
    var msg = assertThrows(RuntimeException.class, () -> buildQuery(slopTest)).getMessage();
    assertEquals("Invalid slop value: '1.5'. Accepts only integer values.", msg);

    var ztqTest = dsl.match_phrase(field, query,
        dsl.namedArgument("zero_terms_query", literal("meow")));
    msg = assertThrows(RuntimeException.class, () -> buildQuery(ztqTest)).getMessage();
    assertEquals(
        "Invalid zero_terms_query value: 'meow'. Available values are: NONE, ALL, NULL.", msg);

    var boostTest = dsl.match(field, query,
        dsl.namedArgument("boost", literal("pewpew")));
    msg = assertThrows(RuntimeException.class, () -> buildQuery(boostTest)).getMessage();
    assertEquals(
        "Invalid boost value: 'pewpew'. Accepts only floating point values greater than 0.", msg);

    var boolTest = dsl.query_string(fields, query,
        dsl.namedArgument("escape", literal("42")));
    msg = assertThrows(RuntimeException.class, () -> buildQuery(boolTest)).getMessage();
    assertEquals(
        "Invalid escape value: '42'. Accepts only boolean values: 'true' or 'false'.", msg);

    var typeTest = dsl.multi_match(fields, query,
        dsl.namedArgument("type", literal("42")));
    msg = assertThrows(RuntimeException.class, () -> buildQuery(typeTest)).getMessage();
    assertTrue(msg.startsWith("Invalid type value: '42'. Available values are:"));

    var operatorTest = dsl.simple_query_string(fields, query,
        dsl.namedArgument("default_operator", literal("42")));
    msg = assertThrows(RuntimeException.class, () -> buildQuery(operatorTest)).getMessage();
    assertTrue(msg.startsWith("Invalid default_operator value: '42'. Available values are:"));

    var flagsTest = dsl.simple_query_string(fields, query,
        dsl.namedArgument("flags", literal("42")));
    msg = assertThrows(RuntimeException.class, () -> buildQuery(flagsTest)).getMessage();
    assertTrue(msg.startsWith("Invalid flags value: '42'. Available values are:"));

    var fuzzinessTest = dsl.match_bool_prefix(field, query,
        dsl.namedArgument("fuzziness", literal("AUTO:")));
    msg = assertThrows(RuntimeException.class, () -> buildQuery(fuzzinessTest)).getMessage();
    assertTrue(msg.startsWith("Invalid fuzziness value: 'AUTO:'. Available values are:"));

    var rewriteTest = dsl.match_bool_prefix(field, query,
        dsl.namedArgument("fuzzy_rewrite", literal("42")));
    msg = assertThrows(RuntimeException.class, () -> buildQuery(rewriteTest)).getMessage();
    assertTrue(msg.startsWith("Invalid fuzzy_rewrite value: '42'. Available values are:"));

    var timezoneTest = dsl.query_string(fields, query,
        dsl.namedArgument("time_zone", literal("42")));
    msg = assertThrows(RuntimeException.class, () -> buildQuery(timezoneTest)).getMessage();
    assertTrue(msg.startsWith("Invalid time_zone value: '42'."));
  }

  @Test
  void should_build_match_bool_prefix_query_with_default_parameters() {
    assertJsonEquals(
        "{\n"
            + "  \"match_bool_prefix\" : {\n"
            + "    \"message\" : {\n"
            + "      \"query\" : \"search query\",\n"
            + "      \"operator\" : \"OR\",\n"
            + "      \"prefix_length\" : 0,\n"
            + "      \"max_expansions\" : 50,\n"
            + "      \"fuzzy_transpositions\" : true,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.match_bool_prefix(
                dsl.namedArgument("field", literal("message")),
                dsl.namedArgument("query", literal("search query")))));
  }

  @Test
  void multi_match_missing_fields() {
    var msg = assertThrows(SemanticCheckException.class, () ->
        dsl.multi_match(
            dsl.namedArgument("query", literal("search query")))).getMessage();
    assertEquals("Expected type STRUCT instead of STRING for parameter #1", msg);
  }

  @Test
  void multi_match_missing_fields_even_with_struct() {
    FunctionExpression expr = dsl.multi_match(
        dsl.namedArgument("something-but-not-fields", DSL.literal(
            new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
                "pewpew", ExprValueUtils.integerValue(42)))))),
        dsl.namedArgument("query", literal("search query")),
        dsl.namedArgument("analyzer", literal("keyword")));
    var msg = assertThrows(SemanticCheckException.class, () -> buildQuery(expr)).getMessage();
    assertEquals("'fields' parameter is missing.", msg);
  }

  @Test
  void multi_match_missing_query_even_with_struct() {
    FunctionExpression expr = dsl.multi_match(
            dsl.namedArgument("fields", DSL.literal(
                    new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
                            "field1", ExprValueUtils.floatValue(1.F),
                            "field2", ExprValueUtils.floatValue(.3F)))))),
            dsl.namedArgument("analyzer", literal("keyword")));
    var msg = assertThrows(SemanticCheckException.class, () -> buildQuery(expr)).getMessage();
    assertEquals("'query' parameter is missing", msg);
  }

  @Test
  void should_build_match_phrase_prefix_query_with_default_parameters() {
    assertJsonEquals(
        "{\n"
            + "  \"match_phrase_prefix\" : {\n"
            + "    \"message\" : {\n"
            + "      \"query\" : \"search query\",\n"
            + "      \"slop\" : 0,\n"
            + "      \"zero_terms_query\" : \"NONE\",\n"
            + "      \"max_expansions\" : 50,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.match_phrase_prefix(
                dsl.namedArgument("field", literal("message")),
                dsl.namedArgument("query", literal("search query")))));
  }

  @Test
  void should_build_match_phrase_prefix_query_with_non_default_parameters() {
    assertJsonEquals(
        "{\n"
            + "  \"match_phrase_prefix\" : {\n"
            + "    \"message\" : {\n"
            + "      \"query\" : \"search query\",\n"
            + "      \"slop\" : 0,\n"
            + "      \"zero_terms_query\" : \"NONE\",\n"
            + "      \"max_expansions\" : 42,\n"
            + "      \"boost\" : 1.2,\n"
            + "      \"analyzer\": english\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(
            dsl.match_phrase_prefix(
                dsl.namedArgument("field", literal("message")),
                dsl.namedArgument("query", literal("search query")),
                dsl.namedArgument("boost", literal("1.2")),
                dsl.namedArgument("max_expansions", literal("42")),
                dsl.namedArgument("analyzer", literal("english")))));
  }

  @Test
  void cast_to_string_in_filter() {
    String json = "{\n"
        + "  \"term\" : {\n"
        + "    \"string_value\" : {\n"
        + "      \"value\" : \"1\",\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";

    assertJsonEquals(json, buildQuery(
        dsl.equal(ref("string_value", STRING), dsl.castString(literal(1)))));
    assertJsonEquals(json, buildQuery(
        dsl.equal(ref("string_value", STRING), dsl.castString(literal("1")))));
  }

  private Float castToFloat(Object o) {
    if (o instanceof Number) {
      return ((Number)o).floatValue();
    }
    if (o instanceof String) {
      return Float.parseFloat((String) o);
    }
    if (o instanceof Boolean) {
      return ((Boolean)o) ? 1F : 0F;
    }
    // unreachable code
    throw new IllegalArgumentException();
  }

  private Integer castToInteger(Object o) {
    if (o instanceof Number) {
      return ((Number)o).intValue();
    }
    if (o instanceof String) {
      return Integer.parseInt((String) o);
    }
    if (o instanceof Boolean) {
      return ((Boolean)o) ? 1 : 0;
    }
    // unreachable code
    throw new IllegalArgumentException();
  }

  @ParameterizedTest(name = "castByte({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_byte_in_filter(LiteralExpression expr) {
    assertJsonEquals(String.format(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"byte_value\" : {\n"
            + "      \"value\" : %d,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}", castToInteger(expr.valueOf().value())),
        buildQuery(dsl.equal(ref("byte_value", BYTE), dsl.castByte(expr))));
  }

  @ParameterizedTest(name = "castShort({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_short_in_filter(LiteralExpression expr) {
    assertJsonEquals(String.format(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"short_value\" : {\n"
            + "      \"value\" : %d,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}", castToInteger(expr.valueOf().value())),
        buildQuery(dsl.equal(ref("short_value", SHORT), dsl.castShort(expr))));
  }

  @ParameterizedTest(name = "castInt({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_int_in_filter(LiteralExpression expr) {
    assertJsonEquals(String.format(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"integer_value\" : {\n"
            + "      \"value\" : %d,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}", castToInteger(expr.valueOf().value())),
        buildQuery(dsl.equal(ref("integer_value", INTEGER), dsl.castInt(expr))));
  }

  @ParameterizedTest(name = "castLong({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_long_in_filter(LiteralExpression expr) {
    assertJsonEquals(String.format(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"long_value\" : {\n"
            + "      \"value\" : %d,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}", castToInteger(expr.valueOf().value())),
        buildQuery(dsl.equal(ref("long_value", LONG), dsl.castLong(expr))));
  }

  @ParameterizedTest(name = "castFloat({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_float_in_filter(LiteralExpression expr) {
    assertJsonEquals(String.format(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"float_value\" : {\n"
            + "      \"value\" : %f,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}", castToFloat(expr.valueOf().value())),
        buildQuery(dsl.equal(ref("float_value", FLOAT), dsl.castFloat(expr))));
  }

  @ParameterizedTest(name = "castDouble({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_double_in_filter(LiteralExpression expr) {
    // double values affected by floating point imprecision, so we can't compare them in json
    // (Double)(Float)3.14 -> 3.14000010490417
    assertEquals(castToFloat(expr.valueOf().value()),
        dsl.castDouble(expr).valueOf().doubleValue(), 0.00001);

    assertJsonEquals(String.format(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"double_value\" : {\n"
            + "      \"value\" : %2.20f,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}", dsl.castDouble(expr).valueOf().doubleValue()),
        buildQuery(dsl.equal(ref("double_value", DOUBLE), dsl.castDouble(expr))));
  }

  @ParameterizedTest(name = "castBooleanTrue({0})")
  @MethodSource({"booleanTrueCastSource"})
  void cast_to_boolean_true_in_filter(LiteralExpression expr) {
    String json = "{\n"
        + "  \"term\" : {\n"
        + "    \"boolean_value\" : {\n"
        + "      \"value\" : true,\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";

    assertJsonEquals(
        json, buildQuery(dsl.equal(ref("boolean_value", BOOLEAN), dsl.castBoolean(expr))));
  }

  @ParameterizedTest(name = "castBooleanFalse({0})")
  @MethodSource({"booleanFalseCastSource"})
  void cast_to_boolean_false_in_filter(LiteralExpression expr) {
    String json = "{\n"
        + "  \"term\" : {\n"
        + "    \"boolean_value\" : {\n"
        + "      \"value\" : false,\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";

    assertJsonEquals(
        json, buildQuery(dsl.equal(ref("boolean_value", BOOLEAN), dsl.castBoolean(expr))));
  }

  @Test
  void cast_from_boolean() {
    Expression booleanExpr = literal(false);
    String json = "{\n"
        + "  \"term\" : {\n"
        + "    \"my_value\" : {\n"
        + "      \"value\" : 0,\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";
    assertJsonEquals(json, buildQuery(
        dsl.equal(ref("my_value", BYTE), dsl.castByte(booleanExpr))));
    assertJsonEquals(json, buildQuery(
        dsl.equal(ref("my_value", SHORT), dsl.castShort(booleanExpr))));
    assertJsonEquals(json, buildQuery(
        dsl.equal(ref("my_value", INTEGER), dsl.castInt(booleanExpr))));
    assertJsonEquals(json, buildQuery(
        dsl.equal(ref("my_value", LONG), dsl.castLong(booleanExpr))));

    json = "{\n"
        + "  \"term\" : {\n"
        + "    \"my_value\" : {\n"
        + "      \"value\" : 0.0,\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";
    assertJsonEquals(json, buildQuery(
        dsl.equal(ref("my_value", FLOAT), dsl.castFloat(booleanExpr))));
    assertJsonEquals(json, buildQuery(
        dsl.equal(ref("my_value", DOUBLE), dsl.castDouble(booleanExpr))));

    json = "{\n"
        + "  \"term\" : {\n"
        + "    \"my_value\" : {\n"
        + "      \"value\" : \"false\",\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";
    assertJsonEquals(json, buildQuery(
        dsl.equal(ref("my_value", STRING), dsl.castString(booleanExpr))));
  }

  @Test
  void cast_to_date_in_filter() {
    String json = "{\n"
        + "  \"term\" : {\n"
        + "    \"date_value\" : {\n"
        + "      \"value\" : \"2021-11-08\",\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";

    assertJsonEquals(json, buildQuery(dsl.equal(
        ref("date_value", DATE), dsl.castDate(literal("2021-11-08")))));
    assertJsonEquals(json, buildQuery(dsl.equal(
        ref("date_value", DATE), dsl.castDate(literal(new ExprDateValue("2021-11-08"))))));
    assertJsonEquals(json, buildQuery(dsl.equal(ref(
        "date_value", DATE), dsl.castDate(literal(new ExprDatetimeValue("2021-11-08 17:00:00"))))));
  }

  @Test
  void cast_to_time_in_filter() {
    String json = "{\n"
        + "  \"term\" : {\n"
        + "    \"time_value\" : {\n"
        + "      \"value\" : \"17:00:00\",\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";

    assertJsonEquals(json, buildQuery(dsl.equal(
        ref("time_value", TIME), dsl.castTime(literal("17:00:00")))));
    assertJsonEquals(json, buildQuery(dsl.equal(
        ref("time_value", TIME), dsl.castTime(literal(new ExprTimeValue("17:00:00"))))));
    assertJsonEquals(json, buildQuery(dsl.equal(ref("time_value", TIME), dsl
        .castTime(literal(new ExprTimestampValue("2021-11-08 17:00:00"))))));
  }

  @Test
  void cast_to_datetime_in_filter() {
    String json = "{\n"
        + "  \"term\" : {\n"
        + "    \"datetime_value\" : {\n"
        + "      \"value\" : \"2021-11-08 17:00:00\",\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";

    assertJsonEquals(json, buildQuery(dsl.equal(ref("datetime_value", DATETIME), dsl
        .castDatetime(literal("2021-11-08 17:00:00")))));
    assertJsonEquals(json, buildQuery(dsl.equal(ref("datetime_value", DATETIME), dsl
        .castDatetime(literal(new ExprTimestampValue("2021-11-08 17:00:00"))))));
  }

  @Test
  void cast_to_timestamp_in_filter() {
    String json = "{\n"
        + "  \"term\" : {\n"
        + "    \"timestamp_value\" : {\n"
        + "      \"value\" : 1636390800000,\n"
        + "      \"boost\" : 1.0\n"
        + "    }\n"
        + "  }\n"
        + "}";

    assertJsonEquals(json, buildQuery(dsl.equal(ref("timestamp_value", TIMESTAMP), dsl
        .castTimestamp(literal("2021-11-08 17:00:00")))));
    assertJsonEquals(json, buildQuery(dsl.equal(ref("timestamp_value", TIMESTAMP), dsl
        .castTimestamp(literal(new ExprTimestampValue("2021-11-08 17:00:00"))))));
  }

  @Test
  void cast_in_range_query() {
    assertJsonEquals(
        "{\n"
            + "  \"range\" : {\n"
            + "    \"timestamp_value\" : {\n"
            + "      \"from\" : 1636390800000,\n"
            + "      \"to\" : null,"
            + "      \"include_lower\" : false,"
            + "      \"include_upper\" : true,"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.greater(ref("timestamp_value", TIMESTAMP), dsl
            .castTimestamp(literal("2021-11-08 17:00:00")))));
  }

  @Test
  void non_literal_in_cast_should_build_script() {
    mockToStringSerializer();
    assertJsonEquals(
        "{\n"
            + "  \"script\" : {\n"
            + "    \"script\" : {\n"
            + "      \"source\" : \"=(string_value, cast_to_string(+(1, 0)))\",\n"
            + "      \"lang\" : \"opensearch_query_expression\"\n"
            + "    },\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.equal(ref("string_value", STRING), dsl.castString(dsl
            .add(literal(1), literal(0)))))
    );
  }

  @Test
  void non_cast_nested_function_should_build_script() {
    mockToStringSerializer();
    assertJsonEquals(
        "{\n"
            + "  \"script\" : {\n"
            + "    \"script\" : {\n"
            + "      \"source\" : \"=(integer_value, abs(+(1, 0)))\",\n"
            + "      \"lang\" : \"opensearch_query_expression\"\n"
            + "    },\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.equal(ref("integer_value", INTEGER), dsl.abs(dsl
            .add(literal(1), literal(0)))))
    );
  }

  private static void assertJsonEquals(String expected, String actual) {
    assertTrue(new JSONObject(expected).similar(new JSONObject(actual)),
        StringUtils.format("Expected: %s, actual: %s", expected, actual));
  }

  private String buildQuery(Expression expr) {
    return filterQueryBuilder.build(expr).toString();
  }

  private void mockToStringSerializer() {
    doAnswer(invocation -> {
      Expression expr = invocation.getArgument(0);
      return expr.toString();
    }).when(serializer).serialize(any());
  }

}
