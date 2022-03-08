/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.filter;

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
import org.opensearch.sql.data.model.ExprValue;
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
    return Stream.of(literal((byte) 1), literal((short) 1), literal(
        1), literal(1L), literal(1F), literal(1D), literal(true), literal("1"));
  }

  private static Stream<LiteralExpression> booleanTrueCastSource() {
    return Stream.of(literal((byte) 1), literal((short) 1), literal(
        1), literal(1L), literal(1F), literal(1D), literal(true), literal("true"));
  }

  private static Stream<LiteralExpression> booleanFalseCastSource() {
    return Stream.of(literal((byte) 0), literal((short) 0), literal(
        0), literal(0L), literal(0F), literal(0D), literal(false), literal("false"));
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
            + "      \"fuzzy_rewrite\" : \"top_terms_N\","
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
                dsl.namedArgument("fuzzy_rewrite", literal("top_terms_N")),
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
    assertThrows(SemanticCheckException.class, () -> buildQuery(expr),
        "Parameter invalid_parameter is invalid for match function.");
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

  @ParameterizedTest(name = "castByte({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_byte_in_filter(LiteralExpression expr) {
    assertJsonEquals(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"byte_value\" : {\n"
            + "      \"value\" : 1,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.equal(ref("byte_value", BYTE), dsl.castByte(expr))));
  }

  @ParameterizedTest(name = "castShort({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_short_in_filter(LiteralExpression expr) {
    assertJsonEquals(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"short_value\" : {\n"
            + "      \"value\" : 1,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.equal(ref("short_value", SHORT), dsl.castShort(expr))));
  }

  @ParameterizedTest(name = "castInt({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_int_in_filter(LiteralExpression expr) {
    assertJsonEquals(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"integer_value\" : {\n"
            + "      \"value\" : 1,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.equal(ref("integer_value", INTEGER), dsl.castInt(expr))));
  }

  @ParameterizedTest(name = "castLong({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_long_in_filter(LiteralExpression expr) {
    assertJsonEquals(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"long_value\" : {\n"
            + "      \"value\" : 1,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.equal(ref("long_value", LONG), dsl.castLong(expr))));
  }

  @ParameterizedTest(name = "castFloat({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_float_in_filter(LiteralExpression expr) {
    assertJsonEquals(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"float_value\" : {\n"
            + "      \"value\" : 1.0,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
        buildQuery(dsl.equal(ref("float_value", FLOAT), dsl.castFloat(expr))));
  }

  @ParameterizedTest(name = "castDouble({0})")
  @MethodSource({"numericCastSource"})
  void cast_to_double_in_filter(LiteralExpression expr) {
    assertJsonEquals(
        "{\n"
            + "  \"term\" : {\n"
            + "    \"double_value\" : {\n"
            + "      \"value\" : 1.0,\n"
            + "      \"boost\" : 1.0\n"
            + "    }\n"
            + "  }\n"
            + "}",
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
