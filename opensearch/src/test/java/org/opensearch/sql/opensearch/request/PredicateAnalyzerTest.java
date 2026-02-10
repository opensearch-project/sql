/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Holder;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.ExpressionNotAnalyzableException;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.QueryExpression;

public class PredicateAnalyzerTest {
  final OpenSearchTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;
  final RexBuilder builder = new RexBuilder(typeFactory);
  final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), builder);
  final List<String> schema = List.of("a", "b", "c", "d", "e");
  final Map<String, ExprType> fieldTypes =
      Map.of(
          "a", OpenSearchDataType.of(MappingType.Integer),
          "b",
              OpenSearchDataType.of(
                  MappingType.Text, Map.of("fields", Map.of("keyword", Map.of("type", "keyword")))),
          "c", OpenSearchDataType.of(MappingType.Text), // Text without keyword cannot be push down
          "d", OpenSearchDataType.of(MappingType.Date),
          "e", OpenSearchDataType.of(MappingType.Boolean));
  final RexInputRef field1 =
      builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
  final RexInputRef field2 =
      builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
  final RexInputRef field4 = builder.makeInputRef(typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP), 3);
  final RexInputRef field5 =
      builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 4);
  final RexLiteral numericLiteral = builder.makeExactLiteral(new BigDecimal(12));
  final RexLiteral stringLiteral = builder.makeLiteral("Hi");
  final RexNode dateTimeLiteral =
      builder.makeLiteral(
          "1987-02-03 04:34:56", typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP), true);
  final RexNode aliasedField2 =
      builder.makeCall(
          SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, builder.makeLiteral("field"), field2);
  final RexNode aliasedStringLiteral =
      builder.makeCall(
          SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, builder.makeLiteral("query"), stringLiteral);

  @Test
  void equals_generatesTermQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermQueryBuilder.class, result);
    assertEquals(
        """
        {
          "term" : {
            "a" : {
              "value" : 12,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notEquals_generatesBoolQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "exists" : {
                  "field" : "a",
                  "boost" : 1.0
                }
              }
            ],
            "must_not" : [
              {
                "term" : {
                  "a" : {
                    "value" : 12,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void gt_generatesRangeQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.GREATER_THAN, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "a" : {
              "from" : 12,
              "to" : null,
              "include_lower" : false,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void gte_generatesRangeQuery() throws ExpressionNotAnalyzableException {
    RexNode call =
        builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "a" : {
              "from" : 12,
              "to" : null,
              "include_lower" : true,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void lt_generatesRangeQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.LESS_THAN, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "a" : {
              "from" : null,
              "to" : 12,
              "include_lower" : true,
              "include_upper" : false,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void lte_generatesRangeQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "a" : {
              "from" : null,
              "to" : 12,
              "include_lower" : true,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void exists_generatesExistsQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, field1);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(ExistsQueryBuilder.class, result);
    assertEquals(
        """
        {
          "exists" : {
            "field" : "a",
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notExists_generatesMustNotExistsQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.IS_NULL, field1);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must_not" : [
              {
                "exists" : {
                  "field" : "a",
                  "boost" : 1.0
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void search_generatesTermsQuery() throws ExpressionNotAnalyzableException {
    final RexLiteral numericLiteral1 = builder.makeExactLiteral(new BigDecimal(13));
    final RexLiteral numericLiteral2 = builder.makeExactLiteral(new BigDecimal(14));
    RexNode call =
        builder.makeIn(field1, ImmutableList.of(numericLiteral, numericLiteral1, numericLiteral2));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermsQueryBuilder.class, result);
    assertEquals(
        """
        {
          "terms" : {
            "a" : [
              12.0,
              13.0,
              14.0
            ],
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void contains_generatesMatchQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.CONTAINS, field2, stringLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(MatchQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match" : {
            "b" : {
              "query" : "Hi",
              "operator" : "OR",
              "prefix_length" : 0,
              "max_expansions" : 50,
              "fuzzy_transpositions" : true,
              "lenient" : false,
              "zero_terms_query" : "NONE",
              "auto_generate_synonyms_phrase_query" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void matchRelevanceQueryFunction_generatesMatchQuery() throws ExpressionNotAnalyzableException {
    List<RexNode> arguments = Arrays.asList(aliasedField2, aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "match", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MatchQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match" : {
            "b" : {
              "query" : "Hi",
              "operator" : "OR",
              "prefix_length" : 0,
              "max_expansions" : 50,
              "fuzzy_transpositions" : true,
              "lenient" : false,
              "zero_terms_query" : "NONE",
              "auto_generate_synonyms_phrase_query" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void matchPhraseRelevanceQueryFunction_generatesMatchPhraseQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            aliasedField2,
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("slop"),
                builder.makeLiteral("2")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "match_phrase", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MatchPhraseQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match_phrase" : {
            "b" : {
              "query" : "Hi",
              "slop" : 2,
              "zero_terms_query" : "NONE",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void matchBoolPrefixRelevanceQueryFunction_generatesMatchBoolPrefixQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            aliasedField2,
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("minimum_should_match"),
                builder.makeLiteral("1")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "match_bool_prefix", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MatchBoolPrefixQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match_bool_prefix" : {
            "b" : {
              "query" : "Hi",
              "operator" : "OR",
              "minimum_should_match" : "1",
              "prefix_length" : 0,
              "max_expansions" : 50,
              "fuzzy_transpositions" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void matchPhrasePrefixRelevanceQueryFunction_generatesMatchPhrasePrefixQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            aliasedField2,
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("analyzer"),
                builder.makeLiteral("standard")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "match_phrase_prefix", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MatchPhrasePrefixQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match_phrase_prefix" : {
            "b" : {
              "query" : "Hi",
              "analyzer" : "standard",
              "slop" : 0,
              "max_expansions" : 50,
              "zero_terms_query" : "NONE",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void queryStringRelevanceQueryFunction_generatesQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("fields"),
                builder.makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    builder.makeLiteral("b"),
                    builder.makeLiteral(
                        1.0, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true),
                    builder.makeLiteral("c"),
                    builder.makeLiteral(
                        2.5, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true))),
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("fuzziness"),
                builder.makeLiteral("1")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "query_string", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(QueryStringQueryBuilder.class, result);
    assertEquals(
        """
        {
          "query_string" : {
            "query" : "Hi",
            "fields" : [
              "b^1.0",
              "c^2.5"
            ],
            "type" : "best_fields",
            "default_operator" : "or",
            "max_determinized_states" : 10000,
            "enable_position_increments" : true,
            "fuzziness" : "1",
            "fuzzy_prefix_length" : 0,
            "fuzzy_max_expansions" : 50,
            "phrase_slop" : 0,
            "escape" : false,
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void simpleQueryStringRelevanceQueryFunction_generatesSimpleQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("fields"),
                builder.makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    builder.makeLiteral("b*"),
                    builder.makeLiteral(
                        1.0, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true))),
            aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "simple_query_string", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(SimpleQueryStringBuilder.class, result);
    assertEquals(
        """
        {
          "simple_query_string" : {
            "query" : "Hi",
            "fields" : [
              "b*^1.0"
            ],
            "flags" : -1,
            "default_operator" : "or",
            "analyze_wildcard" : false,
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_prefix_length" : 0,
            "fuzzy_max_expansions" : 50,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void multiMatchRelevanceQueryFunction_generatesMultiMatchQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("fields"),
                builder.makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    builder.makeLiteral("b*"),
                    builder.makeLiteral(
                        1.0, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true))),
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("max_expansions"),
                builder.makeLiteral("25")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "multi_match", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MultiMatchQueryBuilder.class, result);
    assertEquals(
        """
        {
          "multi_match" : {
            "query" : "Hi",
            "fields" : [
              "b*^1.0"
            ],
            "type" : "best_fields",
            "operator" : "OR",
            "slop" : 0,
            "prefix_length" : 0,
            "max_expansions" : 25,
            "zero_terms_query" : "NONE",
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void likeFunction_keywordField_generatesWildcardQuery() throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(field2, builder.makeLiteral("%Hi%"), builder.makeLiteral(true));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "like", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(WildcardQueryBuilder.class, result);
    assertEquals(
        """
        {
          "wildcard" : {
            "b.keyword" : {
              "wildcard" : "*Hi*",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void ilikeFunction_keywordField_generatesWildcardQuery() throws ExpressionNotAnalyzableException {
    List<RexNode> arguments = Arrays.asList(field2, builder.makeLiteral("%Hi%"));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "ilike", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(WildcardQueryBuilder.class, result);
    assertEquals(
        """
        {
          "wildcard" : {
            "b.keyword" : {
              "wildcard" : "*Hi*",
              "case_insensitive" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void likeFunction_textField_scriptPushDown() throws ExpressionNotAnalyzableException {
    RexInputRef field3 = builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
    List<RexNode> arguments =
        Arrays.asList(field3, builder.makeLiteral("%Hi%"), builder.makeLiteral(true));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "like", arguments.toArray(new RexNode[0]));

    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
            .add("b", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .add("c", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
    QueryExpression expression =
        PredicateAnalyzer.analyzeExpression(call, schema, fieldTypes, rowType, cluster);
    assert (expression
        .builder()
        .toString()
        .contains("\"lang\" : \"opensearch_compounded_script\""));
  }

  @Test
  void andOrNot_generatesCompoundQuery() throws ExpressionNotAnalyzableException {
    RexNode call1 = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, numericLiteral);
    RexNode call2 =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS, field1, builder.makeExactLiteral(new BigDecimal(13)));
    RexNode call3 = builder.makeCall(SqlStdOperatorTable.EQUALS, field2, stringLiteral);
    RexNode orCall = builder.makeCall(SqlStdOperatorTable.OR, call1, call2);
    RexNode andCall = builder.makeCall(SqlStdOperatorTable.AND, orCall, call3);
    RexNode notCall = builder.makeCall(SqlStdOperatorTable.NOT, andCall);
    QueryBuilder result = PredicateAnalyzer.analyze(notCall, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must_not" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "bool" : {
                        "should" : [
                          {
                            "term" : {
                              "a" : {
                                "value" : 12,
                                "boost" : 1.0
                              }
                            }
                          },
                          {
                            "term" : {
                              "a" : {
                                "value" : 13,
                                "boost" : 1.0
                              }
                            }
                          }
                        ],
                        "adjust_pure_negative" : true,
                        "boost" : 1.0
                      }
                    },
                    {
                      "term" : {
                        "b.keyword" : {
                          "value" : "Hi",
                          "boost" : 1.0
                        }
                      }
                    }
                  ],
                  "adjust_pure_negative" : true,
                  "boost" : 1.0
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void equals_generatesTermQuery_TextWithKeyword() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field2, stringLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermQueryBuilder.class, result);
    assertEquals(
        """
        {
          "term" : {
            "b.keyword" : {
              "value" : "Hi",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void equals_scriptPushDown_TextWithoutKeyword() throws ExpressionNotAnalyzableException {
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
            .add("b", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .add("c", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    final RexInputRef field3 =
        builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field3, stringLiteral);
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
    QueryBuilder builder = PredicateAnalyzer.analyze(call, schema, fieldTypes, rowType, cluster);
    assert (builder.toString().contains("\"lang\" : \"opensearch_compounded_script\""));
  }

  @Test
  void equals_scriptPushDown_Struct() throws ExpressionNotAnalyzableException {
    final RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.VARCHAR));
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("d", mapType)
            .build();
    final RexInputRef field4 = builder.makeInputRef(mapType, 0);
    final Map<String, ExprType> newFieldTypes =
        Map.of("d", OpenSearchDataType.of(ExprCoreType.STRUCT));
    final List<String> newSchema = List.of("d");
    RexNode call = builder.makeCall(SqlStdOperatorTable.IS_EMPTY, field4);
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
    QueryBuilder builder =
        PredicateAnalyzer.analyze(call, newSchema, newFieldTypes, rowType, cluster);
    assert (builder.toString().contains("\"lang\" : \"opensearch_compounded_script\""));
  }

  @Test
  void isTrue_predicate() throws ExpressionNotAnalyzableException {
    RexNode call =
        builder.makeCall(
            SqlStdOperatorTable.IS_TRUE,
            builder.makeCall(SqlStdOperatorTable.EQUALS, field2, stringLiteral));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermQueryBuilder.class, result);
    assertEquals(
        """
        {
          "term" : {
            "b.keyword" : {
              "value" : "Hi",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void isNullOr_ScriptPushDown() throws ExpressionNotAnalyzableException {
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
            .add("b", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    // PPL IS_EMPTY is translated to OR(IS_NULL(arg), IS_EMPTY(arg))
    RexNode call = PPLFuncImpTable.INSTANCE.resolve(builder, BuiltinFunctionName.IS_EMPTY, field2);
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
    QueryExpression expression =
        PredicateAnalyzer.analyzeExpression(call, schema, fieldTypes, rowType, cluster);
    assert (expression
        .builder()
        .toString()
        .contains("\"lang\" : \"opensearch_compounded_script\""));
  }

  @Test
  void verify_partial_pushdown() throws ExpressionNotAnalyzableException {
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
            .add("b", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    RexNode call1 = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, numericLiteral);
    RexNode call2 = builder.makeCall(SqlStdOperatorTable.IS_EMPTY, field2);
    // Partial push down part of and
    RexNode andCall = builder.makeCall(SqlStdOperatorTable.AND, List.of(call1, call2));
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));

    PredicateAnalyzer.Visitor visitor =
        new PredicateAnalyzer.Visitor(schema, fieldTypes, rowType, cluster);
    PredicateAnalyzer.Visitor visitSpy = spy(visitor);
    Mockito.doThrow(new PredicateAnalyzer.PredicateAnalyzerException(""))
        .when(visitSpy)
        .tryAnalyzeOperand(call2);
    QueryExpression result =
        PredicateAnalyzer.analyzeExpression(
            andCall, schema, fieldTypes, rowType, cluster, visitSpy);

    QueryBuilder resultBuilder = result.builder();
    assertInstanceOf(BoolQueryBuilder.class, resultBuilder);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "term" : {
                  "a" : {
                    "value" : 12,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        resultBuilder.toString());

    List<RexNode> unAnalyzableNodes = result.getUnAnalyzableNodes();
    assertEquals(1, unAnalyzableNodes.size());
    assertEquals(call2, unAnalyzableNodes.getFirst());

    // If the call2 throw PredicateAnalyzerException, the `or` expression converts to script
    // pushdown.
    RexNode orCall = builder.makeCall(SqlStdOperatorTable.OR, List.of(call1, call2));
    result =
        PredicateAnalyzer.analyzeExpression(orCall, schema, fieldTypes, rowType, cluster, visitSpy);
    resultBuilder = result.builder();
    assertInstanceOf(ScriptQueryBuilder.class, resultBuilder);

    Mockito.doThrow(new PredicateAnalyzer.PredicateAnalyzerException(""))
        .when(visitSpy)
        .tryAnalyzeOperand(orCall);
    RexNode thenAndCall = builder.makeCall(SqlStdOperatorTable.AND, List.of(orCall, call1));
    result =
        PredicateAnalyzer.analyzeExpression(
            thenAndCall, schema, fieldTypes, rowType, cluster, visitSpy);
    resultBuilder = result.builder();
    assertInstanceOf(BoolQueryBuilder.class, resultBuilder);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "term" : {
                  "a" : {
                    "value" : 12,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        resultBuilder.toString());
  }

  @Test
  void multiMatchWithoutFields_generatesMultiMatchQuery() throws ExpressionNotAnalyzableException {
    // Test multi_match with only query parameter (no fields)
    List<RexNode> arguments = List.of(aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "multi_match", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(MultiMatchQueryBuilder.class, result);
    assertEquals(
        """
        {
          "multi_match" : {
            "query" : "Hi",
            "fields" : [ ],
            "type" : "best_fields",
            "operator" : "OR",
            "slop" : 0,
            "prefix_length" : 0,
            "max_expansions" : 50,
            "zero_terms_query" : "NONE",
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void simpleQueryStringWithoutFields_generatesSimpleQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    // Test simple_query_string with only query parameter (no fields)
    List<RexNode> arguments = List.of(aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "simple_query_string", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(SimpleQueryStringBuilder.class, result);
    assertEquals(
        """
        {
          "simple_query_string" : {
            "query" : "Hi",
            "flags" : -1,
            "default_operator" : "or",
            "analyze_wildcard" : false,
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_prefix_length" : 0,
            "fuzzy_max_expansions" : 50,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void queryStringWithoutFields_generatesQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    // Test query_string with only query parameter (no fields)
    List<RexNode> arguments = List.of(aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "query_string", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(QueryStringQueryBuilder.class, result);
    assertEquals(
        """
        {
          "query_string" : {
            "query" : "Hi",
            "fields" : [ ],
            "type" : "best_fields",
            "default_operator" : "or",
            "max_determinized_states" : 10000,
            "enable_position_increments" : true,
            "fuzziness" : "AUTO",
            "fuzzy_prefix_length" : 0,
            "fuzzy_max_expansions" : 50,
            "phrase_slop" : 0,
            "escape" : false,
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void equals_generatesRangeQueryForDateTime() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field4, dateTimeLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "d" : {
              "from" : "1987-02-03T04:34:56.000Z",
              "to" : "1987-02-03T04:34:56.000Z",
              "include_lower" : true,
              "include_upper" : true,
              "format" : "date_time",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notEquals_generatesBoolQueryForDateTime() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, field4, dateTimeLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "should" : [
              {
                "range" : {
                  "d" : {
                    "from" : "1987-02-03T04:34:56.000Z",
                    "to" : null,
                    "include_lower" : false,
                    "include_upper" : true,
                    "format" : "date_time",
                    "boost" : 1.0
                  }
                }
              },
              {
                "range" : {
                  "d" : {
                    "from" : null,
                    "to" : "1987-02-03T04:34:56.000Z",
                    "include_lower" : true,
                    "include_upper" : false,
                    "format" : "date_time",
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void gte_generatesRangeQueryWithFormatForDateTime() throws ExpressionNotAnalyzableException {
    RexNode call =
        builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, field4, dateTimeLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "d" : {
              "from" : "1987-02-03T04:34:56.000Z",
              "to" : null,
              "include_lower" : true,
              "include_upper" : true,
              "format" : "date_time",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void isTrue_booleanField_generatesTermQuery() throws ExpressionNotAnalyzableException {
    // IS_TRUE(boolean_field) should generate a term query with value true
    RexNode call = builder.makeCall(SqlStdOperatorTable.IS_TRUE, field5);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermQueryBuilder.class, result);
    assertEquals(
        """
        {
          "term" : {
            "e" : {
              "value" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void isTrue_booleanFieldCombinedWithOtherCondition_generatesCompoundQuery()
      throws ExpressionNotAnalyzableException {
    // IS_TRUE(boolean_field) AND other_condition
    RexNode isTrueCall = builder.makeCall(SqlStdOperatorTable.IS_TRUE, field5);
    RexNode equalsCall = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, numericLiteral);
    RexNode andCall = builder.makeCall(SqlStdOperatorTable.AND, isTrueCall, equalsCall);
    QueryBuilder result = PredicateAnalyzer.analyze(andCall, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "term" : {
                  "e" : {
                    "value" : true,
                    "boost" : 1.0
                  }
                }
              },
              {
                "term" : {
                  "a" : {
                    "value" : 12,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }
}
