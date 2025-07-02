/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.junit.jupiter.api.Test;
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
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.ExpressionNotAnalyzableException;

public class PredicateAnalyzerTest {
  final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  final RexBuilder builder = new RexBuilder(typeFactory);
  final List<String> schema = List.of("a", "b", "c");
  final Map<String, ExprType> fieldTypes =
      Map.of(
          "a", OpenSearchDataType.of(MappingType.Integer),
          "b",
              OpenSearchDataType.of(
                  MappingType.Text, Map.of("fields", Map.of("keyword", Map.of("type", "keyword")))),
          "c", OpenSearchDataType.of(MappingType.Text)); // Text without keyword cannot be push down
  final RexInputRef field1 =
      builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
  final RexInputRef field2 =
      builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
  final RexLiteral numericLiteral = builder.makeExactLiteral(new BigDecimal(12));
  final RexLiteral stringLiteral = builder.makeLiteral("Hi");
  final RexNode aliasedField2 =
      builder.makeCall(SqlStdOperatorTable.AS, field2, builder.makeLiteral("field"));
  final RexNode aliasedStringLiteral =
      builder.makeCall(SqlStdOperatorTable.AS, stringLiteral, builder.makeLiteral("query"));

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
            }""",
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
            }""",
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
            }""",
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
            }""",
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
            }""",
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
            }""",
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
            }""",
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
        }""",
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
                  12,
                  13,
                  14
                ],
                "boost" : 1.0
              }
            }""",
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
            }""",
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
            }""",
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
                SqlStdOperatorTable.AS, builder.makeLiteral("2"), builder.makeLiteral("slop")));
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
            }""",
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
                SqlStdOperatorTable.AS,
                builder.makeLiteral("1"),
                builder.makeLiteral("minimum_should_match")));
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
            }""",
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
                SqlStdOperatorTable.AS,
                builder.makeLiteral("standard"),
                builder.makeLiteral("analyzer")));
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
            }""",
        result.toString());
  }

  @Test
  void queryStringRelevanceQueryFunction_generatesQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            builder.makeCall(
                SqlStdOperatorTable.AS,
                builder.makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    builder.makeLiteral("b"),
                    builder.makeLiteral(
                        1.0, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true),
                    builder.makeLiteral("c"),
                    builder.makeLiteral(
                        2.5, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true)),
                builder.makeLiteral("fields")),
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.AS,
                builder.makeLiteral("1"),
                builder.makeLiteral("fuzziness")));
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
            }""",
        result.toString());
  }

  @Test
  void simpleQueryStringRelevanceQueryFunction_generatesSimpleQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            builder.makeCall(
                SqlStdOperatorTable.AS,
                builder.makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    builder.makeLiteral("b*"),
                    builder.makeLiteral(
                        1.0, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true)),
                builder.makeLiteral("fields")),
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
            }""",
        result.toString());
  }

  @Test
  void multiMatchRelevanceQueryFunction_generatesMultiMatchQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            builder.makeCall(
                SqlStdOperatorTable.AS,
                builder.makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    builder.makeLiteral("b*"),
                    builder.makeLiteral(
                        1.0, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true)),
                builder.makeLiteral("fields")),
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.AS,
                builder.makeLiteral("25"),
                builder.makeLiteral("max_expansions")));
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
            }""",
        result.toString());
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
            }""",
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
            }""",
        result.toString());
  }

  @Test
  void equals_throwException_TextWithoutKeyword() {
    final RexInputRef field3 =
        builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field3, stringLiteral);
    ExpressionNotAnalyzableException exception =
        assertThrows(
            ExpressionNotAnalyzableException.class,
            () -> PredicateAnalyzer.analyze(call, schema, fieldTypes));
    assertEquals("Can't convert =($2, 'Hi')", exception.getMessage());
  }

  @Test
  void equals_throwException_IncompatibleDateTimeOperands() {
    RexLiteral dateLiteral = builder.makeDateLiteral(DateString.fromDaysSinceEpoch(100));
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, dateLiteral);
    ExpressionNotAnalyzableException exception =
        assertThrows(
            ExpressionNotAnalyzableException.class,
            () -> PredicateAnalyzer.analyze(call, schema, fieldTypes));
    assertEquals("Can't convert =($0, 1970-04-11)", exception.getMessage());
  }
}
