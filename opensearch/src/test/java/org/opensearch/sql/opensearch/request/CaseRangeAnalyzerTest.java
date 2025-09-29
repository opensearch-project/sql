/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;

class CaseRangeAnalyzerTest {

  private RelDataTypeFactory typeFactory;
  private RexBuilder rexBuilder;
  private RelDataType rowType;
  private RexInputRef fieldRef;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);

    // Create a row type with fields: age (INTEGER), name (VARCHAR)
    rowType =
        typeFactory
            .builder()
            .add("age", SqlTypeName.INTEGER)
            .add("name", SqlTypeName.VARCHAR)
            .build();

    fieldRef = rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0); // age field
  }

  @Test
  void testAnalyzeSimpleCaseExpression() {
    // CASE
    //   WHEN age >= 18 THEN 'adult'
    //   WHEN age >= 13 THEN 'teen'
    //   ELSE 'child'
    // END

    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral literal13 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(13));
    RexLiteral adultLiteral = rexBuilder.makeLiteral("adult");
    RexLiteral teenLiteral = rexBuilder.makeLiteral("teen");
    RexLiteral childLiteral = rexBuilder.makeLiteral("child");

    RexCall condition1 =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal18);
    RexCall condition2 =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal13);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE,
                Arrays.asList(condition1, adultLiteral, condition2, teenLiteral, childLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("age_ranges", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();
    assertEquals("age_ranges", builder.getName());
    assertEquals("age", builder.field());

    String expectedJson =
        """
        {
          "age_ranges" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "adult",
                  "from" : 18.0
                },
                {
                  "key" : "teen",
                  "from" : 13.0,
                  "to" : 18.0
                },
                {
                  "key" : "child",
                  "to" : 13.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testAnalyzeLessThanComparison() {
    // CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END

    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral minorLiteral = rexBuilder.makeLiteral("minor");
    RexLiteral adultLiteral = rexBuilder.makeLiteral("adult");

    RexCall condition =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, fieldRef, literal18);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(condition, minorLiteral, adultLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("age_check", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();

    String expectedJson =
        """
        {
          "age_check" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "minor",
                  "to" : 18.0
                },
                {
                  "key" : "adult",
                  "from" : 18.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testAnalyzeWithSearchCondition() {
    // Create a SEARCH condition (Sarg-based range)
    TreeRangeSet<BigDecimal> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closedOpen(BigDecimal.valueOf(18), BigDecimal.valueOf(65)));

    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSet);
    RexNode sargLiteral =
        rexBuilder.makeSearchArgumentLiteral(sarg, typeFactory.createSqlType(SqlTypeName.DECIMAL));

    RexCall searchCall =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, Arrays.asList(fieldRef, sargLiteral));

    RexLiteral workingLiteral = rexBuilder.makeLiteral("working_age");
    RexLiteral otherLiteral = rexBuilder.makeLiteral("other");

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(searchCall, workingLiteral, otherLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("age_groups", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();

    String expectedJson =
        """
        {
          "age_groups" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "working_age",
                  "from" : 18.0,
                  "to" : 65.0
                },
                {
                  "key" : "other",
                  "to" : 18.0
                },
                {
                  "key" : "other",
                  "from" : 65.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testAnalyzeWithNullElse() {
    // CASE WHEN age >= 18 THEN 'adult' ELSE NULL END

    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral adultLiteral = rexBuilder.makeLiteral("adult");
    RexLiteral nullLiteral =
        rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR));

    RexCall condition =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal18);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(condition, adultLiteral, nullLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("age_check", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();
    // Should use DEFAULT_ELSE_KEY for null else clause

    String expectedJson =
        """
        {
          "age_check" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "adult",
                  "from" : 18.0
                },
                {
                  "key" : "null",
                  "to" : 18.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testAnalyzeWithNonLiteralResultShouldNotSucceed() {
    // CASE WHEN age >= 18 THEN age ELSE 0 END (non-literal result)

    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral zeroLiteral = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0));

    RexCall condition =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal18);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE,
                Arrays.asList(condition, fieldRef, zeroLiteral)); // fieldRef as result, not literal

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("test", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertFalse(result.isPresent());
  }

  @Test
  void testAnalyzeDifferentFieldsShouldReturnEmpty() {
    // Test comparing different fields in conditions
    RexInputRef nameFieldRef = rexBuilder.makeInputRef(rowType.getFieldList().get(1).getType(), 1);

    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral literalName = rexBuilder.makeLiteral("John");
    RexLiteral result1 = rexBuilder.makeLiteral("result1");
    RexLiteral result2 = rexBuilder.makeLiteral("result2");
    RexLiteral elseResult = rexBuilder.makeLiteral("else");

    RexCall condition1 =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal18); // age >= 18
    RexCall condition2 =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, nameFieldRef, literalName); // name = 'John'

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE,
                Arrays.asList(condition1, result1, condition2, result2, elseResult));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("test", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertFalse(result.isPresent());
  }

  @Test
  void testAnalyzeWithAndConditionShouldReturnEmpty() {
    // Test AND condition which should be unsupported
    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral literal65 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(65));
    RexLiteral resultLiteral = rexBuilder.makeLiteral("working_age");
    RexLiteral elseLiteral = rexBuilder.makeLiteral("other");

    RexCall condition1 =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal18);
    RexCall condition2 =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, fieldRef, literal65);
    RexCall andCondition =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.AND, condition1, condition2);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(andCondition, resultLiteral, elseLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("test", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertFalse(result.isPresent());
  }

  @Test
  void testAnalyzeWithOrConditionShouldReturnEmpty() {
    // Test OR condition which should be unsupported
    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral literal65 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(65));
    RexLiteral resultLiteral = rexBuilder.makeLiteral("age_group");
    RexLiteral elseLiteral = rexBuilder.makeLiteral("other");

    RexCall condition1 =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, fieldRef, literal18);
    RexCall condition2 =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal65);
    RexCall orCondition =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.OR, condition1, condition2);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(orCondition, resultLiteral, elseLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("test", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertFalse(result.isPresent());
  }

  @Test
  void testAnalyzeWithUnsupportedComparison() {
    // Test GREATER_THAN which should be converted to supported operations or fail
    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral resultLiteral = rexBuilder.makeLiteral("adult");
    RexLiteral elseLiteral = rexBuilder.makeLiteral("minor");

    RexCall condition =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN, fieldRef, literal18); // This should fail

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(condition, resultLiteral, elseLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("test", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertFalse(result.isPresent());
  }

  @Test
  void testAnalyzeWithReversedComparison() {
    // Test literal on left side: 18 <= age (should be converted to age >= 18)
    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral resultLiteral = rexBuilder.makeLiteral("adult");
    RexLiteral elseLiteral = rexBuilder.makeLiteral("minor");

    RexCall condition =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL, literal18, fieldRef); // 18 <= age

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(condition, resultLiteral, elseLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("reversed_test", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();

    String expectedJson =
        """
        {
          "reversed_test" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "adult",
                  "from" : 18.0
                },
                {
                  "key" : "minor",
                  "to" : 18.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testAnalyzeWithNullLiteralValue() {
    // Test with null literal value that can't be converted to Double
    RexLiteral nullLiteral =
        rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER));
    RexLiteral resultLiteral = rexBuilder.makeLiteral("result");
    RexLiteral elseLiteral = rexBuilder.makeLiteral("else");

    RexCall condition =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, nullLiteral);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(condition, resultLiteral, elseLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("test", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertFalse(result.isPresent());
  }

  @Test
  void testSimpleCaseGeneratesExpectedDSL() {
    // CASE WHEN age >= 18 THEN 'adult' ELSE 'minor' END

    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral adultLiteral = rexBuilder.makeLiteral("adult");
    RexLiteral minorLiteral = rexBuilder.makeLiteral("minor");

    RexCall condition =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal18);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(condition, adultLiteral, minorLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("age_groups", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();

    String expectedJson =
        """
        {
          "age_groups" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "adult",
                  "from" : 18.0
                },
                {
                  "key" : "minor",
                  "to" : 18.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testMultipleConditionsGenerateExpectedDSL() {
    // CASE
    //   WHEN age >= 65 THEN 'senior'
    //   WHEN age >= 18 THEN 'adult'
    //   ELSE 'minor'
    // END

    RexLiteral literal65 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(65));
    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral seniorLiteral = rexBuilder.makeLiteral("senior");
    RexLiteral adultLiteral = rexBuilder.makeLiteral("adult");
    RexLiteral minorLiteral = rexBuilder.makeLiteral("minor");

    RexCall condition1 =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal65);
    RexCall condition2 =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal18);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE,
                Arrays.asList(condition1, seniorLiteral, condition2, adultLiteral, minorLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("age_categories", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();

    String expectedJson =
        """
        {
          "age_categories" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "senior",
                  "from" : 65.0
                },
                {
                  "key" : "adult",
                  "from" : 18.0,
                  "to" : 65.0
                },
                {
                  "key" : "minor",
                  "to" : 18.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testLessThanConditionGeneratesExpectedDSL() {
    // CASE WHEN age < 21 THEN 'underage' ELSE 'legal' END

    RexLiteral literal21 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(21));
    RexLiteral underageLiteral = rexBuilder.makeLiteral("underage");
    RexLiteral legalLiteral = rexBuilder.makeLiteral("legal");

    RexCall condition =
        (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, fieldRef, literal21);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(condition, underageLiteral, legalLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("legal_status", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();

    String expectedJson =
        """
        {
          "legal_status" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "underage",
                  "to" : 21.0
                },
                {
                  "key" : "legal",
                  "from" : 21.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testNullElseClauseGeneratesExpectedDSL() {
    // CASE WHEN age >= 18 THEN 'adult' ELSE NULL END

    RexLiteral literal18 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(18));
    RexLiteral adultLiteral = rexBuilder.makeLiteral("adult");
    RexLiteral nullLiteral =
        rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR));

    RexCall condition =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, fieldRef, literal18);

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(condition, adultLiteral, nullLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("adult_check", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();

    String expectedJson =
        """
        {
          "adult_check" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "adult",
                  "from" : 18.0
                },
                {
                  "key" : "null",
                  "to" : 18.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testSearchConditionGeneratesExpectedDSL() {
    // Create a SEARCH condition (Sarg-based range): 18 <= age < 65
    TreeRangeSet<BigDecimal> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closedOpen(BigDecimal.valueOf(18), BigDecimal.valueOf(65)));

    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSet);
    RexNode sargLiteral =
        rexBuilder.makeSearchArgumentLiteral(sarg, typeFactory.createSqlType(SqlTypeName.DECIMAL));

    RexCall searchCall =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, Arrays.asList(fieldRef, sargLiteral));

    RexLiteral workingLiteral = rexBuilder.makeLiteral("working_age");
    RexLiteral otherLiteral = rexBuilder.makeLiteral("other");

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(searchCall, workingLiteral, otherLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("employment_status", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();

    String expectedJson =
        """
        {
          "employment_status" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "working_age",
                  "from" : 18.0,
                  "to" : 65.0
                },
                {
                  "key" : "other",
                  "to" : 18.0
                },
                {
                  "key" : "other",
                  "from" : 65.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  @Test
  void testSearchWithDiscontinuousRanges() {
    // age >= 20 && age < 30 -> '20-30'
    // age >= 40 && age <50 -> '40-50'
    // Create discontinuous ranges: [20, 30) and [40, 50)
    TreeRangeSet<BigDecimal> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closedOpen(BigDecimal.valueOf(20), BigDecimal.valueOf(30)));
    rangeSet.add(Range.closedOpen(BigDecimal.valueOf(40), BigDecimal.valueOf(50)));

    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSet);
    RexNode sargLiteral =
        rexBuilder.makeSearchArgumentLiteral(sarg, typeFactory.createSqlType(SqlTypeName.DECIMAL));

    RexCall searchCall =
        (RexCall)
            rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, Arrays.asList(fieldRef, sargLiteral));

    RexLiteral targetLiteral = rexBuilder.makeLiteral("target_age");
    RexLiteral otherLiteral =
        rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.VARCHAR));

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(searchCall, targetLiteral, otherLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("discontinuous_ranges", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertTrue(result.isPresent());
    RangeAggregationBuilder builder = result.get();

    String expectedJson =
        """
        {
          "discontinuous_ranges" : {
            "range" : {
              "field" : "age",
              "ranges" : [
                {
                  "key" : "target_age",
                  "from" : 20.0,
                  "to" : 30.0
                },
                {
                  "key" : "target_age",
                  "from" : 40.0,
                  "to" : 50.0
                },
                {
                  "key" : "null",
                  "to" : 20.0
                },
                {
                  "key" : "null",
                  "from" : 30.0,
                  "to" : 40.0
                },
                {
                  "key" : "null",
                  "from" : 50.0
                }
              ],
              "keyed" : true
            }
          }
        }""";

    assertEquals(normalizeJson(expectedJson), normalizeJson(builder.toString()));
  }

  /**
   * Helper method to normalize JSON strings for comparison by removing extra whitespace and
   * ensuring consistent formatting.
   */
  private String normalizeJson(String json) {
    return json.replaceAll("\\s+", " ").replaceAll("\\s*([{}\\[\\],:]?)\\s*", "$1").trim();
  }

  @Test
  void testAnalyzeSearchConditionWithInvalidField() {
    // Create a SEARCH condition with non-field reference
    TreeRangeSet<BigDecimal> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closedOpen(BigDecimal.valueOf(18), BigDecimal.valueOf(65)));

    Sarg<BigDecimal> sarg = Sarg.of(RexUnknownAs.UNKNOWN, rangeSet);
    RexNode sargLiteral =
        rexBuilder.makeSearchArgumentLiteral(sarg, typeFactory.createSqlType(SqlTypeName.DECIMAL));
    RexLiteral constantLiteral = rexBuilder.makeExactLiteral(BigDecimal.valueOf(42));

    RexCall searchCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.SEARCH,
                Arrays.asList(constantLiteral, sargLiteral)); // constant instead of field

    RexLiteral resultLiteral = rexBuilder.makeLiteral("result");
    RexLiteral elseLiteral = rexBuilder.makeLiteral("else");

    RexCall caseCall =
        (RexCall)
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE, Arrays.asList(searchCall, resultLiteral, elseLiteral));

    CaseRangeAnalyzer analyzer = CaseRangeAnalyzer.create("test", rowType);
    Optional<RangeAggregationBuilder> result = analyzer.analyze(caseCall);

    assertFalse(result.isPresent());
  }
}
