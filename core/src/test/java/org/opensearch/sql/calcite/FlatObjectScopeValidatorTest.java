/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import java.math.BigDecimal;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;

/**
 * What a query may do with a flat_object field: project a leaf, and look one up in the index -- by
 * value, by existence or by a pattern. Everything else is rejected while the plan is built.
 *
 * <p>The plans here are built as the commands build them: a flat_object field is a column of type
 * {@code MAP<VARCHAR, VARIANT>}, and a leaf of one is {@code ITEM(<that column>, '<path>')}.
 */
class FlatObjectScopeValidatorTest {

  private final RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
  private final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);

  /** A row of two columns: `service`, a keyword, and `attributes`, a flat_object field. */
  private final RelDataType rowType =
      TYPE_FACTORY
          .builder()
          .add("service", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
          .add(
              "attributes",
              TYPE_FACTORY.createMapType(
                  TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
                  TYPE_FACTORY.createSqlType(SqlTypeName.VARIANT, true),
                  true))
          .build();

  private RelNode scan() {
    return LogicalValues.createEmpty(cluster, rowType);
  }

  private RexNode field(int index) {
    return rexBuilder.makeInputRef(rowType.getFieldList().get(index).getType(), index);
  }

  /** {@code attributes.<path>}, as the resolver builds it. */
  private RexNode leaf(String path) {
    return rexBuilder.makeCall(SqlStdOperatorTable.ITEM, field(1), rexBuilder.makeLiteral(path));
  }

  /** The shape a leaf takes when it is compared with text: a cast to VARCHAR over the item. */
  private RexNode leafAsText(String path) {
    return rexBuilder.makeCast(
        TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), leaf(path), true, false);
  }

  private RelNode filter(RexNode condition) {
    return LogicalFilter.create(scan(), condition);
  }

  private RelNode project(RexNode... expressions) {
    List<RexNode> list = List.of(expressions);
    List<String> names = new java.util.ArrayList<>();
    for (int i = 0; i < list.size(); i++) {
      names.add("c" + i);
    }
    return LogicalProject.create(scan(), ImmutableList.of(), list, names);
  }

  private void allowed(RelNode plan) {
    assertDoesNotThrow(() -> FlatObjectScopeValidator.validate(plan));
  }

  /** The message says what this query did; the rule and the doc link ride on the report. */
  private void rejected(RelNode plan, String what) {
    ErrorReport ex = assertThrows(ErrorReport.class, () -> FlatObjectScopeValidator.validate(plan));
    assertTrue(ex.getMessage().startsWith(what), ex.getMessage());
    assertEquals(ErrorCode.UNSUPPORTED_OPERATION, ex.getCode());
    assertTrue(ex.getSuggestion().contains("can only be projected"), ex.getSuggestion());
    assertTrue(
        ex.getSuggestion().contains("supported-field-types/flat-object/"), ex.getSuggestion());
  }

  // ---- projection

  @Test
  void projectingAFieldALeafOrAnotherColumn() {
    allowed(project(field(1)));
    allowed(project(leaf("namespace")));
    allowed(project(field(0), leaf("duration_ms")));
  }

  @Test
  void anExpressionOverALeafIsRejected() {
    RexNode plusOne =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeCast(
                TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), leaf("d"), true, true),
            rexBuilder.makeExactLiteral(BigDecimal.ONE));
    rejected(project(plusOne), "Cannot evaluate an expression over attributes.d");
  }

  // A cast of a leaf reads the value, and a leaf not written as text casts to null: casting 12.5
  // to a string would answer null rather than "12.5".
  @Test
  void aCastOfALeafIsRejectedInAProjection() {
    rejected(
        project(leafAsText("duration_ms")),
        "Cannot evaluate an expression over attributes.duration_ms");
  }

  @Test
  void aLeafNamedTwiceIsReportedOnce() {
    RexNode selfPlus =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeCast(
                TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), leaf("d"), true, true),
            rexBuilder.makeCast(
                TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), leaf("d"), true, true));
    rejected(project(selfPlus), "Cannot evaluate an expression over attributes.d");
  }

  // ---- filters the index answers

  @Test
  void exactTextEitherWayRound() {
    allowed(
        filter(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                leafAsText("namespace"),
                rexBuilder.makeLiteral("prod"))));
    allowed(
        filter(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeLiteral("prod"),
                leafAsText("namespace"))));
  }

  // Comparing a leaf with text casts one side to the other's type; the literal underneath is what
  // is compared either way.
  @Test
  void exactTextWithTheLiteralCastToTheLeafType() {
    RexNode asVariant =
        rexBuilder.makeCast(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARIANT, true),
            rexBuilder.makeLiteral("prod"),
            true,
            false);
    allowed(filter(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leaf("namespace"), asVariant)));
  }

  @Test
  void existenceAndAbsence() {
    allowed(filter(rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, leaf("error.type"))));
    allowed(filter(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, leaf("error.type"))));
  }

  @Test
  void aLeadingPrefix() {
    allowed(
        filter(
            rexBuilder.makeCall(
                SqlStdOperatorTable.LIKE,
                leafAsText("namespace"),
                rexBuilder.makeLiteral("ns-%"))));
    allowed(
        filter(
            rexBuilder.makeCall(
                SqlLibraryOperators.ILIKE,
                leafAsText("namespace"),
                rexBuilder.makeLiteral("NS-%"))));
  }

  // `x = 'a' or x = 'b'` and `x in ('a', 'b')` reach the validator as one SEARCH over a set of
  // points, which the index answers with one terms query.
  @Test
  void aSetOfExactTexts() {
    allowed(filter(search("prod", "staging")));
    allowed(
        filter(
            rexBuilder.makeCall(
                SqlStdOperatorTable.OR,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS, leafAsText("ns"), rexBuilder.makeLiteral("prod")),
                rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    leafAsText("ns"),
                    rexBuilder.makeLiteral("stage")))));
  }

  // A term query answers a leaf whatever the literal looks like: the index files the number 503
  // and the text "503" under one term, which is the answer the same DSL query gives.
  @Test
  void aLiteralThatIsNotPlainText() {
    for (String literal : List.of("503", "12.5", "-1", "1e3", "true", "false", "null")) {
      allowed(
          filter(
              rexBuilder.makeCall(
                  SqlStdOperatorTable.EQUALS,
                  leafAsText("code"),
                  rexBuilder.makeLiteral(literal))));
    }
    allowed(
        filter(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                leaf("code"),
                rexBuilder.makeExactLiteral(BigDecimal.valueOf(503)))));
  }

  // `!=` is an exists filter with the term excluded; `not like` the same with the pattern.
  @Test
  void aNegatedTextFilter() {
    allowed(
        filter(
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT_EQUALS, leafAsText("ns"), rexBuilder.makeLiteral("prod"))));
    allowed(
        filter(
            rexBuilder.makeCall(
                SqlStdOperatorTable.NOT,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.LIKE, leafAsText("ns"), rexBuilder.makeLiteral("ns-%")))));
  }

  // Any pattern, not only a leading prefix: the index answers the rest with a wildcard query.
  @Test
  void anInfixOrSuffixPattern() {
    for (String pattern : List.of("%prod", "%pro%", "p_od", "5%")) {
      allowed(
          filter(
              rexBuilder.makeCall(
                  SqlStdOperatorTable.LIKE, leafAsText("ns"), rexBuilder.makeLiteral(pattern))));
    }
  }

  @Test
  void aFilterOnAnotherFieldIsUntouched() {
    allowed(
        filter(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, field(0), rexBuilder.makeLiteral("checkout"))));
  }

  // ---- filters the index cannot answer

  @Test
  void aNumericComparison() {
    RexNode greater =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeCast(
                TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), leaf("d"), true, true),
            rexBuilder.makeExactLiteral(BigDecimal.TEN));
    rejected(filter(greater), "Cannot compare attributes.d with >");
  }

  // `between`, and comparisons Calcite folds together, arrive as a SEARCH over a range; the
  // message must not name "search", which is Calcite's word for the folded call.
  @Test
  void aRangeThatCalciteFoldedIntoASearch() {
    RexNode range =
        rexBuilder.makeIn(
            leafAsText("code"), List.of(rexBuilder.makeLiteral("a"), rexBuilder.makeLiteral("b")));
    // a points Sarg is allowed; a range one is the comparison case
    allowed(filter(range));
    RexNode between =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                leafAsText("code"),
                rexBuilder.makeLiteral("a")),
            rexBuilder.makeCall(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                leafAsText("code"),
                rexBuilder.makeLiteral("z")));
    rejected(filter(between), "Cannot compare attributes.code");
  }

  @Test
  void aLeafComparedWithAnotherColumn() {
    RexNode columns = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leafAsText("ns"), field(0));
    rejected(filter(columns), "Cannot filter attributes.ns with =");
  }

  // ---- positions rather than expressions

  @Test
  void sortingByALeafOrTheField() {
    rejected(
        LogicalSort.create(project(leaf("duration_ms")), RelCollations.of(0), null, null),
        "Cannot sort by attributes.duration_ms");
    rejected(
        LogicalSort.create(scan(), RelCollations.of(1), null, null), "Cannot sort by attributes");
  }

  @Test
  void groupingByALeaf() {
    RelNode plan =
        LogicalAggregate.create(
            project(leaf("namespace")),
            ImmutableList.of(),
            ImmutableBitSet.of(0),
            null,
            ImmutableList.of());
    rejected(plan, "Cannot group by attributes.namespace");
  }

  @Test
  void aggregatingALeaf() {
    AggregateCall count =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            ImmutableList.of(),
            ImmutableList.of(0),
            -1,
            null,
            RelCollations.EMPTY,
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            "c");
    RelNode plan =
        LogicalAggregate.create(
            project(leaf("duration_ms")),
            ImmutableList.of(),
            ImmutableBitSet.of(),
            null,
            ImmutableList.of(count));
    rejected(plan, "Cannot apply count to attributes.duration_ms");
  }

  @Test
  void aPlanThatDoesNotTouchTheFieldIsUntouched() {
    allowed(
        LogicalSort.create(
            filter(
                rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS, field(0), rexBuilder.makeLiteral("checkout"))),
            RelCollations.of(0),
            null,
            null));
  }

  /** A SEARCH over a set of text points: the shape `in (...)` and a chain of `or` arrive as. */
  private RexNode search(String... points) {
    ImmutableRangeSet.Builder<NlsString> ranges = ImmutableRangeSet.builder();
    for (String point : points) {
      // the literal a text value takes inside a Sarg, charset and collation included
      RexLiteral literal = (RexLiteral) rexBuilder.makeLiteral(point);
      ranges.add(Range.singleton(requireNonNull(literal.getValueAs(NlsString.class))));
    }
    return rexBuilder.makeCall(
        SqlStdOperatorTable.SEARCH,
        leafAsText("ns"),
        rexBuilder.makeSearchArgumentLiteral(
            Sarg.of(RexUnknownAs.UNKNOWN, ranges.build()),
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)));
  }
}
