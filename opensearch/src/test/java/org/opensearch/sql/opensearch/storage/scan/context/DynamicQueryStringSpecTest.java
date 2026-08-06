/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.SemanticCheckException;

class DynamicQueryStringSpecTest {

  private JavaTypeFactoryImpl typeFactory;
  private RexBuilder rexBuilder;
  private RelDataType varcharType;

  @BeforeEach
  void setUp() {
    typeFactory = new JavaTypeFactoryImpl();
    rexBuilder = new RexBuilder(typeFactory);
    varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
  }

  @Test
  void createsPartsAndBuildsRuntimeQuery() {
    RexNode staticPart = rexBuilder.makeLiteral("age:36 AND ");
    RexNode runtimePart = rexBuilder.makeLiteral("( account_number=\"6\" )");
    RexNode expression =
        rexBuilder.makeCall(
            varcharType, SqlStdOperatorTable.CONCAT, List.of(staticPart, runtimePart));

    DynamicQueryStringSpec spec =
        DynamicQueryStringSpec.create(
            expression,
            List.of(runtimePart),
            predicate -> predicate.replace("account_number=\"6\"", "account_number:6"));

    assertEquals(List.of(staticPart, runtimePart), spec.queryParts());
    assertEquals(Set.of(1), spec.runtimePredicateParts());
    assertEquals(
        "age:36 AND ( account_number:6 )",
        spec.buildRuntimeQuery(new String[] {"age:36 AND ", "( account_number=\"6\" )"}));
  }

  @Test
  void marksEveryOccurrenceOfTheSameRuntimePredicate() {
    RexNode runtimePart = rexBuilder.makeLiteral("( account_number=\"6\" )");
    RexNode separator = rexBuilder.makeLiteral(" OR ");
    RexNode expression =
        rexBuilder.makeCall(
            varcharType,
            SqlStdOperatorTable.CONCAT,
            List.of(
                rexBuilder.makeCall(
                    varcharType, SqlStdOperatorTable.CONCAT, List.of(runtimePart, separator)),
                runtimePart));

    DynamicQueryStringSpec spec =
        DynamicQueryStringSpec.create(
            expression, List.of(runtimePart), predicate -> "account_number:6");

    assertEquals(Set.of(0, 2), spec.runtimePredicateParts());
    assertEquals(
        "account_number:6 OR account_number:6",
        spec.buildRuntimeQuery(
            new String[] {"( account_number=\"6\" )", " OR ", "( account_number=\"6\" )"}));
  }

  @Test
  void reportsCorrelationIds() {
    RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
    CorrelationId correlationId = cluster.createCorrel();
    RelDataType rowType = typeFactory.builder().add("search", varcharType).build();
    RexNode correlation = rexBuilder.makeCorrel(rowType, correlationId);
    RexNode runtimePart = rexBuilder.makeFieldAccess(correlation, 0);

    DynamicQueryStringSpec spec =
        DynamicQueryStringSpec.create(runtimePart, List.of(runtimePart), predicate -> predicate);

    assertEquals(Set.of(correlationId), spec.correlationIds());
  }

  @Test
  void rejectsNonCharacterQueryPartsBeforeCodeGeneration() {
    RexNode integerPart = rexBuilder.makeExactLiteral(BigDecimal.ONE);

    SemanticCheckException exception =
        assertThrows(
            SemanticCheckException.class,
            () ->
                new DynamicQueryStringSpec(
                    integerPart, List.of(integerPart), Set.of(0), predicate -> predicate));

    assertEquals(
        "The search command can only use text produced by a subsearch, but the subsearch returned"
            + " INTEGER. Convert the value with tostring() before returning it.",
        exception.getMessage());
  }

  @Test
  void rejectsUnexpectedRuntimeValueCount() {
    RexNode runtimePart = rexBuilder.makeLiteral("( account_number=\"6\" )");
    DynamicQueryStringSpec spec =
        DynamicQueryStringSpec.create(
            runtimePart, List.of(runtimePart), predicate -> "account_number:6");

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> spec.buildRuntimeQuery(new String[] {"first", "second"}));

    assertEquals(
        "The search command could not apply the subsearch result.", exception.getMessage());
  }

  @Test
  void rejectsRuntimePredicateThatIsNotPartOfQueryExpression() {
    RexNode queryPart = rexBuilder.makeLiteral("status:500");
    RexNode unrelatedPart = rexBuilder.makeLiteral("host:api");

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                DynamicQueryStringSpec.create(
                    queryPart, List.of(unrelatedPart), predicate -> predicate));

    assertEquals(
        "The search command could not apply the subsearch result.", exception.getMessage());
  }

  @Test
  void rejectsNullStaticRuntimeValue() {
    RexNode staticPart = rexBuilder.makeLiteral("status:500");
    DynamicQueryStringSpec spec =
        DynamicQueryStringSpec.create(staticPart, List.of(), predicate -> predicate);

    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class, () -> spec.buildRuntimeQuery(new String[] {null}));

    assertEquals(
        "The search command could not apply the subsearch result.", exception.getMessage());
  }

  @Test
  void rejectsNullRuntimeValueArray() {
    RexNode runtimePart = rexBuilder.makeLiteral("status=500");
    DynamicQueryStringSpec spec =
        DynamicQueryStringSpec.create(runtimePart, List.of(runtimePart), predicate -> predicate);

    NullPointerException exception =
        assertThrows(NullPointerException.class, () -> spec.buildRuntimeQuery(null));

    assertEquals(
        "The search command could not apply the subsearch result.", exception.getMessage());
  }

  @Test
  void flattensConcatFunctionAndExposesOriginalExpression() {
    RexNode left = rexBuilder.makeLiteral("status:500");
    RexNode right = rexBuilder.makeLiteral(" AND host:api");
    RexNode expression =
        rexBuilder.makeCall(varcharType, SqlLibraryOperators.CONCAT_FUNCTION, List.of(left, right));

    DynamicQueryStringSpec spec =
        DynamicQueryStringSpec.create(expression, List.of(), predicate -> predicate);

    assertEquals(expression, spec.queryExpression());
    assertEquals(List.of(left, right), spec.queryParts());
  }

  @Test
  void keepsNonConcatenationCallAsOneQueryPart() {
    RexNode expression =
        rexBuilder.makeCall(
            SqlStdOperatorTable.REPLACE,
            rexBuilder.makeLiteral("status=500"),
            rexBuilder.makeLiteral("="),
            rexBuilder.makeLiteral(":"));

    DynamicQueryStringSpec spec =
        DynamicQueryStringSpec.create(expression, List.of(), predicate -> predicate);

    assertEquals(List.of(expression), spec.queryParts());
  }
}
