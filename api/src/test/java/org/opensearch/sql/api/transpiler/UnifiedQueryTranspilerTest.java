/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.transpiler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.calcite.knn.KnnErrors;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.ppl.calcite.OpenSearchSparkSqlDialect;

public class UnifiedQueryTranspilerTest extends UnifiedQueryTestBase {

  private UnifiedQueryTranspiler transpiler;

  @Before
  public void setUp() {
    super.setUp();
    transpiler = UnifiedQueryTranspiler.builder().dialect(SparkSqlDialect.DEFAULT).build();
  }

  @Test
  public void testToSql() {
    String pplQuery = "source = catalog.employees";
    RelNode plan = planner.plan(pplQuery);

    String actualSql = transpiler.toSql(plan);
    String expectedSql = normalize("SELECT *\nFROM `catalog`.`employees`");
    assertEquals(
        "Transpiled SQL using SparkSqlDialect should match expected SQL", expectedSql, actualSql);
  }

  @Test
  public void testToSqlWithCustomDialect() {
    String pplQuery = "source = catalog.employees | where name = 123";
    RelNode plan = planner.plan(pplQuery);

    UnifiedQueryTranspiler customTranspiler =
        UnifiedQueryTranspiler.builder().dialect(OpenSearchSparkSqlDialect.DEFAULT).build();
    String actualSql = customTranspiler.toSql(plan);
    String expectedSql =
        normalize(
            "SELECT *\nFROM `catalog`.`employees`\nWHERE TRY_CAST(`name` AS DOUBLE) = 1.230E2");
    assertEquals(
        "Transpiled query using OpenSearchSparkSqlDialect should translate SAFE_CAST to TRY_CAST",
        expectedSql,
        actualSql);
  }

  @Test
  public void testKnnScoreIsNotTranspilable() {
    RelBuilder builder = context.getPlanContext().relBuilder;
    RexBuilder rexBuilder = builder.getRexBuilder();
    RelNode plan =
        builder
            .push(planner.plan("source = catalog.employees"))
            .project(
                builder.alias(
                    rexBuilder.makeCall(
                        PPLBuiltinOperators.KNN_SCORE,
                        builder.field("name"),
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, builder.literal(1.0f))),
                    "_score"))
            .build();

    for (UnifiedQueryTranspiler target :
        List.of(
            transpiler,
            UnifiedQueryTranspiler.builder().dialect(OpenSearchSparkSqlDialect.DEFAULT).build())) {
      SemanticCheckException e =
          assertThrows(SemanticCheckException.class, () -> target.toSql(plan));
      assertEquals(KnnErrors.NOT_TRANSPILABLE_MESSAGE, e.getMessage());
    }
  }

  /**
   * KNN_SCORE inside a subsearch is found too: {@code where id in [ ... knn ... ]} and a scalar
   * subsearch over a k-NN step, transpiled to PostgreSQL, which would otherwise print the call.
   */
  @Test
  public void testKnnScoreInASubsearchIsNotTranspilable() {
    RelBuilder builder = context.getPlanContext().relBuilder;
    RexNode knnScore =
        builder
            .getRexBuilder()
            .makeCall(
                PPLBuiltinOperators.KNN_SCORE,
                builder.push(planner.plan("source = catalog.employees")).field("name"),
                builder
                    .getRexBuilder()
                    .makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, builder.literal(1.0f)));
    // The top-k boundary keeps a later projection from merging the score away, as in a k-NN plan.
    RelNode knnRows =
        builder
            .project(builder.field("id"), builder.alias(knnScore, "_score"))
            .sortLimit(0, 2, List.of(builder.desc(builder.field("_score"))))
            .build();
    RelNode inSubsearch =
        builder
            .push(planner.plan("source = catalog.employees"))
            .filter(
                RexSubQuery.in(
                    builder.push(knnRows).project(builder.field("id")).build(),
                    ImmutableList.of(builder.field("id"))))
            .build();
    RelNode scalarSubsearch =
        builder
            .push(planner.plan("source = catalog.employees"))
            .project(
                builder.field("id"),
                RexSubQuery.scalar(
                    builder
                        .push(knnRows)
                        .aggregate(builder.groupKey(), builder.max("m", builder.field("_score")))
                        .build()))
            .build();

    UnifiedQueryTranspiler postgres =
        UnifiedQueryTranspiler.builder().dialect(PostgresqlSqlDialect.DEFAULT).build();
    for (RelNode plan : List.of(inSubsearch, scalarSubsearch)) {
      SemanticCheckException e =
          assertThrows(SemanticCheckException.class, () -> postgres.toSql(plan));
      assertEquals(KnnErrors.NOT_TRANSPILABLE_MESSAGE, e.getMessage());
    }
  }

  /** Normalizes line endings to platform-specific format for cross-platform test compatibility. */
  private String normalize(String sql) {
    return sql.replace("\n", System.lineSeparator());
  }
}
