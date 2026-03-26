/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.sql.PreparedStatement;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.api.transpiler.UnifiedQueryTranspiler;
import org.opensearch.sql.executor.QueryType;

/**
 * JMH benchmark for measuring the overhead of unified query API components when processing PPL and
 * SQL queries. The {@code language} and {@code queryPattern} parameters produce a cross-product,
 * enabling side-by-side comparison of equivalent queries across both languages.
 */
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class UnifiedQueryBenchmark extends UnifiedQueryTestBase {

  private static final Map<String, String> PPL_QUERIES =
      Map.of(
          "scan", "source = catalog.employees",
          "filter", "source = catalog.employees | where age > 30",
          "aggregate", "source = catalog.employees | stats count() by department",
          "sort", "source = catalog.employees | sort - age",
          "complex",
              """
              source = catalog.employees \
              | where age > 25 \
              | stats avg(age) by department \
              | sort - department\
              """);

  private static final Map<String, String> SQL_QUERIES =
      Map.of(
          "scan", "SELECT * FROM catalog.employees",
          "filter",
              """
              SELECT *
              FROM catalog.employees
              WHERE age > 30\
              """,
          "aggregate",
              """
              SELECT department, count(*)
              FROM catalog.employees
              GROUP BY department\
              """,
          "sort",
              """
              SELECT *
              FROM catalog.employees
              ORDER BY age DESC\
              """,
          "complex",
              """
              SELECT department, avg(age)
              FROM catalog.employees
              WHERE age > 25
              GROUP BY department
              ORDER BY department\
              """);

  @Param({"PPL", "SQL"})
  private String language;

  @Param({"scan", "filter", "aggregate", "sort", "complex"})
  private String queryPattern;

  private String query;
  private UnifiedQueryTranspiler transpiler;
  private UnifiedQueryCompiler compiler;

  @Override
  protected QueryType queryType() {
    return QueryType.valueOf(language);
  }

  @Setup(Level.Trial)
  public void setUpBenchmark() {
    super.setUp();
    query = (language.equals("PPL") ? PPL_QUERIES : SQL_QUERIES).get(queryPattern);
    transpiler = UnifiedQueryTranspiler.builder().dialect(SparkSqlDialect.DEFAULT).build();
    compiler = new UnifiedQueryCompiler(context);
  }

  @TearDown(Level.Trial)
  public void tearDownBenchmark() throws Exception {
    super.tearDown();
  }

  /** Benchmarks query parsing and Calcite logical plan generation. */
  @Benchmark
  public RelNode planQuery() {
    return planner.plan(query);
  }

  /** Benchmarks the full transpilation pipeline: Query → logical plan → SQL string. */
  @Benchmark
  public String transpileToSql() {
    RelNode plan = planner.plan(query);
    return transpiler.toSql(plan);
  }

  /**
   * Benchmarks the compilation pipeline: Query → logical plan → executable statement. The result
   * includes both compile and close time; close overhead is negligible and avoids resource leaking
   * during benchmark runs.
   */
  @Benchmark
  public void compileQuery() throws Exception {
    RelNode plan = planner.plan(query);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      // Statement is auto-closed after benchmark iteration
    }
  }
}
