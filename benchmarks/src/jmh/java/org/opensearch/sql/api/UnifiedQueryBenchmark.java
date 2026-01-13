/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.sql.PreparedStatement;
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

/**
 * JMH benchmark for measuring the overhead of unified query API components when processing PPL
 * queries. This provides baseline metrics for integration with the opensearch-spark repository.
 *
 * <p>Benchmarks cover:
 *
 * <ul>
 *   <li>{@link UnifiedQueryPlanner}: PPL parsing and Calcite logical plan generation
 *   <li>{@link UnifiedQueryTranspiler}: Logical plan to SQL string conversion
 *   <li>{@link UnifiedQueryCompiler}: Logical plan to executable statement compilation
 * </ul>
 *
 * <p>Query patterns tested:
 *
 * <ul>
 *   <li>Simple source scan
 *   <li>Filter with WHERE clause
 *   <li>Aggregation with GROUP BY
 *   <li>Sort with ORDER BY
 *   <li>Combined operations (filter + aggregation + sort)
 * </ul>
 */
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class UnifiedQueryBenchmark extends UnifiedQueryTestBase {

  /** Common PPL query patterns for benchmarking. */
  @Param({
    "source = catalog.employees",
    "source = catalog.employees | where age > 30",
    "source = catalog.employees | stats count() by department",
    "source = catalog.employees | sort - age",
    "source = catalog.employees | where age > 25 | stats avg(age) by department | sort - department"
  })
  private String pplQuery;

  private UnifiedQueryTranspiler transpiler;
  private UnifiedQueryCompiler compiler;

  @Setup(Level.Trial)
  public void setUpBenchmark() {
    super.setUp();
    transpiler = UnifiedQueryTranspiler.builder().dialect(SparkSqlDialect.DEFAULT).build();
    compiler = new UnifiedQueryCompiler(context);
  }

  @TearDown(Level.Trial)
  public void tearDownBenchmark() throws Exception {
    super.tearDown();
  }

  /** Benchmarks PPL parsing and Calcite logical plan generation. */
  @Benchmark
  public RelNode planPplQuery() {
    return planner.plan(pplQuery);
  }

  /** Benchmarks the full transpilation pipeline: PPL → logical plan → SQL string. */
  @Benchmark
  public String transpilePplToSparkSql() {
    RelNode plan = planner.plan(pplQuery);
    return transpiler.toSql(plan);
  }

  /** Benchmarks the compilation pipeline: PPL → logical plan → executable statement. */
  @Benchmark
  public PreparedStatement compilePplQuery() {
    RelNode plan = planner.plan(pplQuery);
    return compiler.compile(plan);
  }
}
