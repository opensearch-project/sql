/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.sql.api.function.UnifiedFunction;
import org.opensearch.sql.api.function.UnifiedFunctionRepository;

/**
 * JMH benchmark for measuring {@link UnifiedFunction} performance. Tests one representative
 * function per PPL category (json, math, conditional, collection, string).
 *
 * <p>Benchmarks:
 *
 * <ul>
 *   <li>{@link #loadFunction()}: Measures function loading from repository
 *   <li>{@link #evalFunction(Blackhole)}: Measures function evaluation (1000 calls per invocation)
 * </ul>
 */
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class UnifiedFunctionBenchmark extends UnifiedQueryTestBase {

  /** Number of function evaluations per benchmark invocation. */
  private static final int OPS = 1000;

  /** Benchmark specification selecting which function to test. */
  @Param public BenchmarkSpec benchmarkSpec;

  /** Repository for loading unified functions. */
  private UnifiedFunctionRepository repository;

  /** Pre-loaded function instance for evaluation benchmarks. */
  private UnifiedFunction function;

  /** Input arguments for function evaluation. */
  private List<Object> inputs;

  @Setup(Level.Trial)
  public void setUpBenchmark() {
    super.setUp();

    repository = new UnifiedFunctionRepository(context);
    function = benchmarkSpec.loadFunction(repository);
    inputs = benchmarkSpec.getInputs();
  }

  @TearDown(Level.Trial)
  public void tearDownBenchmark() throws Exception {
    super.tearDown();
  }

  /** Benchmarks function loading from repository. */
  @Benchmark
  public UnifiedFunction loadFunction() {
    return benchmarkSpec.loadFunction(repository);
  }

  /** Benchmarks function evaluation with pre-loaded function and inputs. */
  @Benchmark
  @OperationsPerInvocation(OPS)
  public void evalFunction(Blackhole bh) {
    for (int i = 0; i < OPS; i++) {
      bh.consume(function.eval(inputs));
    }
  }

  /** Enum defining benchmark test cases - one representative function per PPL category. */
  @RequiredArgsConstructor
  public enum BenchmarkSpec {
    JSON_EXTRACT(
        inputTypes("VARCHAR", "VARCHAR"),
        sampleInputs("{\"name\":\"test\",\"value\":42}", "$.name")),
    COALESCE(
        inputTypes("VARCHAR", "VARCHAR", "VARCHAR"),
        sampleInputs(null, "first_value", "default_value")),
    MVFIND(
        inputTypes("ARRAY", "VARCHAR"),
        sampleInputs(List.of("debug", "error", "warn", "info"), "err.*")),
    REX_EXTRACT(
        inputTypes("VARCHAR", "VARCHAR", "INTEGER"),
        sampleInputs("192.168.1.1 - GET /api", "(\\d+)", 1));

    private final List<String> inputTypes;
    @Getter private final List<Object> inputs;

    UnifiedFunction loadFunction(UnifiedFunctionRepository repository) {
      return repository
          .loadFunction(name())
          .map(desc -> desc.getBuilder().build(inputTypes))
          .orElseThrow();
    }

    private static List<String> inputTypes(String... types) {
      return List.of(types);
    }

    private static List<Object> sampleInputs(Object... args) {
      return Arrays.asList(args);
    }
  }
}
