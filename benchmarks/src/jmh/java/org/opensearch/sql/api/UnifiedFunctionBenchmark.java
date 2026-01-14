/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.List;
import java.util.concurrent.TimeUnit;
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

  private static final int OPS = 1000;

  @Param public BenchmarkCase benchmarkCase;

  private UnifiedFunctionRepository repository;
  private UnifiedFunction function;
  private List<Object> inputs;

  @Setup(Level.Trial)
  public void setUpBenchmark() {
    super.setUp();

    repository = new UnifiedFunctionRepository(context);
    function = benchmarkCase.loadFunction(repository);
    inputs = benchmarkCase.inputs();
  }

  @TearDown(Level.Trial)
  public void tearDownBenchmark() throws Exception {
    super.tearDown();
  }

  /** Benchmarks function loading from repository. */
  @Benchmark
  public UnifiedFunction loadFunction() {
    return benchmarkCase.loadFunction(repository);
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
  public enum BenchmarkCase {
    JSON_EXTRACT(
        List.of("VARCHAR", "VARCHAR"), List.of("{\"name\":\"test\",\"value\":42}", "$.name")),
    MOD(List.of("INTEGER", "INTEGER"), List.of(17, 5)),
    COALESCE(List.of("VARCHAR", "VARCHAR"), List.of("first_value", "default_value")),
    ARRAY(List.of("INTEGER", "INTEGER", "INTEGER"), List.of(1, 2, 3)),
    SHA2(List.of("VARCHAR", "INTEGER"), List.of("hello world", 256));

    private final List<String> inputTypes;
    private final List<Object> inputs;

    UnifiedFunction loadFunction(UnifiedFunctionRepository repository) {
      return repository
          .loadFunction(name())
          .map(desc -> desc.getBuilder().build(inputTypes))
          .orElseThrow();
    }

    List<Object> inputs() {
      return inputs;
    }
  }
}
