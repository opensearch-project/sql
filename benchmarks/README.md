# OpenSearch SQL/PPL Microbenchmark Suite

This directory contains the microbenchmark suite of OpenSearch SQL/PPL. It relies on [JMH](http://openjdk.java.net/projects/code-tools/jmh/).

## Purpose

Microbenchmarks are intended to spot performance regressions in performance-critical components.

The microbenchmark suite is also handy for ad-hoc microbenchmarks but please remove them again before merging your PR.

## Getting Started

Just run `./gradlew :benchmarks:jmh` from the project root directory or run specific benchmark via your IDE. It will build all microbenchmarks, execute them and print the result.

## Adding Microbenchmarks

Before adding a new microbenchmark, make yourself familiar with the JMH API. You can check our existing microbenchmarks and also the [JMH samples](http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/).

In contrast to tests, the actual name of the benchmark class is not relevant to JMH. However, stick to the naming convention and end the class name of a benchmark with `Benchmark`. To have JMH execute a benchmark, annotate the respective methods with `@Benchmark`.