/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

public class NumericMetric<T> extends Metric<T> {

  private Counter<T> counter;

  public NumericMetric(String name, Counter counter) {
    super(name);
    this.counter = counter;
  }

  public String getName() {
    return super.getName();
  }

  public Counter<T> getCounter() {
    return counter;
  }

  public void increment() {
    counter.increment();
  }

  public void increment(long n) {
    counter.add(n);
  }

  public T getValue() {
    return counter.getValue();
  }

  public void clear() {
    counter.reset();
  }
}
