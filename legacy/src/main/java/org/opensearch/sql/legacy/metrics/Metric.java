/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

public abstract class Metric<T> implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  private String name;

  public Metric(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public abstract T getValue();
}
