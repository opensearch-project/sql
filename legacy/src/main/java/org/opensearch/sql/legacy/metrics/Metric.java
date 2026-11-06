/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

public abstract class Metric<T> implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  private final String name;

  public Metric(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public abstract T getValue();

  /** Write this metric's stats-response entries. Override to emit more than one key. */
  public void collectInto(org.json.JSONObject json) {
    json.put(getName(), getValue());
  }
}
