/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

/** Represents the from clause. Contains index and type which the query refer to. */
public class From {
  private String index;
  private String alias;

  /**
   * Extract index and type from the 'from' string
   *
   * @param from The part after the FROM keyword.
   */
  public From(String from) {
    index = from;
  }

  public From(String from, String alias) {
    this(from);
    this.alias = alias;
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder(index);
    if (alias != null) {
      str.append(" AS ").append(alias);
    }
    return str.toString();
  }
}
