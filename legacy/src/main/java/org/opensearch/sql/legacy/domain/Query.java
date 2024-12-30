/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import java.util.ArrayList;
import java.util.List;

/** Represents abstract query. every query has indexes, types, and where clause. */
public abstract class Query implements QueryStatement {

  private Where where = null;
  private final List<From> from = new ArrayList<>();

  public Where getWhere() {
    return this.where;
  }

  public void setWhere(Where where) {
    this.where = where;
  }

  public List<From> getFrom() {
    return from;
  }

  /**
   * Get the indexes the query refer to.
   *
   * @return list of strings, the indexes names
   */
  public String[] getIndexArr() {
    String[] indexArr = new String[this.from.size()];
    for (int i = 0; i < indexArr.length; i++) {
      indexArr[i] = this.from.get(i).getIndex();
    }
    return indexArr;
  }
}
