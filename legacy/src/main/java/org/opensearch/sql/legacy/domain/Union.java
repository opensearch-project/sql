/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.domain;

import lombok.Getter;

@Getter
public class Union extends Query {
  private final Select firstTable;
  private final Select secondTable;

  public Union(Select firstTable, Select secondTable) {
    this.firstTable = firstTable;
    this.secondTable = secondTable;
    this.firstTable.setPartOfUnion(true);
    this.secondTable.setPartOfUnion(true);
  }
}
