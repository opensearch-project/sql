/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.alias;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;

/** Util class for identifier expression parsing */
class Identifier {

  private final SQLIdentifierExpr idExpr;

  Identifier(SQLIdentifierExpr idExpr) {
    this.idExpr = idExpr;
  }

  String name() {
    return idExpr.getName();
  }

  boolean hasPrefix() {
    return firstDotIndex() != -1;
  }

  /** Assumption: identifier has prefix */
  String prefix() {
    return name().substring(0, firstDotIndex());
  }

  /** Assumption: identifier has prefix */
  void removePrefix() {
    String nameWithoutPrefix = name().substring(prefix().length() + 1);
    idExpr.setName(nameWithoutPrefix);
  }

  private int firstDotIndex() {
    return name().indexOf('.', 1);
  }
}
