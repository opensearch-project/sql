/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** Utility class for creating MAP access operations in Calcite. */
public class MapAccessOperations {

  /**
   * Creates a MAP access expression: MAP_GET(mapExpr, key)
   *
   * @param rexBuilder The RexBuilder to create expressions
   * @param mapExpr The MAP expression to access
   * @param key The key to access in the MAP
   * @return RexNode representing MAP_GET operation
   */
  public static RexNode mapGet(RexBuilder rexBuilder, RexNode mapExpr, String key) {
    RexNode keyLiteral = rexBuilder.makeLiteral(key);
    return rexBuilder.makeCall(PPLBuiltinOperators.MAP_GET, mapExpr, keyLiteral);
  }
}
