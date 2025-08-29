/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.calcite.utils.binning.BinHandler;
import org.opensearch.sql.calcite.utils.binning.BinHandlerFactory;

/**
 * Simplified facade for bin command operations in Calcite. Delegates to specialized handlers for
 * different bin types.
 */
public class BinUtils {

  /** Extracts the field name from a Bin node. */
  public static String extractFieldName(Bin node) {
    if (node.getField() instanceof Field) {
      Field field = (Field) node.getField();
      return field.getField().toString();
    }
    throw new IllegalArgumentException("Bin field must be a Field expression");
  }

  /** Creates the appropriate bin expression that transforms field values to range strings. */
  public static RexNode createBinExpression(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor rexVisitor) {

    BinHandler handler = BinHandlerFactory.getHandler(node);
    return handler.createExpression(node, fieldExpr, context, rexVisitor);
  }
}
