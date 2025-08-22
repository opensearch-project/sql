/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;

/** Interface for handling different types of bin operations. */
public interface BinHandler {

  /**
   * Creates a bin expression for the given node.
   *
   * @param node The bin node to process
   * @param fieldExpr The field expression to bin
   * @param context The Calcite plan context
   * @param visitor The visitor for converting expressions
   * @return The resulting bin expression
   */
  RexNode createExpression(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor);
}
