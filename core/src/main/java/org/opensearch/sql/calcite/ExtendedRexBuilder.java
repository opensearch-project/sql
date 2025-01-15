/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class ExtendedRexBuilder extends RexBuilder {

  public ExtendedRexBuilder(RexBuilder rexBuilder) {
    super(rexBuilder.getTypeFactory());
  }

  public RexNode coalesce(RexNode... nodes) {
    return this.makeCall(SqlStdOperatorTable.COALESCE, nodes);
  }

  public RexNode equals(RexNode n1, RexNode n2) {
    return this.makeCall(SqlStdOperatorTable.EQUALS, n1, n2);
  }
}
