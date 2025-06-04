/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;

@FunctionalInterface
public interface AggHandler {
  RelBuilder.AggCall apply(
      boolean distinct, RexNode field, List<RexNode> argList, CalcitePlanContext context);
}
