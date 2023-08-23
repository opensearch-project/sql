/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.ranking;

import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.window.frame.CurrentRowWindowFrame;

/**
 * Row number window function that assigns row number starting from 1 to each row in a partition.
 */
public class RowNumberFunction extends RankingWindowFunction {

  public RowNumberFunction() {
    super(BuiltinFunctionName.ROW_NUMBER.getName());
  }

  @Override
  protected int rank(CurrentRowWindowFrame frame) {
    if (frame.isNewPartition()) {
      rank = 1;
    }
    return rank++;
  }
}
