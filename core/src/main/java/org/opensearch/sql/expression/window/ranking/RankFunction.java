/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.ranking;

import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.window.frame.CurrentRowWindowFrame;

/**
 * Rank window function that assigns a rank number to each row based on sort items defined in window
 * definition. Use same rank number if sort item values same on previous and current row.
 */
public class RankFunction extends RankingWindowFunction {

  /** Total number of rows have seen in current partition. */
  private int total;

  public RankFunction() {
    super(BuiltinFunctionName.RANK.getName());
  }

  @Override
  protected int rank(CurrentRowWindowFrame frame) {
    if (frame.isNewPartition()) {
      total = 1;
      rank = 1;
    } else {
      total++;
      if (isSortFieldValueDifferent(frame)) {
        rank = total;
      }
    }
    return rank;
  }
}
