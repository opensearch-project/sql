/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.window.ranking;

import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.window.frame.CurrentRowWindowFrame;

/**
 * Dense rank window function that assigns a rank number to each row similarly as rank function. The
 * difference is there is no gap between rank number assigned.
 */
public class DenseRankFunction extends RankingWindowFunction {

  public DenseRankFunction() {
    super(BuiltinFunctionName.DENSE_RANK.getName());
  }

  @Override
  protected int rank(CurrentRowWindowFrame frame) {
    if (frame.isNewPartition()) {
      rank = 1;
    } else {
      if (isSortFieldValueDifferent(frame)) {
        rank++;
      }
    }
    return rank;
  }
}
