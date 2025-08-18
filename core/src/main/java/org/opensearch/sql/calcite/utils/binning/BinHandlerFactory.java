/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.calcite.utils.binning.handlers.*;

/** Factory for creating appropriate bin handlers based on bin type. */
public class BinHandlerFactory {

  private static final SpanBinHandler SPAN_HANDLER = new SpanBinHandler();
  private static final MinSpanBinHandler MIN_SPAN_HANDLER = new MinSpanBinHandler();
  private static final CountBinHandler COUNT_HANDLER = new CountBinHandler();
  private static final RangeBinHandler RANGE_HANDLER = new RangeBinHandler();
  private static final DefaultBinHandler DEFAULT_HANDLER = new DefaultBinHandler();

  /** Gets the appropriate handler for the given bin node. */
  public static BinHandler getHandler(Bin node) {
    switch (node.getBinType()) {
      case SPAN:
        return SPAN_HANDLER;
      case MIN_SPAN:
        return MIN_SPAN_HANDLER;
      case COUNT:
        return COUNT_HANDLER;
      case RANGE:
        return RANGE_HANDLER;
      case DEFAULT:
        return DEFAULT_HANDLER;
      default:
        throw new IllegalArgumentException("Unknown bin type: " + node.getBinType());
    }
  }
}
