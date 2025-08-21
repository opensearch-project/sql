/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning;

import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.CountBin;
import org.opensearch.sql.ast.tree.DefaultBin;
import org.opensearch.sql.ast.tree.MinSpanBin;
import org.opensearch.sql.ast.tree.RangeBin;
import org.opensearch.sql.ast.tree.SpanBin;
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
    if (node instanceof SpanBin) {
      return SPAN_HANDLER;
    } else if (node instanceof MinSpanBin) {
      return MIN_SPAN_HANDLER;
    } else if (node instanceof CountBin) {
      return COUNT_HANDLER;
    } else if (node instanceof RangeBin) {
      return RANGE_HANDLER;
    } else if (node instanceof DefaultBin) {
      return DEFAULT_HANDLER;
    } else {
      throw new IllegalArgumentException("Unknown bin type: " + node.getClass().getSimpleName());
    }
  }
}
