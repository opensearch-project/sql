/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.opensearch.sql.ppl.NowLikeFunctionIT;

public class CalciteNowLikeFunctionIT extends NowLikeFunctionIT {
  public CalciteNowLikeFunctionIT(
      String name,
      Boolean hasFsp,
      Boolean hasShortcut,
      Boolean constValue,
      Supplier<Temporal> referenceGetter,
      BiFunction<CharSequence, DateTimeFormatter, Temporal> parser,
      String serializationPatternStr) {
    super(name, hasFsp, hasShortcut, constValue, referenceGetter, parser, serializationPatternStr);
  }

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }
}
