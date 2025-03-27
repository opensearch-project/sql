/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.sql.calcite.remote.fallback.CalciteNowLikeFunctionIT;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static com.carrotsearch.randomizedtesting.RandomizedTest.$$;
import static org.opensearch.sql.sql.NowLikeFunctionIT.utcDateTimeNow;

//@Ignore("https://github.com/opensearch-project/sql/issues/3400")
public class NonFallbackCalciteNowLikeFunctionIT extends CalciteNowLikeFunctionIT {
  public NonFallbackCalciteNowLikeFunctionIT(
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
    disallowCalciteFallback();
  }

  @ParametersFactory(argumentFormatting = "%1$s")
  public static Iterable<Object[]> compareTwoDates() {
    return Arrays.asList(
            $$(
                    $(
                            "utc_time",
                            false,
                            false,
                            true,
                            (Supplier<Temporal>) (() -> utcDateTimeNow().toLocalTime()),
                            (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalTime::parse,
                            "HH:mm:ss")));
  }


  @Override
  public void testNowLikeFunctions() throws IOException {
    super.testNowLikeFunctions();
  }

}
