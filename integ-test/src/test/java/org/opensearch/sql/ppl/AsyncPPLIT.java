/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * Samples a handful of Calcite PPL IT classes under {@code org.opensearch.sql.calcite.remote} and
 * re-runs them under the async submit path so their existing assertions serve as behavioral
 * coverage for the async pipeline (issue #5765).
 *
 * <p>Each run picks {@link #SAMPLE_SIZE} classes at random from the eligible pool, using the
 * OpenSearch gradle {@code tests.seed} for reproducibility. A failing selection is replayable by
 * passing the same seed on the next invocation.
 *
 * <p>The suite class self-manages the {@code test.ppl.wait_for_completion_timeout} system property
 * read by {@link PPLIntegTestCase#buildRequest}: {@link #enableAsync()} fires once before the
 * sampled classes run, {@link #disableAsync()} clears it so no other test observes async mode.
 * Running the suite is cheap because the main integTest already executes the sampled classes once
 * (sync) — only the selected five are re-executed under async.
 *
 * <p>Excluded from the pool (would trip the sync-only gate in {@code TransportPPLQueryAction} and
 * return 400): explain / analyze endpoints, non-JDBC formats (csv, yaml, viz), analytics-engine
 * indices. Also excluded: cross-cluster, standalone, and tracing suites.
 */
@RunWith(AsyncPPLIT.AsyncRandomSuite.class)
public class AsyncPPLIT {

  /** Classes eligible for async re-run; must stay in sync with the exclusion rationale above. */
  private static final List<Class<?>> POOL =
      Arrays.asList(
          org.opensearch.sql.calcite.remote.CalciteAddColTotalsCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteAddTotalsCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteAliasFieldAggregationIT.class,
          org.opensearch.sql.calcite.remote.CalciteArrayFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteBinChartNullIT.class,
          org.opensearch.sql.calcite.remote.CalciteBinCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteChartCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteConvertCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteConvertTZFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteDataTypeIT.class,
          org.opensearch.sql.calcite.remote.CalciteDateTimeComparisonIT.class,
          org.opensearch.sql.calcite.remote.CalciteDateTimeFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteDateTimeImplementationIT.class,
          org.opensearch.sql.calcite.remote.CalciteDedupCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteDescribeCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteErrorReportStageIT.class,
          org.opensearch.sql.calcite.remote.CalciteEvalCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteExpandCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteFieldFormatCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteFieldsCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteFillNullCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteFlattenCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteFlattenDocValueIT.class,
          org.opensearch.sql.calcite.remote.CalciteForeachCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteGeoIpFunctionsIT.class,
          org.opensearch.sql.calcite.remote.CalciteGeoPointFormatsIT.class,
          org.opensearch.sql.calcite.remote.CalciteHeadCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteHighlightIT.class,
          org.opensearch.sql.calcite.remote.CalciteIPComparisonIT.class,
          org.opensearch.sql.calcite.remote.CalciteIPFunctionsIT.class,
          org.opensearch.sql.calcite.remote.CalciteIncludeMetadataIT.class,
          org.opensearch.sql.calcite.remote.CalciteLegacyAPICompatibilityIT.class,
          org.opensearch.sql.calcite.remote.CalciteLikeQueryIT.class,
          org.opensearch.sql.calcite.remote.CalciteMVAppendFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteMatchBoolPrefixIT.class,
          org.opensearch.sql.calcite.remote.CalciteMatchIT.class,
          org.opensearch.sql.calcite.remote.CalciteMatchPhraseIT.class,
          org.opensearch.sql.calcite.remote.CalciteMatchPhrasePrefixIT.class,
          org.opensearch.sql.calcite.remote.CalciteMathematicalFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteMixedFieldTypeIT.class,
          org.opensearch.sql.calcite.remote.CalciteMultiMatchIT.class,
          org.opensearch.sql.calcite.remote.CalciteMultiValueStatsIT.class,
          org.opensearch.sql.calcite.remote.CalciteMultisearchCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteMvCombineCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteMvExpandCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteNewAddedCommandsIT.class,
          org.opensearch.sql.calcite.remote.CalciteNoMvCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteNotInNullFilterIT.class,
          org.opensearch.sql.calcite.remote.CalciteNotLikeNullIT.class,
          org.opensearch.sql.calcite.remote.CalciteNowLikeFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteObjectFieldOperateIT.class,
          org.opensearch.sql.calcite.remote.CalciteOperatorIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLAggregationIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLAggregationPaginatingIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLAppendCommandIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLAppendPipeCommandIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLAppendcolIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLBasicIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLBuiltinDatetimeFunctionInvalidIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLBuiltinFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLBuiltinFunctionsNullIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLCaseFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLCastFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLConditionBuiltinFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLCryptographicFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLDashboardPatternsIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLDedupIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLEnhancedCoalesceIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLEvalMaxMinFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLEventstatsIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLExistsSubqueryIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLFillnullIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLGraphLookupIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLGrokIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLIPFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLInSubqueryIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLJoinIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLJsonBuiltinFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLLookupIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLMapPathIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLNestedAggregationIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLParseIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLPatternsIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLPluginIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLRenameIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLRestIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLScalarSubqueryIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLSortIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLSpathCollisionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLSpathCommandIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLStringBuiltinFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalcitePPLTrendlineIT.class,
          org.opensearch.sql.calcite.remote.CalciteParseCommandIT.class,
          org.opensearch.sql.calcite.remote.CalcitePartialFilterPushdownIT.class,
          org.opensearch.sql.calcite.remote.CalcitePlannerConcurrencyIT.class,
          org.opensearch.sql.calcite.remote.CalciteQueryAnalysisIT.class,
          org.opensearch.sql.calcite.remote.CalciteQueryStringIT.class,
          org.opensearch.sql.calcite.remote.CalciteRareCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteRegexCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteRelevanceFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteRenameCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteReplaceCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteResourceMonitorIT.class,
          org.opensearch.sql.calcite.remote.CalciteReverseCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteRexCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteSearchCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteSettingsIT.class,
          org.opensearch.sql.calcite.remote.CalciteSimpleQueryStringIT.class,
          org.opensearch.sql.calcite.remote.CalciteSortCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteStatsCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteStreamstatsCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteSystemFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteTextFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteTimeBoundsPruningIT.class,
          org.opensearch.sql.calcite.remote.CalciteTimechartCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteTimechartPerFunctionIT.class,
          org.opensearch.sql.calcite.remote.CalciteTimewrapCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteTopCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteTransposeCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteTrendlineCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteUnionCommandIT.class,
          org.opensearch.sql.calcite.remote.CalciteWhereCommandIT.class);

  /** How many classes to re-run per invocation. */
  static final int SAMPLE_SIZE = 5;

  @BeforeClass
  public static void enableAsync() {
    System.setProperty("test.ppl.wait_for_completion_timeout", "60s");
  }

  @AfterClass
  public static void disableAsync() {
    System.clearProperty("test.ppl.wait_for_completion_timeout");
  }

  /** JUnit runner that materializes the suite children dynamically from a seeded shuffle. */
  public static final class AsyncRandomSuite extends Suite {
    public AsyncRandomSuite(Class<?> klass, RunnerBuilder builder) throws InitializationError {
      // Pass klass through so Suite.getTestClass() resolves to AsyncPPLIT; otherwise JUnit skips
      // the @BeforeClass / @AfterClass hooks on this class and the async property is never set.
      super(builder, klass, pick(SAMPLE_SIZE));
    }

    private static Class<?>[] pick(int n) {
      long seed = resolveSeed();
      List<Class<?>> copy = new ArrayList<>(POOL);
      Collections.shuffle(copy, new Random(seed));
      return copy.subList(0, Math.min(n, copy.size())).toArray(Class<?>[]::new);
    }

    /**
     * Resolves the gradle {@code tests.seed} ({@code "HEX64"} or {@code "HEX64:HEX64"}) to a {@code
     * long}. Falls back to {@link System#nanoTime()} when the property is absent or malformed so
     * local runs without gradle still have a seed.
     */
    private static long resolveSeed() {
      String raw = System.getProperty("tests.seed");
      if (raw == null) {
        return System.nanoTime();
      }
      String primary = raw.split(":")[0];
      try {
        return Long.parseUnsignedLong(primary, 16);
      } catch (NumberFormatException e) {
        return System.nanoTime();
      }
    }
  }
}
