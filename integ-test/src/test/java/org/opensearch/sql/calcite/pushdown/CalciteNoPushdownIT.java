/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.pushdown;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.opensearch.sql.calcite.remote.*;
import org.opensearch.sql.calcite.tpch.CalcitePPLTpchIT;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * This test suite runs all remote Calcite integration tests without pushdown enabled.
 *
 * <p>Individual tests in this suite will be executed independently with pushdown enabled.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  CalciteArrayFunctionIT.class,
  CalciteConvertTZFunctionIT.class,
  CalciteCsvFormatIT.class,
  CalciteDataTypeIT.class,
  CalciteDateTimeComparisonIT.class,
  CalciteDateTimeFunctionIT.class,
  CalciteDateTimeImplementationIT.class,
  CalciteDedupCommandIT.class,
  CalciteDescribeCommandIT.class,
  CalciteExpandCommandIT.class,
  // TODO: Add expected plans for CalciteExplainIT without pushdown
  //  CalciteExplainIT.class,
  CalciteFieldsCommandIT.class,
  CalciteFillNullCommandIT.class,
  CalciteFlattenCommandIT.class,
  CalciteFlattenDocValueIT.class,
  CalciteGeoIpFunctionsIT.class,
  CalciteGeoPointFormatsIT.class,
  CalciteHeadCommandIT.class,
  CalciteInformationSchemaCommandIT.class,
  // TODO: Enable after implementing comparison for IP addresses with Calcite
  //  https://github.com/opensearch-project/sql/issues/3776
  // CalciteIPComparisonIT.class,
  CalciteIPFunctionsIT.class,
  CalciteJsonFunctionsIT.class,
  CalciteLegacyAPICompatibilityIT.class,
  CalciteLikeQueryIT.class,
  CalciteMathematicalFunctionIT.class,
  CalciteNewAddedCommandsIT.class,
  CalciteNowLikeFunctionIT.class,
  CalciteObjectFieldOperateIT.class,
  CalciteOperatorIT.class,
  CalciteParseCommandIT.class,
  CalcitePPLAggregationIT.class,
  CalcitePPLAppendcolIT.class,
  CalcitePPLBasicIT.class,
  CalcitePPLBuiltinDatetimeFunctionInvalidIT.class,
  CalcitePPLBuiltinFunctionIT.class,
  CalcitePPLBuiltinFunctionsNullIT.class,
  CalcitePPLCaseFunctionIT.class,
  CalcitePPLCastFunctionIT.class,
  CalcitePPLConditionBuiltinFunctionIT.class,
  CalcitePPLCryptographicFunctionIT.class,
  CalcitePPLDedupIT.class,
  CalcitePPLEventstatsIT.class,
  CalcitePPLExistsSubqueryIT.class,
  CalcitePPLExplainIT.class,
  CalcitePPLFillnullIT.class,
  CalcitePPLGrokIT.class,
  CalcitePPLInSubqueryIT.class,
  CalcitePPLIPFunctionIT.class,
  CalcitePPLJoinIT.class,
  CalcitePPLJsonBuiltinFunctionIT.class,
  CalcitePPLLookupIT.class,
  CalcitePPLParseIT.class,
  CalcitePPLPatternsIT.class,
  CalcitePPLPluginIT.class,
  CalcitePPLRenameIT.class,
  CalcitePPLScalarSubqueryIT.class,
  CalcitePPLSortIT.class,
  CalcitePPLStringBuiltinFunctionIT.class,
  CalcitePPLTrendlineIT.class,
  CalcitePrometheusDataSourceCommandsIT.class,
  CalciteQueryAnalysisIT.class,
  CalciteRareCommandIT.class,
  CalciteRenameCommandIT.class,
  CalciteResourceMonitorIT.class,
  CalciteSearchCommandIT.class,
  CalciteSettingsIT.class,
  CalciteShowDataSourcesCommandIT.class,
  CalciteSortCommandIT.class,
  CalciteStatsCommandIT.class,
  CalciteSystemFunctionIT.class,
  CalciteTextFunctionIT.class,
  CalciteTopCommandIT.class,
  CalciteTrendlineCommandIT.class,
  CalciteVisualizationFormatIT.class,
  CalciteWhereCommandIT.class,
  CalcitePPLTpchIT.class
})
public class CalciteNoPushdownIT {
  private static boolean wasPushdownEnabled;

  @BeforeClass
  public static void disablePushdown() {
    wasPushdownEnabled = PPLIntegTestCase.GlobalPushdownConfig.enabled;
    PPLIntegTestCase.GlobalPushdownConfig.enabled = false;
  }

  @AfterClass
  public static void restorePushdown() {
    PPLIntegTestCase.GlobalPushdownConfig.enabled = wasPushdownEnabled;
  }
}
