/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

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
  CalciteExplainIT.class,
  CalciteAddTotalsCommandIT.class,
  CalciteAddColTotalsCommandIT.class,
  CalciteArrayFunctionIT.class,
  CalciteBinCommandIT.class,
  CalciteConvertTZFunctionIT.class,
  CalciteCsvFormatIT.class,
  CalciteDataTypeIT.class,
  CalciteDateTimeComparisonIT.class,
  CalciteDateTimeFunctionIT.class,
  CalciteDateTimeImplementationIT.class,
  CalciteDedupCommandIT.class,
  CalciteDescribeCommandIT.class,
  CalciteExpandCommandIT.class,
  CalciteFieldFormatCommandIT.class,
  CalciteFieldsCommandIT.class,
  CalciteFillNullCommandIT.class,
  CalciteFlattenCommandIT.class,
  CalciteFlattenDocValueIT.class,
  CalciteGeoIpFunctionsIT.class,
  CalciteGeoPointFormatsIT.class,
  CalciteHeadCommandIT.class,
  CalciteInformationSchemaCommandIT.class,
  CalciteIPComparisonIT.class,
  CalciteIPFunctionsIT.class,
  CalciteJsonFunctionsIT.class,
  CalciteLegacyAPICompatibilityIT.class,
  CalciteLikeQueryIT.class,
  CalciteMathematicalFunctionIT.class,
  CalciteMultisearchCommandIT.class,
  CalciteMultiValueStatsIT.class,
  CalciteNewAddedCommandsIT.class,
  CalciteNowLikeFunctionIT.class,
  CalciteObjectFieldOperateIT.class,
  CalciteOperatorIT.class,
  CalciteParseCommandIT.class,
  CalcitePPLAggregationIT.class,
  CalcitePPLAppendcolIT.class,
  CalcitePPLAppendCommandIT.class,
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
  CalciteStreamstatsCommandIT.class,
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
  CalcitePPLSpathCommandIT.class,
  CalcitePPLStringBuiltinFunctionIT.class,
  CalcitePPLTrendlineIT.class,
  CalcitePrometheusDataSourceCommandsIT.class,
  CalciteQueryAnalysisIT.class,
  CalciteRareCommandIT.class,
  CalciteRegexCommandIT.class,
  CalciteRexCommandIT.class,
  CalciteRenameCommandIT.class,
  CalciteReplaceCommandIT.class,
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
  CalciteTransposeCommandIT.class,
  CalciteVisualizationFormatIT.class,
  CalciteWhereCommandIT.class,
  CalcitePPLTpchIT.class,
  CalciteFieldFormatCommandIT.class
  CalcitePPLTpchIT.class,
  CalciteMvCombineCommandIT.class
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
