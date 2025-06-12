/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.calcite.pushdown;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.opensearch.sql.calcite.remote.*;
import org.opensearch.sql.ppl.PPLIntegTestCase;

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
  CalciteExplainIT.class,
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
  CalciteMatchBoolPrefixIT.class,
  CalciteMatchIT.class,
  CalciteMatchPhraseIT.class,
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
  CalciteQueryStringIT.class,
  CalciteRareCommandIT.class,
  CalciteRelevanceFunctionIT.class,
  CalciteRenameCommandIT.class,
  CalciteResourceMonitorIT.class,
  CalciteSearchCommandIT.class,
  CalciteSettingsIT.class,
  CalciteShowDataSourcesCommandIT.class,
  CalciteSimpleQueryStringIT.class,
  CalciteSortCommandIT.class,
  CalciteStatsCommandIT.class,
  CalciteSystemFunctionIT.class,
  CalciteTextFunctionIT.class,
  CalciteTopCommandIT.class,
  CalciteTrendlineCommandIT.class,
  CalciteVisualizationFormatIT.class,
  CalciteWhereCommandIT.class
})
public class CalciteNoPushdownIT {
  @BeforeClass
  public static void disablePushdown() {
    PPLIntegTestCase.GlobalPushdownConfig.enabled = false;
  }

  @AfterClass
  public static void enablePushdown() {
    PPLIntegTestCase.GlobalPushdownConfig.enabled = true;
  }
}
