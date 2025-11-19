/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.common.utils.JsonUtils;

public class CalciteDynamicFieldsTimechartIT extends CalcitePPLPermissiveIntegTestCase {

  private static final String TEST_INDEX_DYNAMIC = "test_dynamic_fields";

  @Override
  public void init() throws IOException {
    super.init();
    createTestIndexWithUnmappedFields();
    enableCalcite();
  }

  @Test
  public void testTimechartByDynamicField() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "timechart span=1d count() by event");

    JSONObject result = executeQuery(query);

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], fetch=[200],"
            + " type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "        LogicalAggregate(group=[{0, 1}], count=[SUM($2)])\n"
            + "          LogicalUnion(all=[false])\n"
            + "            LogicalAggregate(group=[{0, 1}], actual_count=[SUM($2)])\n"
            + "              LogicalProject(@timestamp=[CAST($0):TIMESTAMP(0) NOT NULL],"
            + " event=[CASE(IS NOT NULL($3), $1, CASE(IS NULL($1), null:NULL, 'OTHER'))],"
            + " count=[$2])\n"
            + "                LogicalJoin(condition=[IS NOT DISTINCT FROM($1, $3)],"
            + " joinType=[left])\n"
            + "                  LogicalProject(@timestamp=[$1], event=[$0], $f2_0=[$2])\n"
            + "                    LogicalAggregate(group=[{0, 1}], agg#0=[COUNT()])\n"
            + "                      LogicalProject(event=[SAFE_CAST(ITEM($8, 'event'))],"
            + " $f2=[SPAN(SAFE_CAST(ITEM($8, '@timestamp')), 1, 'd')])\n"
            + "                        CalciteLogicalIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n"
            + "                  LogicalSort(sort0=[$1], dir0=[DESC], fetch=[10])\n"
            + "                    LogicalAggregate(group=[{1}], grand_total=[SUM($2)])\n"
            + "                      LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "                        LogicalProject(@timestamp=[$1], event=[$0], $f2_0=[$2])\n"
            + "                          LogicalAggregate(group=[{0, 1}], agg#0=[COUNT()])\n"
            + "                            LogicalProject(event=[SAFE_CAST(ITEM($8, 'event'))],"
            + " $f2=[SPAN(SAFE_CAST(ITEM($8, '@timestamp')), 1, 'd')])\n"
            + "                              CalciteLogicalIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n"
            + "            LogicalProject(@timestamp=[CAST($0):TIMESTAMP(0) NOT NULL], event=[$1],"
            + " count=[0])\n"
            + "              LogicalJoin(condition=[true], joinType=[inner])\n"
            + "                LogicalAggregate(group=[{0}])\n"
            + "                  LogicalProject(@timestamp=[$1])\n"
            + "                    LogicalAggregate(group=[{0, 1}], agg#0=[COUNT()])\n"
            + "                      LogicalProject(event=[SAFE_CAST(ITEM($8, 'event'))],"
            + " $f2=[SPAN(SAFE_CAST(ITEM($8, '@timestamp')), 1, 'd')])\n"
            + "                        CalciteLogicalIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n"
            + "                LogicalAggregate(group=[{0}])\n"
            + "                  LogicalProject($f0=[CASE(IS NOT NULL($3), $1, CASE(IS NULL($1),"
            + " null:NULL, 'OTHER'))])\n"
            + "                    LogicalJoin(condition=[IS NOT DISTINCT FROM($1, $3)],"
            + " joinType=[left])\n"
            + "                      LogicalProject(@timestamp=[$1], event=[$0], $f2_0=[$2])\n"
            + "                        LogicalAggregate(group=[{0, 1}], agg#0=[COUNT()])\n"
            + "                          LogicalProject(event=[SAFE_CAST(ITEM($8, 'event'))],"
            + " $f2=[SPAN(SAFE_CAST(ITEM($8, '@timestamp')), 1, 'd')])\n"
            + "                            CalciteLogicalIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n"
            + "                      LogicalSort(sort0=[$1], dir0=[DESC], fetch=[10])\n"
            + "                        LogicalAggregate(group=[{1}], grand_total=[SUM($2)])\n"
            + "                          LogicalFilter(condition=[IS NOT NULL($1)])\n"
            + "                            LogicalProject(@timestamp=[$1], event=[$0],"
            + " $f2_0=[$2])\n"
            + "                              LogicalAggregate(group=[{0, 1}], agg#0=[COUNT()])\n"
            + "                                LogicalProject(event=[SAFE_CAST(ITEM($8, 'event'))],"
            + " $f2=[SPAN(SAFE_CAST(ITEM($8, '@timestamp')), 1, 'd')])\n"
            + "                                  CalciteLogicalIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "        EnumerableAggregate(group=[{0, 1}], count=[$SUM0($2)])\n"
            + "          EnumerableUnion(all=[false])\n"
            + "            EnumerableAggregate(group=[{0, 1}], actual_count=[$SUM0($2)])\n"
            + "              EnumerableCalc(expr#0..4=[{inputs}], expr#5=[CAST($t0):TIMESTAMP(0)"
            + " NOT NULL], expr#6=[IS NOT NULL($t3)], expr#7=[IS NULL($t1)], expr#8=[null:NULL],"
            + " expr#9=['OTHER'], expr#10=[CASE($t7, $t8, $t9)], expr#11=[CASE($t6, $t1, $t10)],"
            + " @timestamp=[$t5], event=[$t11], count=[$t2])\n"
            + "                EnumerableMergeJoin(condition=[=($1, $3)], joinType=[left])\n"
            + "                  EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "                    EnumerableCalc(expr#0..2=[{inputs}], @timestamp=[$t1],"
            + " event=[$t0], $f2_0=[$t2])\n"
            + "                      EnumerableAggregate(group=[{0, 1}], agg#0=[COUNT()])\n"
            + "                        EnumerableCalc(expr#0..8=[{inputs}], expr#9=['event'],"
            + " expr#10=[ITEM($t8, $t9)], expr#11=[SAFE_CAST($t10)], expr#12=['@timestamp'],"
            + " expr#13=[ITEM($t8, $t12)], expr#14=[SAFE_CAST($t13)], expr#15=[1], expr#16=['d'],"
            + " expr#17=[SPAN($t14, $t15, $t16)], event=[$t11], $f2=[$t17])\n"
            + "                          CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n"
            + "                  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "                    EnumerableLimit(fetch=[10])\n"
            + "                      EnumerableSort(sort0=[$1], dir0=[DESC])\n"
            + "                        EnumerableAggregate(group=[{0}], grand_total=[$SUM0($2)])\n"
            + "                          EnumerableCalc(expr#0..2=[{inputs}], expr#3=[IS NOT"
            + " NULL($t0)], proj#0..2=[{exprs}], $condition=[$t3])\n"
            + "                            EnumerableAggregate(group=[{0, 1}], agg#0=[COUNT()])\n"
            + "                              EnumerableCalc(expr#0..8=[{inputs}], expr#9=['event'],"
            + " expr#10=[ITEM($t8, $t9)], expr#11=[SAFE_CAST($t10)], expr#12=['@timestamp'],"
            + " expr#13=[ITEM($t8, $t12)], expr#14=[SAFE_CAST($t13)], expr#15=[1], expr#16=['d'],"
            + " expr#17=[SPAN($t14, $t15, $t16)], event=[$t11], $f2=[$t17])\n"
            + "                                CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n"
            + "            EnumerableCalc(expr#0..1=[{inputs}], expr#2=[CAST($t0):TIMESTAMP(0) NOT"
            + " NULL], expr#3=[0], @timestamp=[$t2], event=[$t1], count=[$t3])\n"
            + "              EnumerableNestedLoopJoin(condition=[true], joinType=[inner])\n"
            + "                EnumerableAggregate(group=[{1}])\n"
            + "                  EnumerableCalc(expr#0..8=[{inputs}], expr#9=['event'],"
            + " expr#10=[ITEM($t8, $t9)], expr#11=[SAFE_CAST($t10)], expr#12=['@timestamp'],"
            + " expr#13=[ITEM($t8, $t12)], expr#14=[SAFE_CAST($t13)], expr#15=[1], expr#16=['d'],"
            + " expr#17=[SPAN($t14, $t15, $t16)], event=[$t11], $f2=[$t17])\n"
            + "                    CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n"
            + "                EnumerableAggregate(group=[{0}])\n"
            + "                  EnumerableCalc(expr#0..2=[{inputs}], expr#3=[IS NOT NULL($t1)],"
            + " expr#4=[IS NULL($t0)], expr#5=[null:NULL], expr#6=['OTHER'], expr#7=[CASE($t4, $t5,"
            + " $t6)], expr#8=[CASE($t3, $t0, $t7)], $f0=[$t8])\n"
            + "                    EnumerableMergeJoin(condition=[=($0, $1)], joinType=[left])\n"
            + "                      EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "                        EnumerableCalc(expr#0..1=[{inputs}], event=[$t0])\n"
            + "                          EnumerableAggregate(group=[{0, 1}])\n"
            + "                            EnumerableCalc(expr#0..8=[{inputs}], expr#9=['event'],"
            + " expr#10=[ITEM($t8, $t9)], expr#11=[SAFE_CAST($t10)], expr#12=['@timestamp'],"
            + " expr#13=[ITEM($t8, $t12)], expr#14=[SAFE_CAST($t13)], expr#15=[1], expr#16=['d'],"
            + " expr#17=[SPAN($t14, $t15, $t16)], event=[$t11], $f2=[$t17])\n"
            + "                              CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n"
            + "                      EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "                        EnumerableLimit(fetch=[10])\n"
            + "                          EnumerableSort(sort0=[$1], dir0=[DESC])\n"
            + "                            EnumerableAggregate(group=[{0}],"
            + " grand_total=[$SUM0($2)])\n"
            + "                              EnumerableCalc(expr#0..2=[{inputs}], expr#3=[IS NOT"
            + " NULL($t0)], proj#0..2=[{exprs}], $condition=[$t3])\n"
            + "                                EnumerableAggregate(group=[{0, 1}],"
            + " agg#0=[COUNT()])\n"
            + "                                  EnumerableCalc(expr#0..8=[{inputs}],"
            + " expr#9=['event'], expr#10=[ITEM($t8, $t9)], expr#11=[SAFE_CAST($t10)],"
            + " expr#12=['@timestamp'], expr#13=[ITEM($t8, $t12)], expr#14=[SAFE_CAST($t13)],"
            + " expr#15=[1], expr#16=['d'], expr#17=[SPAN($t14, $t15, $t16)], event=[$t11],"
            + " $f2=[$t17])\n"
            + "                                    CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("event", "string"),
        schema("count", "bigint"));
    verifyDataRows(
        result,
        rows("2025-10-25 00:00:00", "get", 3),
        rows("2025-10-25 00:00:00", "post", 1),
        rows("2025-10-25 00:00:00", "put", 1));
  }

  @Test
  public void testTimechartWithDynamicField() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "timechart span=1d avg(latency)");

    JSONObject result = executeQuery(query);

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(sort0=[$0], dir0=[ASC], fetch=[200],"
            + " type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "        LogicalProject(@timestamp=[$0], avg(latency)=[$1])\n"
            + "          LogicalAggregate(group=[{0}], avg(latency)=[AVG($1)])\n"
            + "            LogicalProject(timestamp=[SPAN(SAFE_CAST(ITEM($8, '@timestamp')), 1,"
            + " 'd')], $f3=[SAFE_CAST(ITEM($8, 'latency'))])\n"
            + "              CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[=($t2, $t3)],"
            + " expr#5=[null:BIGINT], expr#6=[CASE($t4, $t5, $t1)], expr#7=[CAST($t6):DOUBLE],"
            + " expr#8=[/($t7, $t2)], timestamp=[$t0], avg(latency)=[$t8])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "          EnumerableAggregate(group=[{0}], agg#0=[$SUM0($1)], agg#1=[COUNT($1)])\n"
            + "            EnumerableCalc(expr#0..8=[{inputs}], expr#9=['@timestamp'],"
            + " expr#10=[ITEM($t8, $t9)], expr#11=[SAFE_CAST($t10)], expr#12=[1], expr#13=['d'],"
            + " expr#14=[SPAN($t11, $t12, $t13)], expr#15=['latency'], expr#16=[ITEM($t8, $t15)],"
            + " expr#17=[SAFE_CAST($t16)], timestamp=[$t14], $f3=[$t17])\n"
            + "              CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n");

    verifySchema(result, schema("@timestamp", "string"), schema("avg(latency)", "double"));
    verifyDataRows(result, rows("2025-10-25 00:00:00", 62));
  }

  @Test
  public void testTrendlineWithDynamicField() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "trendline sma(2, latency) as latency_trend| fields id, latency, latency_trend");
    JSONObject result = executeQuery(query);

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(id=[$1], latency=[$9], latency_trend=[CASE(>(COUNT() OVER (ROWS"
            + " 1 PRECEDING), 1), /(SUM(SAFE_CAST($9)) OVER (ROWS 1 PRECEDING), CAST(COUNT($9) OVER"
            + " (ROWS 1 PRECEDING)):DOUBLE NOT NULL), null:NULL)])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($9)])\n"
            + "          LogicalProject(name=[$0], id=[$1], _id=[$2], _index=[$3], _score=[$4],"
            + " _maxscore=[$5], _sort=[$6], _routing=[$7], _MAP=[$8], latency=[ITEM($8,"
            + " 'latency')])\n"
            + "            CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[>($t3, $t6)],"
            + " expr#8=[CAST($t5):DOUBLE NOT NULL], expr#9=[/($t4, $t8)], expr#10=[null:NULL],"
            + " expr#11=[CASE($t7, $t9, $t10)], proj#0..1=[{exprs}], latency_trend=[$t11])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableWindow(window#0=[window(rows between $3 PRECEDING and CURRENT ROW"
            + " aggs [COUNT(), $SUM0($2), COUNT($1)])])\n"
            + "          EnumerableCalc(expr#0..8=[{inputs}], expr#9=['latency'],"
            + " expr#10=[ITEM($t8, $t9)], expr#11=[SAFE_CAST($t10)], expr#12=[IS NOT NULL($t10)],"
            + " id=[$t1], latency=[$t10], $2=[$t11], $condition=[$t12])\n"
            + "            CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("latency", "int"),
        schema("latency_trend", "double"));
    verifyDataRows(
        result,
        rows(1, 10, null),
        rows(2, 20, 15),
        rows(3, 40, 30),
        rows(4, 80, 60),
        rows(5, 160, 120));
  }

  @Test
  public void testTrendlineSortByDynamicField() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "trendline sort event sma(2, latency) as latency_trend "
                + "| fields id, latency, latency_trend");
    JSONObject result = executeQuery(query);

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(id=[$1], latency=[$10], latency_trend=[CASE(>(COUNT() OVER"
            + " (ROWS 1 PRECEDING), 1), /(SUM(SAFE_CAST($10)) OVER (ROWS 1 PRECEDING),"
            + " CAST(COUNT($10) OVER (ROWS 1 PRECEDING)):DOUBLE NOT NULL), null:NULL)])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($10)])\n"
            + "          LogicalProject(name=[$0], id=[$1], _id=[$2], _index=[$3], _score=[$4],"
            + " _maxscore=[$5], _sort=[$6], _routing=[$7], _MAP=[$8], event=[$9], latency=[ITEM($8,"
            + " 'latency')])\n"
            + "            LogicalSort(sort0=[$9], dir0=[ASC])\n"
            + "              LogicalProject(name=[$0], id=[$1], _id=[$2], _index=[$3], _score=[$4],"
            + " _maxscore=[$5], _sort=[$6], _routing=[$7], _MAP=[$8], event=[SAFE_CAST(ITEM($8,"
            + " 'event'))])\n"
            + "                CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..5=[{inputs}], expr#6=[1], expr#7=[>($t3, $t6)],"
            + " expr#8=[CAST($t5):DOUBLE NOT NULL], expr#9=[/($t4, $t8)], expr#10=[null:NULL],"
            + " expr#11=[CASE($t7, $t9, $t10)], proj#0..1=[{exprs}], latency_trend=[$t11])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableWindow(window#0=[window(rows between $3 PRECEDING and CURRENT ROW"
            + " aggs [COUNT(), $SUM0($2), COUNT($1)])])\n"
            + "          EnumerableCalc(expr#0..2=[{inputs}], expr#3=['latency'], expr#4=[ITEM($t1,"
            + " $t3)], expr#5=[SAFE_CAST($t4)], expr#6=[IS NOT NULL($t4)], id=[$t0], latency=[$t4],"
            + " $2=[$t5], $condition=[$t6])\n"
            + "            EnumerableSort(sort0=[$2], dir0=[ASC])\n"
            + "              EnumerableCalc(expr#0..8=[{inputs}], expr#9=['event'],"
            + " expr#10=[ITEM($t8, $t9)], expr#11=[SAFE_CAST($t10)], id=[$t1], _MAP=[$t8],"
            + " event=[$t11])\n"
            + "                CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n");

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("latency", "int"),
        schema("latency_trend", "double"));
    verifyDataRows(
        result,
        rows(1, 10, null),
        rows(3, 40, 25),
        rows(5, 160, 100),
        rows(2, 20, 90),
        rows(4, 80, 50));
  }

  private void createTestIndexWithUnmappedFields() throws IOException {
    if (isIndexExist(client(), TEST_INDEX_DYNAMIC)) {
      return;
    }

    String mapping =
        JsonUtils.sjson(
            "{",
            "'mappings': {",
            // Disable dynamic mapping - extra fields won't be indexed but will be stored
            "  'dynamic': false,",
            "  'properties': {",
            "    'id': {'type': 'long'},",
            "    'name': {'type': 'text'}",
            "  }",
            "}",
            "}");

    createIndexByRestClient(client(), TEST_INDEX_DYNAMIC, mapping);

    String bulkData =
        JsonUtils.sjson(
            "{'index': {'_id':'1'}}",
            "{'id':1,'event':'get','latency':10,'@timestamp':'2025-10-25T11:12:13.123Z'}",
            "{'index':{'_id':'2'}}",
            "{'id':2,'event':'post','latency':20,'@timestamp':'2025-10-25T12:12:13.123Z'}",
            "{'index':{'_id':'3'}}",
            "{'id':3,'event':'get','latency':40,'@timestamp':'2025-10-25T13:12:13.123Z'}",
            "{'index':{'_id':'4'}}",
            "{'id':4,'event':'put','latency':80,'@timestamp':'2025-10-25T14:12:13.123Z'}",
            "{'index':{'_id':'5'}}",
            "{'id':5,'event':'get','latency':160,'@timestamp':'2025-10-25T15:12:13.123Z'}");

    Request request = new Request("POST", "/" + TEST_INDEX_DYNAMIC + "/_bulk?refresh=true");
    request.setJsonEntity(bulkData);
    client().performRequest(request);
  }
}
