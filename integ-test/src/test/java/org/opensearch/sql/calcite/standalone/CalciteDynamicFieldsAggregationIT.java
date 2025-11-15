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

public class CalciteDynamicFieldsAggregationIT extends CalcitePPLPermissiveIntegTestCase {

  private static final String TEST_INDEX_DYNAMIC = "test_dynamic_fields";

  @Override
  public void init() throws IOException {
    super.init();
    createTestIndexWithUnmappedFields();
    enableCalcite();
  }

  @Test
  public void testStatsByStaticField() throws IOException {
    String query =
        source(TEST_INDEX_DYNAMIC, "stats count() as c by name | fields name, c | sort name");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(sort0=[$0], dir0=[ASC-nulls-first], fetch=[200],"
            + " type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(sort0=[$0], dir0=[ASC-nulls-first])\n"
            + "        LogicalAggregate(group=[{0}], c=[COUNT()])\n"
            + "          LogicalProject(name=[$0])\n"
            + "            CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableSort(sort0=[$0], dir0=[ASC-nulls-first])\n"
            + "        EnumerableAggregate(group=[{0}], c=[COUNT()])\n"
            + "          CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("name", "string"), schema("c", "bigint"));
    verifyDataRows(
        result,
        rows("Alice", 1),
        rows("Bob", 1),
        rows("Charlie", 1),
        rows("Jane", 1),
        rows("John", 1));
  }

  @Test
  public void testStatsByDynamicField() throws IOException {
    String query =
        source(TEST_INDEX_DYNAMIC, "stats count() as c by department | fields department, c");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalAggregate(group=[{0}], c=[COUNT()])\n"
            + "        LogicalProject(department=[ITEM($8, 'department')])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableAggregate(group=[{0}], c=[COUNT()])\n"
            + "        EnumerableCalc(expr#0..8=[{inputs}], expr#9=['department'],"
            + " expr#10=[ITEM($t8, $t9)], department=[$t10])\n"
            + "          CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("c", "bigint"));
    verifyDataRows(result, rows("Engineering", 3), rows("Marketing", 1), rows(null, 1));
  }

  @Test
  public void testStatsWithDynamicField() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC, "stats avg(salary) as avg by department | fields department, avg");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalAggregate(group=[{0}], avg=[AVG($1)])\n"
            + "        LogicalProject(department=[ITEM($8, 'department')], $f3=[SAFE_CAST(ITEM($8,"
            + " 'salary'))])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..2=[{inputs}], expr#3=[0], expr#4=[=($t2, $t3)],"
            + " expr#5=[null:BIGINT], expr#6=[CASE($t4, $t5, $t1)], expr#7=[CAST($t6):DOUBLE],"
            + " expr#8=[/($t7, $t2)], department=[$t0], avg=[$t8])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableAggregate(group=[{0}], agg#0=[$SUM0($1)], agg#1=[COUNT($1)])\n"
            + "          EnumerableCalc(expr#0..8=[{inputs}], expr#9=['department'],"
            + " expr#10=[ITEM($t8, $t9)], expr#11=['salary'], expr#12=[ITEM($t8, $t11)],"
            + " expr#13=[SAFE_CAST($t12)], department=[$t10], $f3=[$t13])\n"
            + "            CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("avg", "double"));
    verifyDataRows(result, rows("Engineering", 77500), rows("Marketing", 65000), rows(null, 60000));
  }

  @Test
  public void testEventstatsByDynamicField() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eventstats count() as c by department | fields id, city, department, c");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(id=[$1], city=[ITEM($8, 'city')], department=[ITEM($8,"
            + " 'department')], c=[COUNT() OVER (PARTITION BY ITEM($8, 'department'))])\n"
            + "        CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableWindow(window#0=[window(partition {2} aggs [COUNT()])])\n"
            + "        EnumerableCalc(expr#0..8=[{inputs}], expr#9=['city'], expr#10=[ITEM($t8,"
            + " $t9)], expr#11=['department'], expr#12=[ITEM($t8, $t11)], id=[$t1], $1=[$t10],"
            + " $2=[$t12])\n"
            + "          CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("city", "string"),
        schema("department", "string"),
        schema("c", "bigint"));
    verifyDataRows(
        result,
        rows(1, "NYC", "Engineering", 3),
        rows(2, "LA", "Marketing", 1),
        rows(3, "Chicago", "Engineering", 3),
        rows(4, "NYC", "Engineering", 3),
        rows(5, "Boston", null, 1));
  }

  @Test
  public void testEventstatsWithDynamicField() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eventstats avg(salary) as avg by department " + "| fields id, city, department, avg");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(id=[$1], city=[ITEM($8, 'city')], department=[ITEM($8,"
            + " 'department')], avg=[/(SUM(SAFE_CAST(ITEM($8, 'salary'))) OVER (PARTITION BY"
            + " ITEM($8, 'department')), CAST(COUNT(SAFE_CAST(ITEM($8, 'salary'))) OVER (PARTITION"
            + " BY ITEM($8, 'department'))):DOUBLE NOT NULL)])\n"
            + "        CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..5=[{inputs}], expr#6=[CAST($t5):DOUBLE NOT NULL],"
            + " expr#7=[/($t4, $t6)], proj#0..2=[{exprs}], avg=[$t7])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableWindow(window#0=[window(partition {2} aggs [$SUM0($3),"
            + " COUNT($3)])])\n"
            + "          EnumerableCalc(expr#0..8=[{inputs}], expr#9=['city'], expr#10=[ITEM($t8,"
            + " $t9)], expr#11=['department'], expr#12=[ITEM($t8, $t11)], expr#13=['salary'],"
            + " expr#14=[ITEM($t8, $t13)], expr#15=[SAFE_CAST($t14)], id=[$t1], $1=[$t10],"
            + " $2=[$t12], $3=[$t15])\n"
            + "            CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("id", "bigint"),
        schema("city", "string"),
        schema("department", "string"),
        schema("avg", "double"));
    verifyDataRows(
        result,
        rows(1, "NYC", "Engineering", 77500),
        rows(2, "LA", "Marketing", 65000),
        rows(3, "Chicago", "Engineering", 77500),
        rows(4, "NYC", "Engineering", 77500),
        rows(5, "Boston", null, 60000));
  }

  @Test
  public void testTopDynamicField() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "top 1 department");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(department=[$0], count=[$1])\n"
            + "        LogicalFilter(condition=[<=($2, 1)])\n"
            + "          LogicalProject(department=[$0], count=[$1], _row_number_=[ROW_NUMBER()"
            + " OVER (ORDER BY $1 DESC)])\n"
            + "            LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "              LogicalProject(department=[ITEM($8, 'department')])\n"
            + "                CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..2=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableCalc(expr#0..2=[{inputs}], expr#3=[1], expr#4=[<=($t2, $t3)],"
            + " proj#0..2=[{exprs}], $condition=[$t4])\n"
            + "          EnumerableWindow(window#0=[window(order by [1 DESC] rows between UNBOUNDED"
            + " PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])])\n"
            + "            EnumerableAggregate(group=[{0}], count=[COUNT()])\n"
            + "              EnumerableCalc(expr#0..8=[{inputs}], expr#9=['department'],"
            + " expr#10=[ITEM($t8, $t9)], department=[$t10])\n"
            + "                CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("count", "bigint"));
    verifyDataRows(result, rows("Engineering", 3));
  }

  @Test
  public void testTopByDynamicField() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "top 1 city by department");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(department=[$0], city=[$1], count=[$2])\n"
            + "        LogicalFilter(condition=[<=($3, 1)])\n"
            + "          LogicalProject(department=[$0], city=[$1], count=[$2],"
            + " _row_number_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2 DESC)])\n"
            + "            LogicalAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "              LogicalProject(department=[ITEM($8, 'department')], city=[ITEM($8,"
            + " 'city')])\n"
            + "                CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..2=[{exprs}])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableCalc(expr#0..3=[{inputs}], expr#4=[1], expr#5=[<=($t3, $t4)],"
            + " proj#0..3=[{exprs}], $condition=[$t5])\n"
            + "          EnumerableWindow(window#0=[window(partition {0} order by [2 DESC] rows"
            + " between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])])\n"
            + "            EnumerableAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "              EnumerableCalc(expr#0..8=[{inputs}], expr#9=['department'],"
            + " expr#10=[ITEM($t8, $t9)], expr#11=['city'], expr#12=[ITEM($t8, $t11)],"
            + " department=[$t10], city=[$t12])\n"
            + "                CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("department", "string"),
        schema("city", "string"),
        schema("count", "bigint"));
    verifyDataRows(
        result, rows("Engineering", "NYC", 2), rows("Marketing", "LA", 1), rows(null, "Boston", 1));
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
            "{'name':'John','id':1,'city':'NYC','department':'Engineering','salary':75000}",
            "{'index':{'_id':'2'}}",
            "{'name':'Jane','id':2,'city':'LA','department':'Marketing','salary':65000}",
            "{'index':{'_id':'3'}}",
            "{'name':'Bob','id':3,'city':'Chicago','department':'Engineering'}",
            "{'index':{'_id':'4'}}",
            "{'name':'Alice','id':4,'city':'NYC','department':'Engineering','salary':80000}",
            "{'index':{'_id':'5'}}",
            "{'name':'Charlie','id':5,'city':'Boston','salary':60000}");

    Request request = new Request("POST", "/" + TEST_INDEX_DYNAMIC + "/_bulk?refresh=true");
    request.setJsonEntity(bulkData);
    client().performRequest(request);
  }
}
