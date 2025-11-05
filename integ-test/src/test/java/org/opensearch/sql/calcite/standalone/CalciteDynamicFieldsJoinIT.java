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
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

public class CalciteDynamicFieldsJoinIT extends CalcitePPLPermissiveIntegTestCase {

  private static final String TEST_DYNAMIC_LEFT = "test_dynamic_left";
  private static final String TEST_DYNAMIC_RIGHT = "test_dynamic_right";

  @Override
  public void init() throws IOException {
    super.init();
    createLeftIndex();
    createRightIndex();
    enableCalcite();
  }

  @Test
  @Ignore("pending join adaptation")
  public void testExpand() throws IOException {
    String query = source(TEST_DYNAMIC_LEFT, "expand arr as expanded | where isnotnull(expanded)");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("count", "bigint"));
    verifyDataRows(result, rows("Engineering", 2));
  }

  @Test
  public void testJoinWithStaticField() throws IOException {
    String query =
        source(TEST_DYNAMIC_LEFT, "join a2 " + TEST_DYNAMIC_RIGHT + " | fields a1, a2, a3, a4, a5");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(a1=[COALESCE(ITEM($5, 'a1'), $0)], a2=[$1], a3=[$4],"
            + " a4=[ITEM(MAP_CONCAT($2, $5), 'a4')], a5=[ITEM(MAP_CONCAT($2, $5), 'a5')])\n"
            + "        LogicalJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "          LogicalProject(a1=[$0], a2=[$1], _MAP=[$8])\n"
            + "            CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          LogicalSystemLimit(fetch=[50000], type=[JOIN_SUBSEARCH_MAXOUT])\n"
            + "            LogicalProject(a2=[$0], a3=[$1], _MAP=[$8])\n"
            + "              CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_right]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..5=[{inputs}], expr#6=['a1'], expr#7=[ITEM($t5, $t6)],"
            + " expr#8=[COALESCE($t7, $t0)], expr#9=[MAP_CONCAT($t2, $t5)], expr#10=['a4'],"
            + " expr#11=[ITEM($t9, $t10)], expr#12=['a5'], expr#13=[ITEM($t9, $t12)], a1=[$t8],"
            + " a2=[$t1], a3=[$t4], a4=[$t11], a5=[$t13])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableMergeJoin(condition=[=($1, $3)], joinType=[inner])\n"
            + "          EnumerableSort(sort0=[$1], dir0=[ASC])\n"
            + "            EnumerableCalc(expr#0..8=[{inputs}], proj#0..1=[{exprs}], _MAP=[$t8])\n"
            + "              CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "            EnumerableLimit(fetch=[50000])\n"
            + "              EnumerableCalc(expr#0..8=[{inputs}], proj#0..1=[{exprs}],"
            + " _MAP=[$t8])\n"
            + "                CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_right]])\n");

    JSONObject result = executeQuery(query);

    System.out.println(result.toString());

    verifyJoinResult(result);
  }

  @Test
  public void testJoinDynamicWithStaticWithoutCast1() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT,
            "join left = l right = r on l.a3 = r.a3 "
                + TEST_DYNAMIC_RIGHT
                + " | fields a1, a2, a3, a4, a5");
    Throwable th = assertThrows(IllegalArgumentException.class, () -> executeQuery(query));
    assertEquals(
        "Join condition needs to use specific type. Please cast explicitly.", th.getMessage());
  }

  @Test
  public void testJoinDynamicWithStaticWithoutCast2() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT,
            "join left = l right = r on r.a3 = l.a3 "
                + TEST_DYNAMIC_RIGHT
                + " | fields a1, a2, a3, a4, a5");
    Throwable th = assertThrows(IllegalArgumentException.class, () -> executeQuery(query));
    assertEquals(
        "Join condition needs to use specific type. Please cast explicitly.", th.getMessage());
  }

  @Test
  public void testJoinDynamicWithStaticWithoutCast3() throws IOException {
    String query =
        source(TEST_DYNAMIC_LEFT, "join a3 " + TEST_DYNAMIC_RIGHT + " | fields a1, a2, a3, a4, a5");
    Throwable th = assertThrows(IllegalArgumentException.class, () -> executeQuery(query));
    assertEquals(
        "Source key `a3` needs to be specific type. Please cast explicitly.", th.getMessage());
  }

  @Test
  public void testJoinDynamicWithStatic() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT,
            "join left = l right = r on cast(l.a3 as string) = r.a3 "
                + TEST_DYNAMIC_RIGHT
                + " | fields a1, a2, a3, a4, a5");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(a1=[COALESCE(ITEM($5, 'a1'), $0)], a2=[$1], a3=[$4],"
            + " a4=[ITEM(MAP_CONCAT($2, $5), 'a4')], a5=[ITEM(MAP_CONCAT($2, $5), 'a5')])\n"
            + "        LogicalJoin(condition=[=(SAFE_CAST(ITEM($2, 'a3')), $4)],"
            + " joinType=[inner])\n"
            + "          LogicalProject(a1=[$0], a2=[$1], _MAP=[$8])\n"
            + "            CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          LogicalSystemLimit(fetch=[50000], type=[JOIN_SUBSEARCH_MAXOUT])\n"
            + "            LogicalProject(a2=[$0], a3=[$1], _MAP=[$8])\n"
            + "              CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_right]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..5=[{inputs}], expr#6=['a1'], expr#7=[ITEM($t5, $t6)],"
            + " expr#8=[COALESCE($t7, $t0)], expr#9=[MAP_CONCAT($t2, $t5)], expr#10=['a4'],"
            + " expr#11=[ITEM($t9, $t10)], expr#12=['a5'], expr#13=[ITEM($t9, $t12)], a1=[$t8],"
            + " a2=[$t1], a3=[$t4], a4=[$t11], a5=[$t13])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableMergeJoin(condition=[=($3, $4)], joinType=[inner])\n"
            + "          EnumerableSort(sort0=[$3], dir0=[ASC])\n"
            + "            EnumerableCalc(expr#0..8=[{inputs}], expr#9=['a3'], expr#10=[ITEM($t8,"
            + " $t9)], expr#11=[SAFE_CAST($t10)], proj#0..1=[{exprs}], _MAP=[$t8], $f3=[$t11])\n"
            + "              CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "            EnumerableLimit(fetch=[50000])\n"
            + "              EnumerableCalc(expr#0..8=[{inputs}], a3=[$t1], _MAP=[$t8])\n"
            + "                CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_right]])");

    JSONObject result = executeQuery(query);
    verifyJoinResult(result);
  }

  @Test
  public void testJoinStaticWithDynamic() throws IOException {
    String query =
        source(TEST_DYNAMIC_LEFT, "join a1 " + TEST_DYNAMIC_RIGHT + " | fields a1, a2, a3, a4, a5");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(a1=[COALESCE(ITEM($5, 'a1'), $0)], a2=[$1], a3=[$4],"
            + " a4=[ITEM(MAP_CONCAT($2, $5), 'a4')], a5=[ITEM(MAP_CONCAT($2, $5), 'a5')])\n"
            + "        LogicalJoin(condition=[=($0, ITEM($5, 'a1'))], joinType=[inner])\n"
            + "          LogicalProject(a1=[$0], a2=[$1], _MAP=[$8])\n"
            + "            CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          LogicalSystemLimit(fetch=[50000], type=[JOIN_SUBSEARCH_MAXOUT])\n"
            + "            LogicalProject(a2=[$0], a3=[$1], _MAP=[$8])\n"
            + "              CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_right]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..5=[{inputs}], expr#6=['a1'], expr#7=[ITEM($t4, $t6)],"
            + " expr#8=[COALESCE($t7, $t0)], expr#9=[MAP_CONCAT($t2, $t4)], expr#10=['a4'],"
            + " expr#11=[ITEM($t9, $t10)], expr#12=['a5'], expr#13=[ITEM($t9, $t12)], a1=[$t8],"
            + " a2=[$t1], a3=[$t3], a4=[$t11], a5=[$t13])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableMergeJoin(condition=[=($0, $5)], joinType=[inner])\n"
            + "          EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "            EnumerableCalc(expr#0..8=[{inputs}], proj#0..1=[{exprs}], _MAP=[$t8])\n"
            + "              CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          EnumerableSort(sort0=[$2], dir0=[ASC])\n"
            + "            EnumerableCalc(expr#0..8=[{inputs}], expr#9=['a1'], expr#10=[ITEM($t8,"
            + " $t9)], a3=[$t1], _MAP=[$t8], $f2=[$t10])\n"
            + "              EnumerableLimit(fetch=[50000])\n"
            + "                CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_right]])\n");

    JSONObject result = executeQuery(query);
    verifyJoinResult(result);
  }

  @Test
  public void testJoinDynamicWithDynamicWithoutCast1() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT,
            "join left = l right = r on l.a4 = r.a4 "
                + TEST_DYNAMIC_RIGHT
                + " | fields a1, a2, a3, a4, a5");
    Throwable th = assertThrows(IllegalArgumentException.class, () -> executeQuery(query));
    assertEquals(
        "Join condition needs to use specific type. Please cast explicitly.", th.getMessage());
  }

  @Test
  public void testJoinDynamicWithDynamicWithoutCast2() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT,
            "join left = l right = r on r.a4 = l.a4 "
                + TEST_DYNAMIC_RIGHT
                + " | fields a1, a2, a3, a4, a5");
    Throwable th = assertThrows(IllegalArgumentException.class, () -> executeQuery(query));
    assertEquals(
        "Join condition needs to use specific type. Please cast explicitly.", th.getMessage());
  }

  @Test
  public void testJoinDynamicWithDynamicWithoutCast3() throws IOException {
    String query =
        source(TEST_DYNAMIC_LEFT, "join a4 " + TEST_DYNAMIC_RIGHT + " | fields a1, a2, a3, a4, a5");
    Throwable th = assertThrows(IllegalArgumentException.class, () -> executeQuery(query));
    assertEquals(
        "Source key `a4` needs to be specific type. Please cast explicitly.", th.getMessage());
  }

  @Test
  public void testJoinDynamicWithDynamic() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT,
            "join left = l right = r on cast(l.a4 as int) = cast(r.a4 as int) "
                + TEST_DYNAMIC_RIGHT
                + " | fields a1, a2, a3, a4, a5");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(a1=[COALESCE(ITEM($5, 'a1'), $0)], a2=[$1], a3=[$4],"
            + " a4=[ITEM(MAP_CONCAT($2, $5), 'a4')], a5=[ITEM(MAP_CONCAT($2, $5), 'a5')])\n"
            + "        LogicalJoin(condition=[=(SAFE_CAST(ITEM($2, 'a4')), SAFE_CAST(ITEM($5,"
            + " 'a4')))], joinType=[inner])\n"
            + "          LogicalProject(a1=[$0], a2=[$1], _MAP=[$8])\n"
            + "            CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          LogicalSystemLimit(fetch=[50000], type=[JOIN_SUBSEARCH_MAXOUT])\n"
            + "            LogicalProject(a2=[$0], a3=[$1], _MAP=[$8])\n"
            + "              CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_right]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..6=[{inputs}], expr#7=['a1'], expr#8=[ITEM($t5, $t7)],"
            + " expr#9=[COALESCE($t8, $t0)], expr#10=[MAP_CONCAT($t2, $t5)], expr#11=['a4'],"
            + " expr#12=[ITEM($t10, $t11)], expr#13=['a5'], expr#14=[ITEM($t10, $t13)], a1=[$t9],"
            + " a2=[$t1], a3=[$t4], a4=[$t12], a5=[$t14])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableMergeJoin(condition=[=($3, $6)], joinType=[inner])\n"
            + "          EnumerableSort(sort0=[$3], dir0=[ASC])\n"
            + "            EnumerableCalc(expr#0..8=[{inputs}], expr#9=['a4'], expr#10=[ITEM($t8,"
            + " $t9)], expr#11=[SAFE_CAST($t10)], proj#0..1=[{exprs}], _MAP=[$t8], $f3=[$t11])\n"
            + "              CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          EnumerableSort(sort0=[$2], dir0=[ASC])\n"
            + "            EnumerableCalc(expr#0..8=[{inputs}], expr#9=['a4'], expr#10=[ITEM($t8,"
            + " $t9)], expr#11=[SAFE_CAST($t10)], a3=[$t1], _MAP=[$t8], $f2=[$t11])\n"
            + "              EnumerableLimit(fetch=[50000])\n"
            + "                CalciteEnumerableIndexScan(table=[[OpenSearch,"
            + " test_dynamic_right]])\n");

    JSONObject result = executeQuery(query);
    verifyJoinResult(result);
  }

  @Test
  public void testLookupStaticWithDynamic() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT, "lookup " + TEST_DYNAMIC_RIGHT + " a1 | fields a1, a2, a3, a4, a5");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(a1=[COALESCE(ITEM($17, 'a1'), $0)], a2=[$9], a3=[$10],"
            + " a4=[ITEM(MAP_CONCAT($8, $17), 'a4')], a5=[ITEM(MAP_CONCAT($8, $17), 'a5')])\n"
            + "        LogicalJoin(condition=[=($0, ITEM($17, 'a1'))], joinType=[left])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_right]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..5=[{inputs}], expr#6=['a1'], expr#7=[ITEM($t4, $t6)],"
            + " expr#8=[COALESCE($t7, $t0)], expr#9=[MAP_CONCAT($t1, $t4)], expr#10=['a4'],"
            + " expr#11=[ITEM($t9, $t10)], expr#12=['a5'], expr#13=[ITEM($t9, $t12)], a1=[$t8],"
            + " a2=[$t2], a3=[$t3], a4=[$t11], a5=[$t13])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableHashJoin(condition=[=($0, $5)], joinType=[left])\n"
            + "          EnumerableCalc(expr#0..8=[{inputs}], a1=[$t0], _MAP=[$t8])\n"
            + "            EnumerableLimit(fetch=[200])\n"
            + "              CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          EnumerableCalc(expr#0..8=[{inputs}], expr#9=['a1'], expr#10=[ITEM($t8,"
            + " $t9)], proj#0..1=[{exprs}], _MAP=[$t8], $f3=[$t10])\n"
            + "            CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_right]])\n");

    JSONObject result = executeQuery(query);
    verifyJoinResult(result);
  }

  @Test
  public void testLookupStaticWithStatic() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT, "lookup " + TEST_DYNAMIC_RIGHT + " a2 | fields a1, a2, a3, a4, a5");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(a1=[COALESCE(ITEM($17, 'a1'), $0)], a2=[$1], a3=[$10],"
            + " a4=[ITEM(MAP_CONCAT($8, $17), 'a4')], a5=[ITEM(MAP_CONCAT($8, $17), 'a5')])\n"
            + "        LogicalJoin(condition=[=($1, $9)], joinType=[left])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_right]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..5=[{inputs}], expr#6=['a1'], expr#7=[ITEM($t5, $t6)],"
            + " expr#8=[COALESCE($t7, $t0)], expr#9=[MAP_CONCAT($t2, $t5)], expr#10=['a4'],"
            + " expr#11=[ITEM($t9, $t10)], expr#12=['a5'], expr#13=[ITEM($t9, $t12)], a1=[$t8],"
            + " a2=[$t1], a3=[$t4], a4=[$t11], a5=[$t13])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        EnumerableHashJoin(condition=[=($1, $3)], joinType=[left])\n"
            + "          EnumerableCalc(expr#0..8=[{inputs}], proj#0..1=[{exprs}], _MAP=[$t8])\n"
            + "            EnumerableLimit(fetch=[200])\n"
            + "              CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_left]])\n"
            + "          EnumerableCalc(expr#0..8=[{inputs}], proj#0..1=[{exprs}], _MAP=[$t8])\n"
            + "            CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_right]])\n");

    JSONObject result = executeQuery(query);
    verifyJoinResult(result);
  }

  @Test
  public void testLookupDynamicWithStaticWithoutCast() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT, "lookup " + TEST_DYNAMIC_RIGHT + " a3 | fields a1, a2, a3, a4, a5");

    Throwable th = assertThrows(IllegalArgumentException.class, () -> executeQuery(query));
    assertEquals(
        "Source key `a3` needs to be specific type. Please cast explicitly.", th.getMessage());
  }

  @Test
  public void testLookupDynamicWithStaticWithCast() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT,
            "eval a3=cast(a3 as string)"
                + "| lookup "
                + TEST_DYNAMIC_RIGHT
                + " a3 | fields a1, a2, a3, a4, a5");

    JSONObject result = executeQuery(query);
    verifyJoinResult(result);
  }

  @Test
  public void testLookupDynamicWithDynamicWithoutCast() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT, "lookup " + TEST_DYNAMIC_RIGHT + " a4 | fields a1, a2, a3, a4, a5");
    Throwable th = assertThrows(IllegalArgumentException.class, () -> executeQuery(query));
    assertEquals(
        "Source key `a4` needs to be specific type. Please cast explicitly.", th.getMessage());
  }

  @Test
  public void testLookupDynamicWithDynamicWithCast() throws IOException {
    String query =
        source(
            TEST_DYNAMIC_LEFT,
            "eval a4=cast(a4 as int)"
                + "|lookup "
                + TEST_DYNAMIC_RIGHT
                + " a4 | fields a1, a2, a3, a4, a5");

    JSONObject result = executeQuery(query);
    verifyJoinResult(result);
  }

  private void createLeftIndex() throws IOException {
    if (isIndexExist(client(), TEST_DYNAMIC_LEFT)) {
      return;
    }

    String mapping =
        "{"
            + "\"mappings\": {"
            // Disable dynamic mapping - extra fields won't be indexed but will be stored
            + "  \"dynamic\": false,"
            + "  \"properties\": {"
            + "    \"a1\": {\"type\": \"text\"},"
            + "    \"a2\": {\"type\": \"long\"}"
            + "  }"
            + "}"
            + "}";

    createIndexByRestClient(client(), TEST_DYNAMIC_LEFT, mapping);

    String bulkData =
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"a1\":\"1-a1\",\"a2\":12,\"a3\":\"1-a3\",\"a4\":14,\"a5\":\"1-a5\"}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"a1\":\"2-a1\",\"a2\":22,\"a3\":\"2-a3\",\"a4\":24,\"a5\":\"2-a5\"}\n"
            + "{\"index\":{\"_id\":\"3\"}}\n"
            + "{\"a1\":\"3-a1\",\"a2\":32,\"a3\":\"3-a3\",\"a4\":34}\n"
            + "{\"index\":{\"_id\":\"4\"}}\n"
            + "{\"a1\":\"4-a1\",\"a2\":42,\"a3\":\"4-a3\",\"a4\":44}\n";

    Request request = new Request("POST", "/" + TEST_DYNAMIC_LEFT + "/_bulk?refresh=true");
    request.setJsonEntity(bulkData);
    client().performRequest(request);
  }

  private void createRightIndex() throws IOException {
    if (isIndexExist(client(), TEST_DYNAMIC_RIGHT)) {
      return;
    }

    String mapping =
        "{"
            + "\"mappings\": {"
            // Disable dynamic mapping - extra fields won't be indexed but will be stored
            + "  \"dynamic\": false,"
            + "  \"properties\": {"
            + "    \"a2\": {\"type\": \"long\"},"
            + "    \"a3\": {\"type\": \"text\"}"
            + "  }"
            + "}"
            + "}";

    createIndexByRestClient(client(), TEST_DYNAMIC_RIGHT, mapping);

    String bulkData =
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"a1\":\"1-a1\",\"a2\":12,\"a3\":\"1-a3\",\"a4\":14,\"a5\":\"1-a5_2\"}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"a1\":\"2-a1\",\"a2\":22,\"a3\":\"2-a3\",\"a4\":24}\n"
            + "{\"index\":{\"_id\":\"3\"}}\n"
            + "{\"a1\":\"3-a1\",\"a2\":32,\"a3\":\"3-a3\",\"a4\":34,\"a5\":\"3-a5_2\"}\n"
            + "{\"index\":{\"_id\":\"4\"}}\n"
            + "{\"a1\":\"4-a1\",\"a2\":42,\"a3\":\"4-a3\",\"a4\":44}\n";

    Request request = new Request("POST", "/" + TEST_DYNAMIC_RIGHT + "/_bulk?refresh=true");
    request.setJsonEntity(bulkData);
    client().performRequest(request);
  }

  private void verifyJoinResult(JSONObject result) {
    verifySchema(
        result,
        schema("a1", "string"),
        schema("a2", "bigint"),
        schema("a3", "string"),
        schema("a4", "int"),
        schema("a5", "string"));
    verifyDataRows(
        result,
        rows("1-a1", 12, "1-a3", 14, "1-a5_2"),
        rows("2-a1", 22, "2-a3", 24, "2-a5"),
        rows("3-a1", 32, "3-a3", 34, "3-a5_2"),
        rows("4-a1", 42, "4-a3", 44, null));
  }
}
