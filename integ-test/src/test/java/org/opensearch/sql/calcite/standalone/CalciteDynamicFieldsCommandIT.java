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

public class CalciteDynamicFieldsCommandIT extends CalcitePPLPermissiveIntegTestCase {

  private static final String TEST_INDEX_DYNAMIC = "test_dynamic_fields";

  @Override
  public void init() throws IOException {
    super.init();
    createTestIndexWithUnmappedFields();
    enableCalcite();
  }

  @Test
  public void testBasicProjection() throws IOException {
    String query =
        source(TEST_INDEX_DYNAMIC, "fields firstname, lastname, department, salary | head 1");
    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(fetch=[1])\n"
            + "        LogicalProject(firstname=[$1], lastname=[$2], department=[ITEM($9,"
            + " 'department')], salary=[ITEM($9, 'salary')])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=['department'],"
            + " expr#11=[ITEM($t9, $t10)], expr#12=['salary'], expr#13=[ITEM($t9, $t12)],"
            + " firstname=[$t1], lastname=[$t2], department=[$t11], salary=[$t13])\n"
            + "        EnumerableLimit(fetch=[1])\n"
            + "          CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);
    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("department", "string"),
        schema("salary", "int"));
    verifyDataRows(result, rows("John", "Doe", "Engineering", 75000));
  }

  @Test
  public void testWildcardNotExpanded() throws IOException {
    // wildcard won't expand dynamic fields for now
    JSONObject result =
        executeQuery(source(TEST_INDEX_DYNAMIC, "fields firstname, lastname, depart*, * | head 1"));

    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("account_number", "bigint"));
    verifyDataRows(result, rows("John", "Doe", 1));
  }

  @Test
  public void testProjectExclude() throws IOException {
    JSONObject result = executeQuery(source(TEST_INDEX_DYNAMIC, "fields - firstname | head 1"));

    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("lastname", "string"),
        schema("city", "string"),
        schema("department", "string"),
        schema("salary", "int"),
        schema("json", "string"));
    verifyDataRows(result, rows(1, "Doe", "NYC", "{\"n\":1}", "Engineering", 75000));
  }

  @Test
  public void testDedup() {
    JSONObject result =
        executeQuery(
            source(TEST_INDEX_DYNAMIC, "dedup department | fields account_number, department"));

    verifySchema(result, schema("account_number", "bigint"), schema("department", "string"));
    verifyDataRows(result, rows(3, "Sales"), rows(1, "Engineering"), rows(2, "Marketing"));
  }

  @Test
  public void testWhereAndFields() throws IOException {
    String query =
        source(TEST_INDEX_DYNAMIC, "where salary >= 80000 | fields account_number, salary");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("salary", "int"));
    verifyDataRows(result, rows(4, 80000));
  }

  @Test
  public void testFieldsAndWhere() throws IOException {
    String query =
        source(TEST_INDEX_DYNAMIC, "fields account_number, salary | where salary >= 80000");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("salary", "int"));
    verifyDataRows(result, rows(4, 80000));
  }

  @Test
  public void testRegex() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC, "regex department=\"Eng.*\" | fields account_number, department");
    assertThrows(RuntimeException.class, () -> executeQuery(query));
  }

  @Test
  public void testCastAndRegex() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eval department=CAST(department AS string) | regex department=\"Eng.*\" | fields"
                + " account_number, department");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("department", "string"));
    verifyDataRows(result, rows(1, "Engineering"), rows(4, "Engineering"));
  }

  @Test
  public void testRex() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "rex field=firstname \"(?<initial>[A-Z])\" | fields firstname, initial | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("firstname", "string"), schema("initial", "string"));
    verifyDataRows(result, rows("John", "J"));
  }

  @Test
  public void testCastAndRex() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eval department=CAST(department AS string) | rex field=department '(?<initial>[A-Z])'"
                + " | fields department, initial | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("initial", "string"));
    verifyDataRows(result, rows("Engineering", "E"));
  }

  @Test
  public void testCastAndParse() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eval department=CAST(department AS string) | fields department | parse department"
                + " '(?<initial>[A-Z]).*' | fields department, initial | head 1");
    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(fetch=[1])\n"
            + "        LogicalProject(department=[SAFE_CAST(ITEM($9, 'department'))],"
            + " initial=[ITEM(PARSE(SAFE_CAST(ITEM($9, 'department')),"
            + " '(?<initial>[A-Z]).*':VARCHAR, 'regex':VARCHAR), 'initial':VARCHAR)])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=['department'],"
            + " expr#11=[ITEM($t9, $t10)], expr#12=[SAFE_CAST($t11)],"
            + " expr#13=['(?<initial>[A-Z]).*':VARCHAR], expr#14=['regex':VARCHAR],"
            + " expr#15=[PARSE($t12, $t13, $t14)], expr#16=['initial':VARCHAR], expr#17=[ITEM($t15,"
            + " $t16)], department=[$t12], initial=[$t17])\n"
            + "        EnumerableLimit(fetch=[1])\n"
            + "          CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("initial", "string"));
    verifyDataRows(result, rows("Engineering", "E"));
  }

  @Test
  @Ignore("rename requires refactoring as it currently uses field list")
  public void testRename() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "rename department as dep, salary as sal | fields account_number, dep, sal | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("dep", "string"),
        schema("sal", "bigint"));
    verifyDataRows(result, rows(1, "Engineering", 75000));
  }

  @Test
  public void testSort() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "sort city | fields account_number, city | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("city", "string"));
    verifyDataRows(result, rows(5, "Boston"));
  }

  @Test
  public void testRevers() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "fields account_number, city | reverse | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("city", "string"));
    verifyDataRows(result, rows(5, "Boston"));
  }

  @Test
  public void testBin() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "bin salary span=10000 | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("salary", "string"),
        schema("city", "string"),
        schema("department", "string"),
        schema("json", "string"));
    verifyDataRows(
        result, rows(1, "John", "Doe", "70000-80000", "NYC", "{\"n\":1}", "Engineering"));
  }

  @Test
  public void testCastAndPatterns() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eval department=CAST(department as string) | patterns department method=simple_pattern"
                + " | fields department, patterns_field | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("patterns_field", "string"));
    verifyDataRows(result, rows("Engineering", "<*>"));
  }

  @Test
  public void testCastAndPatternsWithAggregation() throws IOException {
    // TODO:
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eval department=CAST(department as string) | patterns department mode=aggregation"
                + " method=simple_pattern | fields patterns_field, pattern_count, sample_logs |"
                + " head 1");
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("patterns_field", "string"),
        schema("pattern_count", "bigint"),
        schema("sample_logs", "array"));
    verifyDataRows(
        result, rows("<*>", 4, new String[] {"Engineering", "Marketing", "Sales", "Engineering"}));
  }

  @Test
  public void testSpath() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "spath input=json n | fields n | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("n", "string"));
    verifyDataRows(result, rows("1"));
  }

  @Test
  @Ignore("fillnull require logic change as it currently use field list")
  public void testFillnull() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "fillnull with '<NULL>' in department | where account_number >= 4 | fields"
                + " account_number, department");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("department", "string"));
    verifyDataRows(result, rows(4, "Engineering"), rows(5, "<NULL>"));
  }

  @Test
  @Ignore("Need to define the spec with the dynamic fields since we cannot decide the field set")
  public void testFlatten() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "flatten obj as (a, b)");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("department", "string"));
    verifyDataRows(result, rows(4, "Engineering"), rows(5, "<NULL>"));
  }

  @Test
  @Ignore("pending join adaptation")
  public void testExpand() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "expand arr as expanded | where isnotnull(expanded)");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("count", "bigint"));
    verifyDataRows(result, rows("Engineering", 2));
  }

  @Test
  @Ignore("pending join adaptation")
  public void testJoinWithStaticField() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "join left=l right=r on l.account_number = r.account_number | fields l.firstname,"
                + " r.department | head 1");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("l.firstname", "string"), schema("r.department", "string"));
    verifyDataRows(result, rows("John", "Engineering"));
  }

  @Test
  @Ignore("pending join adaptation")
  public void testJoinWithDynamicField() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "join left=l right=r on l.department = r.department | fields l.firstname, r.department"
                + " | head 1");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("l.firstname", "string"), schema("r.department", "string"));
    verifyDataRows(result, rows("John", "Engineering"));
  }

  @Test
  @Ignore("pending join adaptation")
  public void testLookupWithStaticField() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "lookup "
                + TEST_INDEX_DYNAMIC
                + " account_number as acc_num | fields firstname, department");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("firstname", "string"), schema("department", "string"));
    verifyDataRows(result, rows("John", "Engineering"));
  }

  @Test
  @Ignore("pending aggregation adaptation")
  public void testTop() throws IOException {
    String query = source(TEST_INDEX_DYNAMIC, "top 1 department");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("count", "bigint"));
    verifyDataRows(result, rows("Engineering", 2));
  }

  @Test
  @Ignore("pending aggregation adaptation")
  public void testStats() throws IOException {
    String query =
        source(TEST_INDEX_DYNAMIC, "stats count() by department | fields department, count()");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("count()", "bigint"));
    verifyDataRows(result, rows("Engineering", 2), rows("Marketing", 1), rows("Sales", 1));
  }

  @Test
  @Ignore("pending aggregation adaptation")
  public void testTimechart() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "timechart span=1d count() by department | fields _time, department, count()");

    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("_time", "timestamp"),
        schema("department", "string"),
        schema("count()", "bigint"));
    // Expected data would depend on the time-based aggregation
    verifyDataRows(result, rows("2023-01-01 00:00:00", "Engineering", 2));
  }

  @Test
  @Ignore("pending aggregation adaptation")
  public void testEventstats() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eventstats count() by department | fields account_number, department, count()");

    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("department", "string"),
        schema("count()", "bigint"));
    verifyDataRows(result, rows(1, "Engineering", 2), rows(4, "Engineering", 2));
  }

  @Test
  @Ignore("pending aggregation adaption")
  public void testTrendline() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "trendline sma(2, salary) as salary_trend | fields account_number, salary,"
                + " salary_trend");

    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("account_number", "bigint"),
        schema("salary", "int"),
        schema("salary_trend", "double"));
    verifyDataRows(result, rows(1, 75000, 70000.0), rows(2, 65000, 72500.0));
  }

  @Test
  @Ignore("pending subquery adaptation")
  public void testMultisearch() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "multisearch [search department='Engineering'] [search department='Marketing'] | fields"
                + " account_number, department");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("department", "string"));
    verifyDataRows(result, rows(1, "Engineering"), rows(4, "Engineering"), rows(2, "Marketing"));
  }

  @Test
  @Ignore("pending subquery adaptation")
  public void testSubquery() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "where account_number in [search department='Engineering' | fields account_number] |"
                + " fields account_number, firstname");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("firstname", "string"));
    verifyDataRows(result, rows(1, "John"), rows(4, "Alice"));
  }

  @Test
  public void testEval() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "eval salary = cast(salary as int) * 2 | fields firstname,"
                + " lastname, salary | head 1");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(fetch=[1])\n"
            + "        LogicalProject(firstname=[$1], lastname=[$2], salary=[*(SAFE_CAST(ITEM($9,"
            + " 'salary')), 2)])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=['salary'], expr#11=[ITEM($t9,"
            + " $t10)], expr#12=[SAFE_CAST($t11)], expr#13=[2], expr#14=[*($t12, $t13)],"
            + " firstname=[$t1], lastname=[$t2], salary=[$t14])\n"
            + "        EnumerableLimit(fetch=[1])\n"
            + "          CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "");

    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("firstname", "string"),
        schema("lastname", "string"),
        schema("salary", "int"));
  }

  private void createTestIndexWithUnmappedFields() throws IOException {
    if (isIndexExist(client(), TEST_INDEX_DYNAMIC)) {
      return;
    }

    String mapping =
        "{"
            + "\"mappings\": {"
            // Disable dynamic mapping - extra fields won't be indexed but will be stored
            + "  \"dynamic\": false,"
            + "  \"properties\": {"
            + "    \"firstname\": {\"type\": \"text\"},"
            + "    \"lastname\": {\"type\": \"text\"},"
            + "    \"account_number\": {\"type\": \"long\"}"
            + "  }"
            + "}"
            + "}";

    createIndexByRestClient(client(), TEST_INDEX_DYNAMIC, mapping);

    String bulkData =
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"firstname\":\"John\",\"lastname\":\"Doe\",\"account_number\":1,\"city\":\"NYC\",\"department\":\"Engineering\",\"salary\":75000,\"json\":\"{\\\"n\\\":1}\"}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"firstname\":\"Jane\",\"lastname\":\"Smith\",\"account_number\":2,\"city\":\"LA\",\"department\":\"Marketing\",\"salary\":65000}\n"
            + "{\"index\":{\"_id\":\"3\"}}\n"
            + "{\"firstname\":\"Bob\",\"lastname\":\"Johnson\",\"account_number\":3,\"city\":\"Chicago\",\"department\":\"Sales\"}\n"
            + "{\"index\":{\"_id\":\"4\"}}\n"
            + "{\"firstname\":\"Alice\",\"lastname\":\"Brown\",\"account_number\":4,\"city\":\"Seattle\",\"department\":\"Engineering\",\"salary\":80000}\n"
            + "{\"index\":{\"_id\":\"5\"}}\n"
            + "{\"firstname\":\"Charlie\",\"lastname\":\"Wilson\",\"account_number\":5,\"city\":\"Boston\",\"salary\":60000,\"arr\":[1,2,3],\"obj\":{\"a\":1,\"b\":2}}\n";

    Request request = new Request("POST", "/" + TEST_INDEX_DYNAMIC + "/_bulk?refresh=true");
    request.setJsonEntity(bulkData);
    client().performRequest(request);
  }
}
