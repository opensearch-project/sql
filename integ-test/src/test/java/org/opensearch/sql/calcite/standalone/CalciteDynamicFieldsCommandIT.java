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
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("department", "string"));
    verifyDataRows(result, rows(1, "Engineering"), rows(4, "Engineering"));
  }

  @Test
  public void testRex() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "rex field=department '(?<initial>[A-Z])'" + " | fields department, initial | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("initial", "string"));
    verifyDataRows(result, rows("Engineering", "E"));
  }

  @Test
  public void testRexWithNumeric() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "rex field=salary '(?<initial>[0-9])'" + " | fields salary, initial | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("salary", "int"), schema("initial", "string"));
    verifyDataRows(result, rows(75000, "7"));
  }

  @Test
  public void testParse() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "parse department '(?<initial>[A-Z]).*' | fields department, initial | head 1");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(fetch=[1])\n"
            + "        LogicalProject(department=[ITEM($9, 'department')],"
            + " initial=[ITEM(PARSE(SAFE_CAST(ITEM($9, 'department')),"
            + " '(?<initial>[A-Z]).*':VARCHAR, 'regex':VARCHAR), 'initial':VARCHAR)])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=['department'],"
            + " expr#11=[ITEM($t9, $t10)], expr#12=[SAFE_CAST($t11)],"
            + " expr#13=['(?<initial>[A-Z]).*':VARCHAR], expr#14=['regex':VARCHAR],"
            + " expr#15=[PARSE($t12, $t13, $t14)], expr#16=['initial':VARCHAR], expr#17=[ITEM($t15,"
            + " $t16)], department=[$t11], initial=[$t17])\n"
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
    String query =
        source(TEST_INDEX_DYNAMIC, "bin salary span=10000 | fields account_number, salary");
    JSONObject result = executeQuery(query);

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalProject(account_number=[$0], salary=[SPAN_BUCKET(ITEM($9, 'salary'),"
            + " 10000)])\n"
            + "        CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableCalc(expr#0..9=[{inputs}], expr#10=['salary'], expr#11=[ITEM($t9,"
            + " $t10)], expr#12=[10000], expr#13=[SPAN_BUCKET($t11, $t12)], account_number=[$t0],"
            + " salary=[$t13])\n"
            + "      EnumerableLimit(fetch=[200])\n"
            + "        CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n");

    verifySchema(result, schema("account_number", "bigint"), schema("salary", "string"));
    verifyDataRows(
        result,
        rows(1, "70000-80000"),
        rows(2, "60000-70000"),
        rows(3, null),
        rows(4, "80000-90000"),
        rows(5, "60000-70000"));
  }

  @Test
  public void testPatterns() throws IOException {
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "patterns department method=simple_pattern"
                + " | fields department, patterns_field | head 1");
    JSONObject result = executeQuery(query);

    verifySchema(result, schema("department", "string"), schema("patterns_field", "string"));
    verifyDataRows(result, rows("Engineering", "<*>"));
  }

  @Test
  public void testPatternsWithAggregation() throws IOException {
    // TODO:
    String query =
        source(
            TEST_INDEX_DYNAMIC,
            "patterns department mode=aggregation"
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
            "eval salary = 1 + salary * 2 | fields account_number, salary | head 1");

    assertExplainYaml(
        query,
        "calcite:\n"
            + "  logical: |\n"
            + "    LogicalSystemLimit(fetch=[200], type=[QUERY_SIZE_LIMIT])\n"
            + "      LogicalSort(fetch=[1])\n"
            + "        LogicalProject(account_number=[$0], salary=[+(1, *(SAFE_CAST(ITEM($9,"
            + " 'salary')), 2.0E0:DOUBLE))])\n"
            + "          CalciteLogicalIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n"
            + "  physical: |\n"
            + "    EnumerableLimit(fetch=[200])\n"
            + "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=[1], expr#11=['salary'],"
            + " expr#12=[ITEM($t9, $t11)], expr#13=[SAFE_CAST($t12)], expr#14=[2.0E0:DOUBLE],"
            + " expr#15=[*($t13, $t14)], expr#16=[+($t10, $t15)], account_number=[$t0],"
            + " salary=[$t16])\n"
            + "        EnumerableLimit(fetch=[1])\n"
            + "          CalciteEnumerableIndexScan(table=[[OpenSearch, test_dynamic_fields]])\n");

    JSONObject result = executeQuery(query);

    verifySchema(result, schema("account_number", "bigint"), schema("salary", "double"));
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
