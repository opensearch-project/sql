/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HOBBIES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OCCUPATION;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.TestsConstants;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLJoinIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    supportAllJoinTypes();

    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.OCCUPATION);
    loadIndex(Index.HOBBIES);
    Request request1 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_STATE_COUNTRY + "/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"Jim\",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    Request request2 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_STATE_COUNTRY + "/_doc/6?refresh=true");
    request2.setJsonEntity(
        "{\"name\":\"Peter\",\"age\":57,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request2);
    Request request3 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_STATE_COUNTRY + "/_doc/7?refresh=true");
    request3.setJsonEntity(
        "{\"name\":\"Rick\",\"age\":70,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request3);
    Request request4 =
        new Request("PUT", "/" + TestsConstants.TEST_INDEX_STATE_COUNTRY + "/_doc/8?refresh=true");
    request4.setJsonEntity(
        "{\"name\":\"David\",\"age\":40,\"state\":\"Washington\",\"country\":\"USA\",\"year\":2023,\"month\":4}");
    client().performRequest(request4);
  }

  @Test
  public void testJoinWithCondition() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | inner join left=a, right=b ON a.name = b.name AND a.year = 2023"
                    + " AND a.month = 4 AND b.year = 2023 AND b.month = 4 %s | fields a.name,"
                    + " a.age, a.state, a.country, b.occupation, b.country, b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "int"));
    verifyDataRows(
        actual,
        rows("Jake", 70, "California", "USA", "Engineer", "England", 100000),
        rows("Hello", 30, "New York", "USA", "Artist", "USA", 70000),
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
        rows("David", 40, "Washington", "USA", "Doctor", "USA", 120000),
        rows("David", 40, "Washington", "USA", "Unemployed", "Canada", 0));
  }

  @Test
  public void testJoinWithTwoJoinConditions() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name AND a.country ="
                    + " b.country AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month ="
                    + " 4 %s | fields a.name, a.age, a.state, a.country, b.occupation, b.country,"
                    + " b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "int"));
    verifyDataRows(
        actual,
        rows("Hello", 30, "New York", "USA", "Artist", "USA", 70000),
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
        rows("David", 40, "Washington", "USA", "Doctor", "USA", 120000));
  }

  @Test
  public void testJoinTwoColumnsAndDisjointFilters() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name AND a.country ="
                    + " b.country AND a.year = 2023 AND a.month = 4 AND b.salary > 100000 %s |"
                    + " fields a.name, a.age, a.state, a.country, b.occupation, b.country,"
                    + " b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "int"));
    verifyDataRows(
        actual,
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows("David", 40, "Washington", "USA", "Doctor", "USA", 120000));
  }

  @Test
  public void testJoinThenStats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name %s | stats avg(salary)"
                    + " by span(age, 10) as age_span",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(actual, schema("avg(salary)", "double"), schema("age_span", "int"));
    verifyDataRows(
        actual, rows(105000.0, 20), rows(70000.0, 30), rows(60000.0, 40), rows(100000.0, 70));
  }

  @Test
  public void testJoinThenStatsWithGroupBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name %s | stats avg(salary)"
                    + " by span(age, 10) as age_span, b.country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("avg(salary)", "double"),
        schema("age_span", "int"),
        schema("b.country", "string"));
    verifyDataRows(
        actual,
        rows(105000.0, 20, "Canada"),
        rows(70000.0, 30, "USA"),
        rows(0.0, 40, "Canada"),
        rows(120000.0, 40, "USA"),
        rows(100000.0, 70, "England"));
  }

  @Test
  public void testComplexInnerJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'USA' OR country = 'England' | inner join left=a,"
                    + " right=b ON a.name = b.name %s | stats avg(salary) by span(age, 10) as"
                    + " age_span, b.country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("avg(salary)", "double"),
        schema("age_span", "int"),
        schema("b.country", "string"));
    verifyDataRows(
        actual,
        rows(70000.0, 30, "USA"),
        rows(0.0, 40, "Canada"),
        rows(120000.0, 40, "USA"),
        rows(100000.0, 70, "England"));
  }

  @Test
  public void testComplexLeftJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | left join left=a,"
                    + " right=b ON a.name = b.name %s | sort a.age | fields a.name, a.age, a.state,"
                    + " a.country, b.occupation, b.country, b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "int"));
    verifyDataRowsInOrder(
        actual,
        rows("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows("Jim", 27, "B.C", "Canada", null, null, null),
        rows("Peter", 57, "B.C", "Canada", null, null, null),
        rows("Rick", 70, "B.C", "Canada", null, null, null));
  }

  @Test
  public void testComplexRightJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | right join left=a,"
                    + " right=b ON a.name = b.name %s | sort a.age | fields a.name, a.age, a.state,"
                    + " a.country, b.occupation, b.country, b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "int"));
    verifyDataRowsInOrder(
        actual,
        rows(null, null, null, null, "Engineer", "England", 100000),
        rows(null, null, null, null, "Artist", "USA", 70000),
        rows(null, null, null, null, "Doctor", "USA", 120000),
        rows(null, null, null, null, "Unemployed", "Canada", 0),
        rows("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000));
  }

  @Test
  public void testComplexSemiJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | left semi join"
                + " left=a, right=b ON a.name = b.name %s | sort a.age | fields name, country,"
                + " state, month, year, age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"));
    verifyDataRowsInOrder(
        actual,
        rows("Jane", "Canada", "Quebec", 4, 2023, 20),
        rows("John", "Canada", "Ontario", 4, 2023, 25));
  }

  @Test
  public void testComplexAntiJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | left anti join"
                    + " left=a, right=b ON a.name = b.name %s | sort a.age | fields name, country, state, month, year, age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"));
    verifyDataRowsInOrder(
        actual,
        rows("Jim", "Canada", "B.C", 4, 2023, 27),
        rows("Peter", "Canada", "B.C", 4, 2023, 57),
        rows("Rick", "Canada", "B.C", 4, 2023, 70));
  }

  @Test
  public void testComplexCrossJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | join left=a,"
                    + " right=b on 1=1 %s | sort a.age | stats count()",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(actual, schema("count()", "bigint"));
    verifyDataRowsInOrder(actual, rows(30));
  }

  @Test
  public void testNonEquiJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'USA' OR country = 'England' | inner join left=a,"
                    + " right=b ON age < salary %s |  where occupation = 'Doctor' OR occupation ="
                    + " 'Engineer' | fields a.name, age, state, a.country, occupation, b.country,"
                    + " salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "int"));
    verifyDataRows(
        actual,
        rows("Jake", 70, "California", "USA", "Engineer", "England", 100000),
        rows("Jake", 70, "California", "USA", "Doctor", "Canada", 120000),
        rows("Jake", 70, "California", "USA", "Doctor", "USA", 120000),
        rows("Hello", 30, "New York", "USA", "Engineer", "England", 100000),
        rows("Hello", 30, "New York", "USA", "Doctor", "Canada", 120000),
        rows("Hello", 30, "New York", "USA", "Doctor", "USA", 120000),
        rows("David", 40, "Washington", "USA", "Engineer", "England", 100000),
        rows("David", 40, "Washington", "USA", "Doctor", "Canada", 120000),
        rows("David", 40, "Washington", "USA", "Doctor", "USA", 120000));
  }

  @Test
  public void testCrossJoinWithJoinCriteriaFallbackToInnerJoin() throws IOException {
    var cross =
        executeQuery(
            String.format(
                "source = %s | where country = 'USA' | cross join left=a, right=b ON a.name ="
                    + " b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    var inner =
        executeQuery(
            String.format(
                "source = %s | where country = 'USA' | inner join left=a, right=b ON a.name ="
                    + " b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(cross.toString(), inner.toString());
  }

  @Test
  public void testMultipleJoins() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s" +
                "| where country = 'Canada' OR country = 'England'" +
                "| inner join left=a, right=b" +
                "    ON a.name = b.name AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4" +
                "    %s" +
                "| eval a_name = a.name" +
                "| eval a_country = a.country" +
                "| eval b_country = b.country" +
                "| fields a_name, age, state, a_country, occupation, b_country, salary" +
                "| left join left=a, right=b" +
                "    ON a.a_name = b.name" +
                "    %s" +
                "| eval aa_country = a.a_country" +
                "| eval ab_country = a.b_country" +
                "| eval bb_country = b.country" +
                "| fields a_name, age, state, aa_country, occupation, ab_country, salary, bb_country, hobby, language" +
                "| cross join left=a, right=b on 1=1" +
                "    %s" +
                "| eval new_country = a.aa_country" +
                "| eval new_salary = b.salary" +
                "| stats avg(new_salary) as avg_salary by span(age, 5) as age_span, state" +
                "| left semi join left=a, right=b" +
                "    ON a.state = b.state" +
                "    %s" +
                "| eval new_avg_salary = floor(avg_salary)" +
                "| fields state, age_span, new_avg_salary",
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_STATE_COUNTRY));
    verifyNumOfRows(actual, 2);
  }

  @Test
  public void testMultipleJoinsWithRelationSubquery() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s| where country = 'Canada' OR country = 'England'| inner join left=a,"
                    + " right=b    ON a.name = b.name AND a.year = 2023 AND a.month = 4 AND b.year"
                    + " = 2023 AND b.month = 4    [      source = %s    ]| eval a_name = a.name|"
                    + " eval a_country = a.country| eval b_country = b.country| fields a_name, age,"
                    + " state, a_country, occupation, b_country, salary| left join left=a, right=b "
                    + "   ON a.a_name = b.name    [      source = %s    ]| eval aa_country ="
                    + " a.a_country| eval ab_country = a.b_country| eval bb_country = b.country|"
                    + " fields a_name, age, state, aa_country, occupation, ab_country, salary,"
                    + " bb_country, hobby, language| cross join left=a, right=b on 1=1 [     "
                    + " source = %s    ]| eval new_country = a.aa_country| eval new_salary ="
                    + " b.salary| stats avg(new_salary) as avg_salary by span(age, 5) as age_span,"
                    + " state| left semi join left=a, right=b    ON a.state = b.state    [     "
                    + " source = %s    ]| eval new_avg_salary = floor(avg_salary)| fields state,"
                    + " age_span, new_avg_salary",
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_STATE_COUNTRY));
    verifyNumOfRows(actual, 2);
  }

  @Test
  public void testMultipleJoinsWithoutTableAliases() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | JOIN ON %s.name = %s.name %s | JOIN ON %s.name = %s.name %s | fields"
                    + " %s.name, %s.name, %s.name",
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES));
    verifySchema(
        actual,
        schema("name", "string"),
        schema(TEST_INDEX_OCCUPATION + ".name", "string"),
        schema(TEST_INDEX_HOBBIES + ".name", "string"));
    verifyDataRows(
        actual,
        rows("David", "David", "David"),
        rows("David", "David", "David"),
        rows("Hello", "Hello", "Hello"),
        rows("Jake", "Jake", "Jake"),
        rows("Jane", "Jane", "Jane"),
        rows("John", "John", "John"));
  }

  @Test
  public void testMultipleJoinsWithPartTableAliases() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s | JOIN right = t3"
                    + " ON t1.name = t3.name %s | fields t1.name, t2.name, t3.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION, TEST_INDEX_HOBBIES));
    verifySchema(
        actual, schema("name", "string"), schema("t2.name", "string"), schema("t3.name", "string"));
    verifyDataRows(
        actual,
        rows("David", "David", "David"),
        rows("David", "David", "David"),
        rows("Hello", "Hello", "Hello"),
        rows("Jake", "Jake", "Jake"),
        rows("Jane", "Jane", "Jane"),
        rows("John", "John", "John"));
  }

  @Test
  public void testMultipleJoinsWithSelfJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s | JOIN right = t3"
                    + " ON t1.name = t3.name %s | JOIN right = t4 ON t1.name = t4.name %s | fields"
                    + " t1.name, t2.name, t3.name, t4.name",
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("t2.name", "string"),
        schema("t3.name", "string"),
        schema("t4.name", "string"));
    verifyDataRows(
        actual,
        rows("David", "David", "David", "David"),
        rows("David", "David", "David", "David"),
        rows("Hello", "Hello", "Hello", "Hello"),
        rows("Jake", "Jake", "Jake", "Jake"),
        rows("Jane", "Jane", "Jane", "Jane"),
        rows("John", "John", "John", "John"));
  }

  @Test
  public void testMultipleJoinsWithSubquerySelfJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s | JOIN right = t3"
                    + " ON t1.name = t3.name %s | JOIN ON t1.name = t4.name [ source = %s ] as t4 |"
                    + " fields t1.name, t2.name, t3.name, t4.name",
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("t2.name", "string"),
        schema("t3.name", "string"),
        schema("t4.name", "string"));
    verifyDataRows(
        actual,
        rows("David", "David", "David", "David"),
        rows("David", "David", "David", "David"),
        rows("Hello", "Hello", "Hello", "Hello"),
        rows("Jake", "Jake", "Jake", "Jake"),
        rows("Jane", "Jane", "Jane", "Jane"),
        rows("John", "John", "John", "John"));
  }

  @Test
  public void testCheckAccessTheReferenceByAliases() throws IOException {
    JSONObject res1 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields t1.name,"
                    + " t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res2 =
        executeQuery(
            String.format(
                "source = %s as t1 | JOIN ON t1.name = t2.name %s as t2 | fields t1.name, t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res2.getJSONArray("datarows").toString());

    JSONObject res3 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s as tt | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res4 =
        executeQuery(
            String.format(
                "source = %s as tt | JOIN left = t1 right = t2 ON t1.name = t2.name %s | fields"
                    + " t1.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res5 =
        executeQuery(
            String.format(
                "source = %s as tt | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields"
                    + " t1.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(
        res3.getJSONArray("datarows").toString(), res4.getJSONArray("datarows").toString());
    assertJsonEquals(
        res4.getJSONArray("datarows").toString(), res5.getJSONArray("datarows").toString());
  }

  @Test
  public void testCheckAccessTheReferenceBySubqueryAliases() throws IOException {
    JSONObject res1 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name [ source = %s ] as t2 | fields"
                    + " t1.name, t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res2 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name [ source = %s as t2 ] | fields"
                    + " t1.name, t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res2.getJSONArray("datarows").toString());

    JSONObject res3 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s as"
                    + " tt ] | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res4 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name [ source = %s as tt ] as t2"
                    + " | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res5 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s ]"
                    + " as tt | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(
        res3.getJSONArray("datarows").toString(), res4.getJSONArray("datarows").toString());
    assertJsonEquals(
        res4.getJSONArray("datarows").toString(), res5.getJSONArray("datarows").toString());
  }

  @Test
  public void testCheckAccessTheReferenceByOverrideAliases() throws IOException {
    JSONObject res1 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s as tt | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res2 =
        executeQuery(
            String.format(
                "source = %s as tt | JOIN left = t1 right = t2 ON t1.name = t2.name %s | fields"
                    + " t1.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res3 =
        executeQuery(
            String.format(
                "source = %s as tt | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields"
                    + " t1.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res2.getJSONArray("datarows").toString());
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res3.getJSONArray("datarows").toString());
  }

  @Test
  public void testCheckAccessTheReferenceByOverrideSubqueryAliases() throws IOException {
    JSONObject res1 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s as tt ]"
                    + " | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res2 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s as tt ]"
                    + " as t2 | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res3 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s ] as tt"
                    + " | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res2.getJSONArray("datarows").toString());
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res3.getJSONArray("datarows").toString());
  }

  @Test
  public void testCheckAccessTheReferenceByOverrideSubqueryAliases2() throws IOException {
    JSONObject res1 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s as tt ]"
                    + " | fields t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res2 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s as tt ]"
                    + " as t2 | fields t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res3 =
        executeQuery(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s ] as tt"
                    + " | fields t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res2.getJSONArray("datarows").toString());
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res3.getJSONArray("datarows").toString());
  }

  @Test
  public void testInnerJoinWithRelationSubquery() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where country = 'USA' OR country = 'England'"
                    + "| inner join left=a, right=b"
                    + "    ON a.name = b.name"
                    + "    ["
                    + "      source = %s"
                    + "      | where salary > 0"
                    + "      | fields name, country, salary"
                    + "      | sort salary"
                    + "      | head 3"
                    + "    ]"
                    + "| stats avg(salary) by span(age, 10) as age_span, b.country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("avg(salary)", "double"),
        schema("age_span", "int"),
        schema("b.country", "string"));
    verifyDataRowsInOrder(actual, rows(70000.0, 30, "USA"), rows(100000, 70, "England"));
  }

  @Test
  public void testLeftJoinWithRelationSubquery() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s"
                    + "| where country = 'USA' OR country = 'England'"
                    + "| left join left=a, right=b"
                    + "    ON a.name = b.name"
                    + "    ["
                    + "      source = %s"
                    + "      | where salary > 0"
                    + "      | fields name, country, salary"
                    + "      | sort salary"
                    + "      | head 3"
                    + "    ]"
                    + "| stats avg(salary) by span(age, 10) as age_span, b.country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("b.country", "string"),
        schema("age_span", "int"),
        schema("avg(salary)", "double"));
    verifyDataRows(
        actual, rows(70000.0, 30, "USA"), rows(null, 40, null), rows(100000, 70, "England"));
  }

  @Test
  public void testJoinWithFieldList() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | join type=inner name,year,month %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"),
        schema("occupation", "string"),
        schema("salary", "int"));
    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | join type=inner name,year,month %s | fields name, country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyDataRows(
        actual2,
        rows("Jake", "England"),
        rows("Hello", "USA"),
        rows("John", "Canada"),
        rows("Jane", "Canada"),
        rows("David", "USA"),
        rows("David", "Canada"));
  }

  @Test
  public void testJoinWithFieldList2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | join type=inner overwrite=false name,year,month %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"),
        schema("occupation", "string"),
        schema("salary", "int"));
    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | join type=inner overwrite=false name,year,month %s | fields"
                    + " name, country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyDataRows(
        actual2,
        rows("Jake", "USA"),
        rows("Hello", "USA"),
        rows("John", "Canada"),
        rows("Jane", "Canada"),
        rows("David", "USA"),
        rows("David", "USA"));
  }

  @Test
  public void testJoinWithFieldListSelfJoin() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | join name,year,month %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyNumOfRows(actual, 8);
  }

  @Test
  public void testJoinWithFieldListSelfJoin2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | join type=inner overwrite=true name,year,month %s | join"
                    + " type=left overwrite=false name,year,month %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyNumOfRows(actual, 8);
  }

  @Test
  public void testJoinWithoutFieldList() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | join type=inner overwrite=false %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyNumOfRows(actual, 8);
  }

  @Test
  public void testJoinWithFieldListMaxEqualsOne() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | join type=inner max=1 name,year,month %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"),
        schema("occupation", "string"),
        schema("salary", "int"));
    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | join type=inner max=1 name,year,month %s | fields name, country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyDataRows(
        actual2,
        rows("Jake", "England"),
        rows("Jane", "Canada"),
        rows("John", "Canada"),
        rows("Hello", "USA"),
        rows("David", "USA"));
  }

  @Test
  public void testJoinWhenLegacyNotPreferred() throws IOException {
    withSettings(
        Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "false",
        () -> {
          JSONObject actual = null;
          try {
            actual =
                executeQuery(
                    String.format(
                        "source=%s | join type=inner name,year,month %s",
                        TestsConstants.TEST_INDEX_STATE_COUNTRY,
                        TestsConstants.TEST_INDEX_OCCUPATION));
          } catch (IOException e) {
            fail();
          }
          verifySchema(
              actual,
              schema("name", "string"),
              schema("age", "int"),
              schema("state", "string"),
              schema("country", "string"),
              schema("year", "int"),
              schema("month", "int"),
              schema("occupation", "string"),
              schema("salary", "int"));
          JSONObject actual2 = null;
          try {
            actual2 =
                executeQuery(
                    String.format(
                        "source=%s | join type=inner max=1 name,year,month %s | fields name,"
                            + " country",
                        TestsConstants.TEST_INDEX_STATE_COUNTRY,
                        TestsConstants.TEST_INDEX_OCCUPATION));
          } catch (IOException e) {
            fail();
          }
          verifyDataRows(
              actual2,
              rows("Jake", "England"),
              rows("Jane", "Canada"),
              rows("John", "Canada"),
              rows("Hello", "USA"),
              rows("David", "USA"));
        });
  }

  @Test
  public void testJoinComparing() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where country = 'Canada' | join type=inner max=0 country %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyNumOfRows(actual, 15);
    actual =
        executeQuery(
            String.format(
                "source=%s | where country = 'Canada' | join type=inner max=1 country %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyNumOfRows(actual, 5);
    actual =
        executeQuery(
            String.format(
                "source=%s | where country = 'Canada' | join type=inner max=2 country %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyNumOfRows(actual, 10);
    actual =
        executeQuery(
            String.format(
                "source=%s | where country = 'Canada' | join max=0 left=l right=r on l.country ="
                    + " r.country %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyNumOfRows(actual, 15);
    actual =
        executeQuery(
            String.format(
                "source=%s | where country = 'Canada' | join max=1 left=l right=r on l.country ="
                    + " r.country %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyNumOfRows(actual, 5);
    actual =
        executeQuery(
            String.format(
                "source=%s | where country = 'Canada' | join max=2 left=l right=r on l.country ="
                    + " r.country %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyNumOfRows(actual, 10);
  }

  @Test
  public void testJoinWithoutFieldListMaxEqualsOne() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | join type=inner overwrite=false max=1 %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("state", "string"),
        schema("country", "string"),
        schema("year", "int"),
        schema("month", "int"));
    verifyNumOfRows(actual, 8);
  }

  @Test
  public void testJoinSubsearchMaxOut() throws IOException {
    setJoinSubsearchMaxOut(5);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where country = 'Canada' | join type=inner max=0 country %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyNumOfRows(actual, 10);
    resetJoinSubsearchMaxOut();
    actual =
        executeQuery(
            String.format(
                "source=%s | where country = 'Canada' | join type=inner max=0 country %s",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifyNumOfRows(actual, 15);
  }

  @Test
  public void testSimpleSortPushDownForSMJ() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | join left=a right=b on a.age + 3 = b.age - 2 %s | fields name, age,"
                    + " b.name, b.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("age", "int"),
        schema("b.name", "string"),
        schema("b.age", "int"));
    verifyDataRows(actual, rows("Jane", 20, "John", 25), rows("John", 25, "Hello", 30));
  }

  @Test
  public void testComplexSortPushDownForSMJ() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval name2=substring(name, 2, 1) | join left=a right=b on a.name2 ="
                    + " b.state2 [ source=%s | eval state2=substring(state, 2, 1) ] | fields name,"
                    + " name2, b.name, b.state, state2",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("name2", "string"),
        schema("b.name", "string"),
        schema("b.state", "string"),
        schema("state2", "string"));
    verifyDataRows(
        actual,
        rows("Jake", "a", "Jake", "California", "a"),
        rows("Jake", "a", "David", "Washington", "a"),
        rows("Jane", "a", "Jake", "California", "a"),
        rows("Jane", "a", "David", "Washington", "a"),
        rows("David", "a", "Jake", "California", "a"),
        rows("David", "a", "David", "Washington", "a"),
        rows("Hello", "e", "Hello", "New York", "e"),
        rows("Peter", "e", "Hello", "New York", "e"));
  }

  @Test
  public void testComplexSortPushDownForSMJWithMaxOptionAndFieldList() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval name2=substring(name, 2, 1) | join max=1 name2,age [ source=%s |"
                    + " eval name2=substring(state, 2, 1) ] | fields name, country, state, month, year, age, name2",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("name2", "string"));
    verifyDataRows(
        actual,
        rows("David", "USA", "Washington", 4, 2023, 40, "a"),
        rows("Jake", "USA", "California", 4, 2023, 70, "a"),
        rows("Hello", "USA", "New York", 4, 2023, 30, "e"));
  }
}
