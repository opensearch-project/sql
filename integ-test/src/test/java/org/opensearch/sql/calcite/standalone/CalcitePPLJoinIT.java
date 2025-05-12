/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HOBBIES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OCCUPATION;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestsConstants;

public class CalcitePPLJoinIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

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
  public void testJoinWithCondition() {
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
        schema("age", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "integer"));
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
  public void testJoinWithTwoJoinConditions() {
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
        schema("age", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "integer"));
    verifyDataRows(
        actual,
        rows("Hello", 30, "New York", "USA", "Artist", "USA", 70000),
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
        rows("David", 40, "Washington", "USA", "Doctor", "USA", 120000));
  }

  @Test
  public void testJoinTwoColumnsAndDisjointFilters() {
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
        schema("age", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "integer"));
    verifyDataRows(
        actual,
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows("David", 40, "Washington", "USA", "Doctor", "USA", 120000));
  }

  @Test
  public void testJoinThenStats() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name %s | stats avg(salary)"
                    + " by span(age, 10) as age_span",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(actual, schema("avg(salary)", "double"), schema("age_span", "integer"));
    verifyDataRows(
        actual, rows(105000.0, 20), rows(70000.0, 30), rows(60000.0, 40), rows(100000.0, 70));
  }

  @Test
  public void testJoinThenStatsWithGroupBy() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name %s | stats avg(salary)"
                    + " by span(age, 10) as age_span, b.country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("avg(salary)", "double"),
        schema("age_span", "integer"),
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
  public void testComplexInnerJoin() {
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
        schema("age_span", "integer"),
        schema("b.country", "string"));
    verifyDataRows(
        actual,
        rows(70000.0, 30, "USA"),
        rows(0.0, 40, "Canada"),
        rows(120000.0, 40, "USA"),
        rows(100000.0, 70, "England"));
  }

  @Test
  public void testComplexLeftJoin() {
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
        schema("age", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "integer"));
    verifyDataRowsInOrder(
        actual,
        rows("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows("Jim", 27, "B.C", "Canada", null, null, null),
        rows("Peter", 57, "B.C", "Canada", null, null, null),
        rows("Rick", 70, "B.C", "Canada", null, null, null));
  }

  @Test
  public void testComplexRightJoin() {
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
        schema("age", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "integer"));
    verifyDataRowsInOrder(
        actual,
        rows("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows(null, null, null, null, "Engineer", "England", 100000),
        rows(null, null, null, null, "Artist", "USA", 70000),
        rows(null, null, null, null, "Doctor", "USA", 120000),
        rows(null, null, null, null, "Unemployed", "Canada", 0));
  }

  @Test
  public void testComplexSemiJoin() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | left semi join"
                    + " left=a, right=b ON a.name = b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "integer"),
        schema("year", "integer"),
        schema("age", "integer"));
    verifyDataRowsInOrder(
        actual,
        rows("Jane", "Canada", "Quebec", 4, 2023, 20),
        rows("John", "Canada", "Ontario", 4, 2023, 25));
  }

  @Test
  public void testComplexAntiJoin() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | left anti join"
                    + " left=a, right=b ON a.name = b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "integer"),
        schema("year", "integer"),
        schema("age", "integer"));
    verifyDataRowsInOrder(
        actual,
        rows("Jim", "Canada", "B.C", 4, 2023, 27),
        rows("Peter", "Canada", "B.C", 4, 2023, 57),
        rows("Rick", "Canada", "B.C", 4, 2023, 70));
  }

  @Test
  public void testComplexCrossJoin() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | join left=a,"
                    + " right=b %s | sort a.age | stats count()",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(actual, schema("count()", "long"));
    verifyDataRowsInOrder(actual, rows(30));
  }

  @Test
  public void testNonEquiJoin() {
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
        schema("age", "integer"),
        schema("state", "string"),
        schema("country", "string"),
        schema("occupation", "string"),
        schema("b.country", "string"),
        schema("salary", "integer"));
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
  public void testCrossJoinWithJoinCriteriaFallbackToInnerJoin() {
    String cross =
        execute(
            String.format(
                "source = %s | where country = 'USA' | cross join left=a, right=b ON a.name ="
                    + " b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String inner =
        execute(
            String.format(
                "source = %s | where country = 'USA' | inner join left=a, right=b ON a.name ="
                    + " b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(cross, inner);
  }

  @Ignore // TODO seems a calcite bug
  public void testMultipleJoins() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where country = 'Canada' OR country = 'England'
                   | inner join left=a, right=b
                       ON a.name = b.name AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4
                       %s
                   | eval a_name = a.name
                   | eval a_country = a.country
                   | eval b_country = b.country
                   | fields a_name, age, state, a_country, occupation, b_country, salary
                   | left join left=a, right=b
                       ON a.a_name = b.name
                       %s
                   | eval aa_country = a.a_country
                   | eval ab_country = a.b_country
                   | eval bb_country = b.country
                   | fields a_name, age, state, aa_country, occupation, ab_country, salary, bb_country, hobby, language
                   | cross join left=a, right=b
                       %s
                   | eval new_country = a.aa_country
                   | eval new_salary = b.salary
                   | stats avg(new_salary) as avg_salary by span(age, 5) as age_span, state
                   | left semi join left=a, right=b
                       ON a.state = b.state
                       %s
                   | eval new_avg_salary = floor(avg_salary)
                   | fields state, age_span, new_avg_salary
                   """,
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_STATE_COUNTRY));
  }

  @Ignore // TODO seems a calcite bug
  public void testMultipleJoinsWithRelationSubquery() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where country = 'Canada' OR country = 'England'
                   | inner join left=a, right=b
                       ON a.name = b.name AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4
                       [
                         source = %s
                       ]
                   | eval a_name = a.name
                   | eval a_country = a.country
                   | eval b_country = b.country
                   | fields a_name, age, state, a_country, occupation, b_country, salary
                   | left join left=a, right=b
                       ON a.a_name = b.name
                       [
                         source = %s
                       ]
                   | eval aa_country = a.a_country
                   | eval ab_country = a.b_country
                   | eval bb_country = b.country
                   | fields a_name, age, state, aa_country, occupation, ab_country, salary, bb_country, hobby, language
                   | cross join left=a, right=b
                       [
                         source = %s
                       ]
                   | eval new_country = a.aa_country
                   | eval new_salary = b.salary
                   | stats avg(new_salary) as avg_salary by span(age, 5) as age_span, state
                   | left semi join left=a, right=b
                       ON a.state = b.state
                       [
                         source = %s
                       ]
                   | eval new_avg_salary = floor(avg_salary)
                   | fields state, age_span, new_avg_salary
                   """,
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_STATE_COUNTRY));
  }

  @Test
  public void testMultipleJoinsWithoutTableAliases() {
    // source = STATE_COUNTRY
    // | JOIN ON STATE_COUNTRY.name = OCCUPATION.name OCCUPATION
    // | JOIN ON OCCUPATION.name = HOBBIES.name HOBBIES
    // | fields STATE_COUNTRY.name, OCCUPATION.name, HOBBIES.name
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
        schema("OpenSearch." + TEST_INDEX_OCCUPATION + ".name", "string"),
        schema("OpenSearch." + TEST_INDEX_HOBBIES + ".name", "string"));
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
  public void testMultipleJoinsWithPartTableAliases() {
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
  public void testMultipleJoinsWithSelfJoin() {
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
  public void testMultipleJoinsWithSubquerySelfJoin() {
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
  public void testCheckAccessTheReferenceByAliases() {
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
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res5 =
        executeQuery(
            String.format(
                "source = %s as tt | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(
        res3.getJSONArray("datarows").toString(), res4.getJSONArray("datarows").toString());
    assertJsonEquals(
        res4.getJSONArray("datarows").toString(), res5.getJSONArray("datarows").toString());
  }

  @Test
  public void testCheckAccessTheReferenceBySubqueryAliases() {
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
  public void testCheckAccessTheReferenceByOverrideAliases() {
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
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    JSONObject res3 =
        executeQuery(
            String.format(
                "source = %s as tt | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res2.getJSONArray("datarows").toString());
    assertJsonEquals(
        res1.getJSONArray("datarows").toString(), res3.getJSONArray("datarows").toString());
  }

  @Test
  public void testCheckAccessTheReferenceByOverrideSubqueryAliases() {
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
  public void testCheckAccessTheReferenceByOverrideSubqueryAliases2() {
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
  public void testInnerJoinWithRelationSubquery() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where country = 'USA' OR country = 'England'
                   | inner join left=a, right=b
                       ON a.name = b.name
                       [
                         source = %s
                         | where salary > 0
                         | fields name, country, salary
                         | sort salary
                         | head 3
                       ]
                   | stats avg(salary) by span(age, 10) as age_span, b.country
                   """,
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("avg(salary)", "double"),
        schema("age_span", "integer"),
        schema("b.country", "string"));
    verifyDataRowsInOrder(actual, rows(70000.0, 30, "USA"), rows(100000, 70, "England"));
  }

  @Test
  public void testLeftJoinWithRelationSubquery() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                   source = %s
                   | where country = 'USA' OR country = 'England'
                   | left join left=a, right=b
                       ON a.name = b.name
                       [
                         source = %s
                         | where salary > 0
                         | fields name, country, salary
                         | sort salary
                         | head 3
                       ]
                   | stats avg(salary) by span(age, 10) as age_span, b.country
                   """,
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    verifySchema(
        actual,
        schema("b.country", "string"),
        schema("age_span", "integer"),
        schema("avg(salary)", "double"));
    verifyDataRows(
        actual, rows(70000.0, 30, "USA"), rows(null, 40, null), rows(100000, 70, "England"));
  }
}
