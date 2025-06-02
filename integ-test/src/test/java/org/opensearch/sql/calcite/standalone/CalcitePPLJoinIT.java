/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HOBBIES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OCCUPATION;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class CalcitePPLJoinIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.OCCUPATION);
    loadIndex(Index.HOBBIES);
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
        schema("country0", "string"),
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
        schema("country0", "string"),
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
        schema("country0", "string"),
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
    verifySchema(actual, schema("age_span", "double"), schema("avg(salary)", "double"));
    verifyDataRows(
        actual,
        rows(70.0, 100000.0),
        rows(40.0, 60000.0),
        rows(20.0, 105000.0),
        rows(30.0, 70000.0));
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
        schema("b.country", "string"),
        schema("age_span", "double"),
        schema("avg(salary)", "double"));
    verifyDataRows(
        actual,
        rows("Canada", 40.0, 0.0),
        rows("Canada", 20.0, 105000.0),
        rows("USA", 40.0, 120000.0),
        rows("England", 70.0, 100000.0),
        rows("USA", 30.0, 70000.0));
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
        schema("b.country", "string"),
        schema("age_span", "double"),
        schema("avg(salary)", "double"));
    verifyDataRows(
        actual,
        rows("Canada", 40.0, 0.0),
        rows("USA", 40.0, 120000.0),
        rows("England", 70.0, 100000.0),
        rows("USA", 30.0, 70000.0));
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
        schema("country0", "string"),
        schema("salary", "integer"));
    verifyDataRows(
        actual,
        rows("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows("Jim", 27, "B.C", "Canada", null, null, 0),
        rows("Peter", 57, "B.C", "Canada", null, null, 0),
        rows("Rick", 70, "B.C", "Canada", null, null, 0));
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
        schema("country0", "string"),
        schema("salary", "integer"));
    verifyDataRows(
        actual,
        rows("Jane", 20, "Quebec", "Canada", "Scientist", "Canada", 90000),
        rows("John", 25, "Ontario", "Canada", "Doctor", "Canada", 120000),
        rows(null, 0, null, null, "Engineer", "England", 100000),
        rows(null, 0, null, null, "Artist", "USA", 70000),
        rows(null, 0, null, null, "Doctor", "USA", 120000),
        rows(null, 0, null, null, "Unemployed", "Canada", 0));
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
    verifyDataRows(
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
    verifyDataRows(
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
    verifyDataRows(actual, rows(30));
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
        schema("country0", "string"),
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
                " source = %s | where country = 'Canada' OR country = 'England' | inner join left=a, right=b ON a.name = b.name AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4 %s | eval a_name = a.name | eval a_country = a.country| eval b_country = b.country | fields a_name, age, state, a_country, occupation, b_country, salary | left join left=a, right=bON a.a_name = b.name%s  | eval aa_country = a.a_country| eval ab_country = a.b_country | eval bb_country = b.country| fields a_name, age, state, aa_country, occupation, ab_country, salary, bb_country, hobby, language | cross join left=a, right=b %s| eval new_country = a.aa_country| eval new_salary = b.salary | stats avg(new_salary) as avg_salary by span(age, 5) as age_span, state| left semi join left=a, right=bON a.state = b.state %s | eval new_avg_salary = floor(avg_salary)| fields state, age_span, new_avg_salary",
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_STATE_COUNTRY));
  }

  @Test
  public void testMultipleJoinsWithoutTableAliases() {
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
        actual, schema("name", "string"), schema("name0", "string"), schema("name1", "string"));
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
        actual, schema("name", "string"), schema("name0", "string"), schema("name1", "string"));
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
  public void testMultipleJoinsWithSelfJoin1() {
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
        schema("name0", "string"),
        schema("name1", "string"),
        schema("name2", "string"));
    verifyDataRows(
        actual,
        rows("David", "David", "David", "David"),
        rows("David", "David", "David", "David"),
        rows("Hello", "Hello", "Hello", "Hello"),
        rows("Jake", "Jake", "Jake", "Jake"),
        rows("Jane", "Jane", "Jane", "Jane"),
        rows("John", "John", "John", "John"));
  }

  @Ignore // TODO table subquery not support
  public void testMultipleJoinsWithSelfJoin2() {
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
  }

  @Test
  public void testCheckAccessTheReferenceByAliases1() {
    String res1 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields t1.name,"
                    + " t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res2 =
        execute(
            String.format(
                "source = %s as t1 | JOIN ON t1.name = t2.name %s as t2 | fields t1.name, t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res1, res2);

    String res3 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s as tt | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res4 =
        execute(
            String.format(
                "source = %s as tt | JOIN left = t1 right = t2 ON t1.name = t2.name %s | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res5 =
        execute(
            String.format(
                "source = %s as tt | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res3, res4);
    assertEquals(res4, res5);
  }

  @Ignore // TODO table subquery not support
  public void testCheckAccessTheReferenceByAliases2() {
    String res1 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name [ source = %s ] as t2 | fields"
                    + " t1.name, t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res2 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name [ source = %s as t2 ] | fields"
                    + " t1.name, t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res1, res2);

    String res3 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s as"
                    + " tt ] | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res4 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name [ source = %s as tt ] as t2"
                    + " | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res5 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s ]"
                    + " as tt | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res3, res4);
    assertEquals(res4, res5);
  }

  @Test
  public void testCheckAccessTheReferenceByAliases3() {
    String res1 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s as tt | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res2 =
        execute(
            String.format(
                "source = %s as tt | JOIN left = t1 right = t2 ON t1.name = t2.name %s | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res3 =
        execute(
            String.format(
                "source = %s as tt | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res1, res2);
    assertEquals(res1, res3);
  }
}
