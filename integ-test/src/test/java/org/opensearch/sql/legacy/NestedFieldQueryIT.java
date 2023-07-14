/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.opensearch.sql.util.MatcherUtils.hitAll;
import static org.opensearch.sql.util.MatcherUtils.kvString;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.function.Function;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.core.Is;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.ResponseException;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchHit;

/**
 * Integration test cases for both rewriting and projection logic.
 * <p>
 * Test result:
 * 1) SELECT * or any field or aggregate function on any field
 * 2) WHERE single or multiple conditions on nested type
 * 3) GROUP BY regular field and aggregate on any field
 * 4) GROUP BY nested field and COUNT(*)
 * 5) UNION/MINUS but only SELECT nested field
 * <p>
 * Does NOT support:
 * 1) GROUP BY nested field and aggregate other than COUNT
 * 2) UNION/MINUS and SELECT nested field (need to flatten during set computation)
 * 3) JOIN (doesn't work if put alias before nested field name and may have similar problem as UNION/MINUS during computation)
 * 4) Subquery
 * 5) HAVING
 * 6) Verification for conditions mixed with regular and nested fields
 */
public class NestedFieldQueryIT extends SQLIntegTestCase {

  private static final String FROM =
      "FROM " + TestsConstants.TEST_INDEX_NESTED_TYPE + " n, n.message m";


  @Override
  protected void init() throws Exception {
    loadIndex(Index.NESTED);
    loadIndex(Index.EMPLOYEE_NESTED);
  }

  @Test
  public void selectAll() throws IOException {
    queryAll("SELECT *");
  }

  @Test
  public void noCondition() throws IOException {
    queryAll("SELECT myNum, someField, m.author, m.info");
  }

  private void queryAll(String sql) throws IOException {
    assertThat(
        query(sql),
        hits(
            hit(
                myNum(1),
                someField("b"),
                innerHits("message",
                    hit(
                        author("e"),
                        info("a")
                    )
                )
            ),
            hit(
                myNum(2),
                someField("a"),
                innerHits("message",
                    hit(
                        author("f"),
                        info("b")
                    )
                )
            ),
            hit(
                myNum(3),
                someField("a"),
                innerHits("message",
                    hit(
                        author("g"),
                        info("c")
                    )
                )
            ),
            hit(
                myNum(4),
                someField("b"),
                innerHits("message",
                    hit(
                        author("h"),
                        info("c")
                    ),
                    hit(
                        author("i"),
                        info("a")
                    )
                )
            ),
            hit(
                myNum(new int[] {3, 4}),
                someField("a"),
                innerHits("message",
                    hit(
                        author("zz"),
                        info("zz")
                    )
                )
            )
        )
    );
  }

  @Test
  public void singleCondition() throws IOException {
    assertThat(
        query(
            "SELECT myNum, m.author, m.info",
            "WHERE m.info = 'c'"
        ),
        hits(
            hit(
                myNum(3),
                innerHits("message",
                    hit(
                        author("g"),
                        info("c")
                    )
                )
            ),
            hit(
                myNum(4),
                innerHits("message",
                    hit(
                        author("h"),
                        info("c")
                    )
                )
            )
        )
    );
  }

  @Test
  public void multipleConditionsOfNestedField() throws IOException {
    assertThat(
        query(
            "SELECT someField, m.author, m.info",
            "WHERE m.info = 'c' AND m.author = 'h'"
        ),
        hits(
            hit(
                someField("b"),
                innerHits("message",
                    hit(
                        author("h"),
                        info("c")
                    )
                )
            )
        )
    );
  }

  @Test
  public void multipleConditionsOfNestedFieldNoMatch() throws IOException {
    assertThat(
        query(
            "SELECT someField, m.author, m.info",
            "WHERE m.info = 'c' AND m.author = 'i'"
        ),
        hits()
    );
  }

  @Test
  public void multipleConditionsOfRegularAndNestedField() throws IOException {
    assertThat(
        query(
            "SELECT myNum, m.author, m.info",
            "WHERE myNum = 3 AND m.info = 'c'"
        ),
        hits(
            hit(
                myNum(3),
                innerHits("message",
                    hit(
                        author("g"),
                        info("c")
                    )
                )
            )
        )
    );
  }

  @Test
  public void multipleConditionsOfRegularOrNestedField() throws IOException {
    assertThat(
        query(
            "SELECT myNum, m.author, m.info",
            "WHERE myNum = 2 OR m.info = 'c'"
        ),
        hits(
            hit(
                myNum(2)
            ), // Note: no inner hit here because of no match in nested field
            hit(
                myNum(3),
                innerHits("message",
                    hit(
                        author("g"),
                        info("c")
                    )
                )
            ),
            hit(
                myNum(4),
                innerHits("message",
                    hit(
                        author("h"),
                        info("c")
                    )
                )
            )
        )
    );
  }

  @Test
  public void leftJoinSelectAll() throws IOException {
    String sql = "SELECT * " +
        "FROM opensearch-sql_test_index_employee_nested e " +
        "LEFT JOIN e.projects p";
    String explain = explainQuery(sql);
    assertThat(explain, containsString("{\"bool\":{\"must_not\":[{\"nested\":{\"query\":" +
        "{\"exists\":{\"field\":\"projects\",\"boost\":1.0}},\"path\":\"projects\""));

    assertThat(explain, containsString("\"_source\":{\"includes\":[\"projects.*\""));

    JSONObject results = executeQuery(sql);
    Assert.assertThat(getTotalHits(results), equalTo(4));
  }

  @Test
  public void leftJoinSpecificFields() throws IOException {
    String sql = "SELECT e.name, p.name, p.started_year " +
        "FROM opensearch-sql_test_index_employee_nested e " +
        "LEFT JOIN e.projects p";
    String explain = explainQuery(sql);
    assertThat(explain, containsString("{\"bool\":{\"must_not\":[{\"nested\":{\"query\":" +
        "{\"exists\":{\"field\":\"projects\",\"boost\":1.0}},\"path\":\"projects\""));
    assertThat(explain, containsString("\"_source\":{\"includes\":[\"name\"],"));
    assertThat(explain,
        containsString("\"_source\":{\"includes\":[\"projects.name\",\"projects.started_year\"]"));

    JSONObject results = executeQuery(sql);
    Assert.assertThat(getTotalHits(results), equalTo(4));
  }

  @Ignore("Comma join in left join won't pass syntax check in new ANTLR parser. "
      + "Ignore for now and require to change grammar too when we want to support this case.")
  @Test
  public void leftJoinExceptionOnExtraNestedFields() throws IOException {
    String sql = "SELECT * " +
        "FROM opensearch-sql_test_index_employee_nested e " +
        "LEFT JOIN e.projects p, e.comments c";

    try {
      String explain = explainQuery(sql);
      Assert.fail("Expected ResponseException, but none was thrown");
    } catch (ResponseException e) {
      assertThat(e.getResponse().getStatusLine().getStatusCode(),
          equalTo(RestStatus.BAD_REQUEST.getStatus()));
      final String entity = TestUtils.getResponseBody(e.getResponse());
      assertThat(entity,
          containsString("only single nested field is allowed as right table for LEFT JOIN"));
      assertThat(entity, containsString("\"type\":\"verification_exception\""));
    }
  }


  @Test
  public void aggregationWithoutGroupBy() throws IOException {
    String sql = "SELECT AVG(m.dayOfWeek) AS avgDay " + FROM;

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "message.dayOfWeek@NESTED");

    Assert.assertThat(((BigDecimal) aggregation.query("/avgDay/value")).doubleValue(), closeTo(3.166666666, 0.01));
  }

  @Test
  public void groupByNestedFieldAndCount() throws IOException {
    final String sql = "SELECT m.info, COUNT(*) " + FROM + " GROUP BY m.info";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "message.info@NESTED");
    JSONArray msgInfoBuckets = (JSONArray) aggregation.optQuery("/message.info/buckets");

    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(4));
    Assert.assertThat(msgInfoBuckets.query("/0/key"), equalTo("a"));
    Assert.assertThat(msgInfoBuckets.query("/0/COUNT(*)/value"), equalTo(2));
    Assert.assertThat(msgInfoBuckets.query("/1/key"), equalTo("c"));
    Assert.assertThat(msgInfoBuckets.query("/1/COUNT(*)/value"), equalTo(2));
    Assert.assertThat(msgInfoBuckets.query("/2/key"), equalTo("b"));
    Assert.assertThat(msgInfoBuckets.query("/2/COUNT(*)/value"), equalTo(1));
    Assert.assertThat(msgInfoBuckets.query("/3/key"), equalTo("zz"));
    Assert.assertThat(msgInfoBuckets.query("/3/COUNT(*)/value"), equalTo(1));
  }

  @Test
  public void groupByRegularFieldAndSum() throws IOException {
    final String sql = "SELECT *, SUM(m.dayOfWeek) AS sumDay " + FROM + " GROUP BY someField";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "someField");
    JSONArray msgInfoBuckets = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(2));
    Assert.assertThat(msgInfoBuckets.query("/0/key"), equalTo("a"));
    Assert.assertThat(((BigDecimal) msgInfoBuckets.query("/0/message.dayOfWeek@NESTED/sumDay/value")).doubleValue(),
        closeTo(9.0, 0.01));
    Assert.assertThat(msgInfoBuckets.query("/1/key"), equalTo("b"));
    Assert.assertThat(((BigDecimal) msgInfoBuckets.query("/1/message.dayOfWeek@NESTED/sumDay/value")).doubleValue(),
        closeTo(10.0, 0.01));
  }

  @Test
  public void nestedFiledIsNotNull() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested as e, e.projects as p " +
        "WHERE p IS NOT NULL";

    assertThat(
        executeQuery(sql),
        hitAll(
            kvString("/_source/name", Is.is("Bob Smith")),
            kvString("/_source/name", Is.is("Jane Smith"))
        )
    );
  }

  // Doesn't support: aggregate function other than COUNT()
  @SuppressWarnings("unused")
  public void groupByNestedFieldAndAvg() throws IOException {
    query(
        "SELECT m.info, AVG(m.dayOfWeek)",
        "GROUP BY m.info"
    );
    query(
        "SELECT m.info, AVG(myNum)",
        "GROUP BY m.info"
    );
  }

  @Test
  public void groupByNestedAndRegularField() throws IOException {
    final String sql = "SELECT someField, m.info, COUNT(*) " + FROM + " GROUP BY someField, m.info";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "someField");
    JSONArray msgInfoBuckets = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(msgInfoBuckets);
    Assert.assertThat(msgInfoBuckets.length(), equalTo(2));
    Assert.assertThat(msgInfoBuckets.query("/0/key"), equalTo("a"));
    Assert.assertThat(msgInfoBuckets.query("/1/key"), equalTo("b"));

    JSONArray innerBuckets =
        (JSONArray) msgInfoBuckets.optQuery("/0/message.info@NESTED/message.info/buckets");
    Assert.assertThat(innerBuckets.query("/0/key"), equalTo("b"));
    Assert.assertThat(innerBuckets.query("/0/COUNT(*)/value"), equalTo(1));
    Assert.assertThat(innerBuckets.query("/1/key"), equalTo("c"));
    Assert.assertThat(innerBuckets.query("/1/COUNT(*)/value"), equalTo(1));
    Assert.assertThat(innerBuckets.query("/2/key"), equalTo("zz"));
    Assert.assertThat(innerBuckets.query("/2/COUNT(*)/value"), equalTo(1));

    innerBuckets =
        (JSONArray) msgInfoBuckets.optQuery("/1/message.info@NESTED/message.info/buckets");
    Assert.assertThat(innerBuckets.query("/0/key"), equalTo("a"));
    Assert.assertThat(innerBuckets.query("/0/COUNT(*)/value"), equalTo(2));
    Assert.assertThat(innerBuckets.query("/1/key"), equalTo("c"));
    Assert.assertThat(innerBuckets.query("/1/COUNT(*)/value"), equalTo(1));
  }

  @Test
  public void countAggWithoutWhere() throws IOException {
    String sql = "SELECT e.name, COUNT(p) as c " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "GROUP BY e.name " +
        "HAVING c > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/c/value"), equalTo(3));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/c/value"), equalTo(2));
  }

  @Test
  public void countAggWithWhereOnParent() throws IOException {
    String sql = "SELECT e.name, COUNT(p) as c " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE e.name like '%smith%' " +
        "GROUP BY e.name " +
        "HAVING c > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/c/value"), equalTo(3));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/c/value"), equalTo(2));
  }

  @Test
  public void countAggWithWhereOnNested() throws IOException {
    String sql = "SELECT e.name, COUNT(p) as c " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING c > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/projects@FILTER/c/value"), equalTo(2));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/projects@FILTER/c/value"), equalTo(2));
  }

  @Test
  public void countAggWithWhereOnParentOrNested() throws IOException {
    String sql = "SELECT e.name, COUNT(p) as c " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE e.name like '%smith%' or p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING c > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/c/value"), equalTo(3));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/c/value"), equalTo(2));
  }

  @Test
  public void countAggWithWhereOnParentAndNested() throws IOException {
    String sql = "SELECT e.name, COUNT(p) as c " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE e.name like '%smith%' AND p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING c > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/projects@FILTER/c/value"), equalTo(2));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/projects@FILTER/c/value"), equalTo(2));
  }

  @Test
  public void countAggWithWhereOnNestedAndNested() throws IOException {
    String sql = "SELECT e.name, COUNT(p) as c " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.started_year > 2000 AND p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING c > 0";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/projects@FILTER/c/value"), equalTo(1));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/projects@FILTER/c/value"), equalTo(1));
  }

  @Test
  public void countAggWithWhereOnNestedOrNested() throws IOException {
    String sql = "SELECT e.name, COUNT(p) as c " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.started_year > 2000 OR p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING c > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/projects@FILTER/c/value"), equalTo(2));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/projects@FILTER/c/value"), equalTo(2));
  }

  @Test
  public void countAggOnNestedInnerFieldWithoutWhere() throws IOException {
    String sql = "SELECT e.name, COUNT(p.started_year) as count " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING count > 0";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(
        bucket.query("/0/projects.started_year@NESTED/projects.started_year@FILTER/count/value"),
        equalTo(2));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(
        bucket.query("/1/projects.started_year@NESTED/projects.started_year@FILTER/count/value"),
        equalTo(2));
  }

  @Test
  public void maxAggOnNestedInnerFieldWithoutWhere() throws IOException {
    String sql = "SELECT e.name, MAX(p.started_year) as max " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.name LIKE '%security%' " +
        "GROUP BY e.name";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(
        ((BigDecimal) bucket.query("/0/projects.started_year@NESTED/projects.started_year@FILTER/max/value")).doubleValue(),
        closeTo(2015.0, 0.01));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(
        ((BigDecimal) bucket.query("/1/projects.started_year@NESTED/projects.started_year@FILTER/max/value")).doubleValue(),
        closeTo(2015.0, 0.01));
  }

  @Test
  public void havingCountAggWithoutWhere() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "GROUP BY e.name " +
        "HAVING COUNT(p) > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/count_0/value"), equalTo(3));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/count_0/value"), equalTo(2));
  }

  @Test
  public void havingCountAggWithWhereOnParent() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE e.name like '%smith%' " +
        "GROUP BY e.name " +
        "HAVING COUNT(p) > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/count_0/value"), equalTo(3));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/count_0/value"), equalTo(2));
  }

  @Test
  public void havingCountAggWithWhereOnNested() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING COUNT(p) > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/projects@FILTER/count_0/value"), equalTo(2));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/projects@FILTER/count_0/value"), equalTo(2));
  }

  @Test
  public void havingCountAggWithWhereOnParentOrNested() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE e.name like '%smith%' or p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING COUNT(p) > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/count_0/value"), equalTo(3));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/count_0/value"), equalTo(2));
  }

  @Test
  public void havingCountAggWithWhereOnParentAndNested() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE e.name like '%smith%' AND p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING COUNT(p) > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/projects@FILTER/count_0/value"), equalTo(2));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/projects@FILTER/count_0/value"), equalTo(2));
  }

  @Test
  public void havingCountAggWithWhereOnNestedAndNested() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.started_year > 2000 AND p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING COUNT(p) > 0";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/projects@FILTER/count_0/value"), equalTo(1));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/projects@FILTER/count_0/value"), equalTo(1));
  }

  @Test
  public void havingCountAggWithWhereOnNestedOrNested() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.started_year > 2000 OR p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING COUNT(p) > 1";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(bucket.query("/0/projects@NESTED/projects@FILTER/count_0/value"), equalTo(2));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(bucket.query("/1/projects@NESTED/projects@FILTER/count_0/value"), equalTo(2));
  }

  @Test
  public void havingCountAggOnNestedInnerFieldWithoutWhere() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING COUNT(p.started_year) > 0";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(
        bucket.query("/0/projects.started_year@NESTED/projects.started_year@FILTER/count_0/value"),
        equalTo(2));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(
        bucket.query("/1/projects.started_year@NESTED/projects.started_year@FILTER/count_0/value"),
        equalTo(2));
  }

  @Test
  public void havingMaxAggOnNestedInnerFieldWithoutWhere() throws IOException {
    String sql = "SELECT e.name " +
        "FROM opensearch-sql_test_index_employee_nested AS e, e.projects AS p " +
        "WHERE p.name LIKE '%security%' " +
        "GROUP BY e.name " +
        "HAVING MAX(p.started_year) > 1990";

    JSONObject result = executeQuery(sql);
    JSONObject aggregation = getAggregation(result, "name.keyword");
    JSONArray bucket = (JSONArray) aggregation.optQuery("/buckets");

    Assert.assertNotNull(bucket);
    Assert.assertThat(bucket.length(), equalTo(2));
    Assert.assertThat(bucket.query("/0/key"), equalTo("Bob Smith"));
    Assert.assertThat(
        ((BigDecimal) bucket.query("/0/projects.started_year@NESTED/projects.started_year@FILTER/max_0/value")).doubleValue(),
        closeTo(2015.0, 0.01));
    Assert.assertThat(bucket.query("/1/key"), equalTo("Jane Smith"));
    Assert.assertThat(
        ((BigDecimal) bucket.query("/1/projects.started_year@NESTED/projects.started_year@FILTER/max_0/value")).doubleValue(),
        closeTo(2015.0, 0.01));
  }

  /***********************************************************
   Matchers for Non-Aggregation Testing
   ***********************************************************/

  @SafeVarargs
  private final Matcher<SearchResponse> hits(Matcher<SearchHit>... subMatchers) {
    return featureValueOf("hits", arrayContainingInAnyOrder(subMatchers),
        resp -> resp.getHits().getHits());
  }

  @SafeVarargs
  private final Matcher<SearchHit> hit(Matcher<SearchHit>... subMatchers) {
    return allOf(subMatchers);
  }

  private Matcher<SearchHit> myNum(int value) {
    return kv("myNum", is(value));
  }

  private Matcher<SearchHit> myNum(int[] values) {

    return new BaseMatcher<SearchHit>() {

      @Override
      public boolean matches(Object item) {

        if (item instanceof SearchHit) {
          final SearchHit hit = (SearchHit) item;
          ArrayList<Integer> actualValues = (ArrayList<Integer>) hit.getSourceAsMap().get("myNum");

          if (actualValues.size() != values.length) {
            return false;
          }
          for (int i = 0; i < values.length; ++i) {
            if (values[i] != actualValues.get(i)) {
              return false;
            }
          }
          return true;
        }

        return false;
      }

      @Override
      public void describeTo(Description description) {
      }
    };
  }

  private Matcher<SearchHit> someField(String value) {
    return kv("someField", is(value));
  }

  private Matcher<SearchHit> author(String value) {
    return kv("author", is(value));
  }

  private Matcher<SearchHit> info(String value) {
    return kv("info", is(value));
  }

  private Matcher<SearchHit> kv(String key, Matcher<Object> valMatcher) {
    return featureValueOf(key, valMatcher, hit -> hit.getSourceAsMap().get(key));
  }

  @SafeVarargs
  private final Matcher<SearchHit> innerHits(String path, Matcher<SearchHit>... innerHitMatchers) {
    return featureValueOf(
        "innerHits",
        arrayContainingInAnyOrder(innerHitMatchers),
        hit -> hit.getInnerHits().get(path).getHits()
    );
  }

  /***********************************************************
   Matchers for Aggregation Testing
   ***********************************************************/

  private <T, U> FeatureMatcher<T, U> featureValueOf(String name, Matcher<U> subMatcher,
                                                     Function<T, U> getter) {
    return new FeatureMatcher<T, U>(subMatcher, name, name) {
      @Override
      protected U featureValueOf(T actual) {
        return getter.apply(actual);
      }
    };
  }

  /***********************************************************
   Query Utility to Fetch Response for SQL
   ***********************************************************/

  private SearchResponse query(String select, String... statements) throws IOException {
    return execute(select + " " + FROM + " " + String.join(" ", statements));
  }

  private SearchResponse execute(String sql) throws IOException {
    final JSONObject jsonObject = executeQuery(sql);

    final XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
        NamedXContentRegistry.EMPTY,
        LoggingDeprecationHandler.INSTANCE,
        jsonObject.toString());
    return SearchResponse.fromXContent(parser);
  }

  private JSONObject getAggregation(final JSONObject queryResult, final String aggregationName) {
    final String aggregationsObjName = "aggregations";
    Assert.assertTrue(queryResult.has(aggregationsObjName));

    final JSONObject aggregations = queryResult.getJSONObject(aggregationsObjName);
    Assert.assertTrue(aggregations.has(aggregationName));
    return aggregations.getJSONObject(aggregationName);
  }

}
