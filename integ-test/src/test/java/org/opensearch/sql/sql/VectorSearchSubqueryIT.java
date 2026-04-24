/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

/**
 * Integration tests for vectorSearch() used inside subqueries. Locks in the rejection of outer
 * WHERE on a vectorSearch() subquery, which would otherwise silently yield zero rows because the
 * outer predicate is applied only after the k-NN search has already selected top-k documents by
 * vector distance.
 *
 * <p>Uses _explain-only plus error-path queries, so the k-NN plugin is not required — the planner
 * validation fires during planning, before any k-NN execution.
 */
public class VectorSearchSubqueryIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  private static final String TEST_INDEX = TestsConstants.TEST_INDEX_ACCOUNT;

  @Test
  public void testOuterWhereOnSubqueryRejected() throws IOException {
    // Without the guard the outer predicate is dropped from the pushed DSL and applied only in
    // memory after k-NN returned top-k, which can yield silent zero rows.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT * FROM (SELECT v.firstname, v.state "
                        + "FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t "
                        + "WHERE t.state = 'TX'"));

    assertThat(
        ex.getMessage(),
        containsString("Outer WHERE on a vectorSearch() subquery is not supported"));
    assertThat(ex.getMessage(), containsString("silently yield zero rows"));
  }

  @Test
  public void testOuterWhereOnSubqueryRejectedWithLimit() throws IOException {
    // Same shape with an outer LIMIT — exercises a second planner path (LogicalLimit above
    // LogicalFilter above LogicalProject above scan builder).
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT * FROM (SELECT v.firstname, v.state "
                        + "FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t "
                        + "WHERE t.state = 'TX' "
                        + "LIMIT 3"));

    assertThat(
        ex.getMessage(),
        containsString("Outer WHERE on a vectorSearch() subquery is not supported"));
  }

  @Test
  public void testOuterWhereOnSubqueryRejectedExplain() throws IOException {
    // The guard must fire during planning, before any k-NN execution — so _explain must also
    // return the validation error rather than a silently dropped predicate in the DSL.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                explainQuery(
                    "SELECT * FROM (SELECT v.firstname, v.state "
                        + "FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t "
                        + "WHERE t.state = 'TX'"));

    assertThat(
        ex.getMessage(),
        containsString("Outer WHERE on a vectorSearch() subquery is not supported"));
  }

  @Test
  public void testOuterWhereWithInnerWhereStillRejected() throws IOException {
    // Outer WHERE must be rejected even when the subquery already has its own inner WHERE.
    // The shape reaches the planner as Filter(outer) -> Project -> Filter(inner) -> Scan, and
    // the outer predicate is still separated from the k-NN search by the subquery project
    // boundary. Without preserving the project marker across the inner filter, the walker
    // would miss this shape and the outer predicate would silently produce zero rows.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    "SELECT * FROM (SELECT v.firstname, v.state, v.age "
                        + "FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v "
                        + "WHERE v.age > 10) t "
                        + "WHERE t.state = 'TX'"));

    assertThat(
        ex.getMessage(),
        containsString("Outer WHERE on a vectorSearch() subquery is not supported"));
  }

  @Test
  public void testInnerWhereStillWorks() throws IOException {
    // Positive control: WHERE directly on vectorSearch() inside the subquery must still plan
    // successfully — the rejection is scoped to OUTER filters that cannot reach the push-down
    // contract. We use _explain because the default integ-test cluster has no k-NN plugin.
    String explain =
        explainQuery(
            "SELECT * FROM (SELECT v.firstname, v.state "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v "
                + "WHERE v.state = 'TX') t");

    assertThat(explain, containsString("wrapper"));
    // Inner WHERE should push down, so the state predicate appears in the DSL.
    assertThat(explain, containsString("state"));
  }

  @Test
  public void testInnerWhereWithOuterProjectStillWorks() throws IOException {
    // Another positive control: the outer layer can still project and limit columns from the
    // subquery without the guard firing — only outer WHERE is rejected.
    String explain =
        explainQuery(
            "SELECT t.firstname FROM (SELECT v.firstname, v.state "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v "
                + "WHERE v.state = 'TX') t "
                + "LIMIT 3");

    assertThat(explain, containsString("wrapper"));
  }

  @Test
  public void testSubqueryNoWhereStillWorks() throws IOException {
    // Baseline: a subquery with no WHERE anywhere must not be rejected — the guard fires only
    // when an outer LogicalFilter sits above a subquery project boundary.
    String explain =
        explainQuery(
            "SELECT * FROM (SELECT v.firstname "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t "
                + "LIMIT 3");

    assertThat(explain, containsString("wrapper"));
  }

  @Test
  public void testInnerOrderByScoreDescInSubqueryAllowed() throws IOException {
    // Positive control: inner ORDER BY _score DESC on the vectorSearch() relation inside the
    // subquery is the only supported sort, and must continue to plan successfully even when
    // wrapped in an outer SELECT. Proves the walker does not over-reject sort shapes that are
    // below the subquery Project rather than above it.
    String explain =
        explainQuery(
            "SELECT * FROM (SELECT v.firstname, v._score "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v "
                + "ORDER BY v._score DESC) t "
                + "LIMIT 3");

    assertThat(explain, containsString("wrapper"));
  }

  @Test
  public void testOuterOrderByOnSubqueryRejected() throws IOException {
    // Outer ORDER BY over a vectorSearch() subquery would run on a truncated top-k slice rather
    // than the full relation, silently reordering only the already-ANN-selected rows.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                explainQuery(
                    "SELECT * FROM (SELECT v.firstname, v.state "
                        + "FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t "
                        + "ORDER BY t.state"));

    assertThat(
        ex.getMessage(),
        containsString("Outer ORDER BY on a vectorSearch() subquery is not supported"));
  }

  @Test
  public void testOuterOffsetOnSubqueryRejected() throws IOException {
    // Outer OFFSET silently drops top-k rows by vector distance. The inner query already caps at
    // k and any outer OFFSET shifts that window in an opaque way, so reject it.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                explainQuery(
                    "SELECT * FROM (SELECT v.firstname "
                        + "FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t "
                        + "LIMIT 3 OFFSET 2"));

    assertThat(
        ex.getMessage(),
        containsString("Outer OFFSET on a vectorSearch() subquery is not supported"));
  }

  @Test
  public void testOuterLimitWithoutOffsetOnSubqueryAllowed() throws IOException {
    // Positive control: outer LIMIT without OFFSET just caps the row count and must plan without
    // error. Locks in the offset==0 boundary of the OFFSET rejection.
    String explain =
        explainQuery(
            "SELECT * FROM (SELECT v.firstname "
                + "FROM vectorSearch(table='"
                + TEST_INDEX
                + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t "
                + "LIMIT 3");

    assertThat(explain, containsString("wrapper"));
  }

  @Test
  public void testOuterAggregationOnSubqueryRejected() throws IOException {
    // Outer aggregation (here COUNT(*)) over a vectorSearch() subquery would run on the
    // truncated top-k slice, producing a count that silently depends on k rather than the full
    // population. vectorSearch() does not support aggregations, so reject the outer-subquery
    // variant with the same subquery-boundary walker that catches outer WHERE.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                explainQuery(
                    "SELECT COUNT(*) FROM (SELECT v.firstname "
                        + "FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t"));

    assertThat(
        ex.getMessage(),
        containsString(
            "Outer GROUP BY / aggregation / DISTINCT on a vectorSearch() subquery is not"
                + " supported"));
  }

  @Test
  public void testOuterGroupByOnSubqueryRejected() throws IOException {
    // GROUP BY rewrites to LogicalAggregation and is caught by the same subquery-boundary walker.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                explainQuery(
                    "SELECT t.state, COUNT(*) FROM (SELECT v.firstname, v.state "
                        + "FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t "
                        + "GROUP BY t.state"));

    assertThat(
        ex.getMessage(),
        containsString(
            "Outer GROUP BY / aggregation / DISTINCT on a vectorSearch() subquery is not"
                + " supported"));
  }

  @Test
  public void testOuterDistinctOnSubqueryRejected() throws IOException {
    // SELECT DISTINCT rewrites to a LogicalAggregation with empty aggregator list and the select
    // items as the group-by list. The subquery-boundary walker must catch this shape too.
    ResponseException ex =
        expectThrows(
            ResponseException.class,
            () ->
                explainQuery(
                    "SELECT DISTINCT t.state FROM (SELECT v.firstname, v.state "
                        + "FROM vectorSearch(table='"
                        + TEST_INDEX
                        + "', field='embedding', vector='[1.0, 2.0]', option='k=5') AS v) t"));

    assertThat(
        ex.getMessage(),
        containsString(
            "Outer GROUP BY / aggregation / DISTINCT on a vectorSearch() subquery is not"
                + " supported"));
  }
}
