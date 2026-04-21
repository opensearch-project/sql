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
}
