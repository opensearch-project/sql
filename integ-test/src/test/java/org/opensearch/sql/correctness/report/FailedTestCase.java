/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.report;

import static org.opensearch.sql.correctness.report.TestCaseReport.TestResult.FAILURE;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.correctness.runner.resultset.DBResult;

/** Report for test case that fails due to inconsistent result set. */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Getter
public class FailedTestCase extends TestCaseReport {

  /** Inconsistent result sets for reporting */
  private final List<DBResult> resultSets;

  /** Explain where the difference is caused the test failure. */
  private final String explain;

  /** Errors occurred for partial other databases. */
  private final String errors;

  public FailedTestCase(int id, String sql, List<DBResult> resultSets, String errors) {
    super(id, sql, FAILURE);
    this.resultSets = resultSets;
    this.resultSets.sort(Comparator.comparing(DBResult::getDatabaseName));
    this.errors = errors;

    // Generate explanation by diff the first result with remaining
    this.explain =
        resultSets.subList(1, resultSets.size()).stream()
            .map(result -> resultSets.get(0).diff(result))
            .collect(Collectors.joining(", "));
  }
}
