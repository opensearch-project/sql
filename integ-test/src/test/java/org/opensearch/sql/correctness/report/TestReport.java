/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.report;

import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Test report class to generate JSON report. */
@EqualsAndHashCode
@ToString
@Getter
public class TestReport {

  private final TestSummary summary = new TestSummary();

  private final List<TestCaseReport> tests = new ArrayList<>();

  /**
   * Add a test case report to the whole report.
   *
   * @param testCase report for a single test case
   */
  public void addTestCase(TestCaseReport testCase) {
    tests.add(testCase);
    if ("Success".equals(testCase.getResult())) {
      summary.addSuccess();
    } else {
      summary.addFailure();
    }
  }
}
