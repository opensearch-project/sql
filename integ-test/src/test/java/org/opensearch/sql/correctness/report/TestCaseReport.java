/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.report;

import static org.opensearch.sql.correctness.report.TestCaseReport.TestResult.SUCCESS;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Base class for different test result. */
@EqualsAndHashCode
@ToString
public abstract class TestCaseReport {

  public enum TestResult {
    SUCCESS,
    FAILURE;
  }

  @Getter private final int id;

  @Getter private final String sql;

  private final TestResult result;

  public TestCaseReport(int id, String sql, TestResult result) {
    this.id = id;
    this.sql = sql;
    this.result = result;
  }

  public String getResult() {
    return result == SUCCESS ? "Success" : "Failed";
  }
}
