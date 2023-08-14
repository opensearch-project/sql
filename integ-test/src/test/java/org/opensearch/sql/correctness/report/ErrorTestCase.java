/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.report;

import static org.opensearch.sql.correctness.report.TestCaseReport.TestResult.FAILURE;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Report for test case that ends with an error. */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Getter
public class ErrorTestCase extends TestCaseReport {

  /** Root cause of the error */
  private final String reason;

  public ErrorTestCase(int id, String sql, String reason) {
    super(id, sql, FAILURE);
    this.reason = reason;
  }
}
