/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.report;

import static org.opensearch.sql.correctness.report.TestCaseReport.TestResult.SUCCESS;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Report for successful test case result. */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Getter
public class SuccessTestCase extends TestCaseReport {

  public SuccessTestCase(int id, String sql) {
    super(id, sql, SUCCESS);
  }
}
