/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.report;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Test summary section. */
@EqualsAndHashCode
@ToString
@Getter
public class TestSummary {

  private int total;

  private int success;

  private int failure;

  public void addSuccess() {
    success++;
    total++;
  }

  public void addFailure() {
    failure++;
    total++;
  }
}
