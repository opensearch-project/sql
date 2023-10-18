/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;

@Data
public class StatementId {
  private final String id;

  public static StatementId newStatementId() {
    return new StatementId(RandomStringUtils.randomAlphanumeric(16));
  }

  @Override
  public String toString() {
    return "statementId=" + id;
  }
}
