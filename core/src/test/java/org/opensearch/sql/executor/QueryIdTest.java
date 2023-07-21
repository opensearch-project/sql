/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.google.common.base.Strings;
import org.junit.jupiter.api.Test;

class QueryIdTest {

  @Test
  public void createQueryId() {
    assertFalse(Strings.isNullOrEmpty(QueryId.queryId().getQueryId()));
  }
}
