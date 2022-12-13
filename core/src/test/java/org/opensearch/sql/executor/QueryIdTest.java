/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
