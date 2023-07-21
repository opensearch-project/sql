/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QueryManagerTest {

  @Mock private QueryId queryId;

  private QueryManager queryManager =
      id -> {
        throw new UnsupportedOperationException();
      };

  @Test
  void cancelIsNotSupportedByDefault() {
    assertThrows(UnsupportedOperationException.class, () -> queryManager.cancel(queryId));
  }
}
