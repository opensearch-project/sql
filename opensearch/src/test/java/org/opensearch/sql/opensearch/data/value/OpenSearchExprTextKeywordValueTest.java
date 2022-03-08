/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import org.junit.jupiter.api.Test;

class OpenSearchExprTextKeywordValueTest {

  @Test
  public void testTypeOfExprTextKeywordValue() {
    assertEquals(OPENSEARCH_TEXT_KEYWORD, new OpenSearchExprTextKeywordValue("A").type());
  }

}
