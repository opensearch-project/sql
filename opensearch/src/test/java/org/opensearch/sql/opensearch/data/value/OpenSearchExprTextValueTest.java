/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.value;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT;

import org.junit.jupiter.api.Test;

class OpenSearchExprTextValueTest {
  @Test
  public void typeOfExprTextValue() {
    assertEquals(OPENSEARCH_TEXT, new OpenSearchExprTextValue("A").type());
  }
}
