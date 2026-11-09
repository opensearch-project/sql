/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchParseException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.exc.StreamReadException;
import tools.jackson.databind.JsonNode;

public class OpenSearchJsonContentTest {
  @Test
  public void testGetValueWithIOException() {
    JsonNode jsonNode = mock(JsonNode.class);
    JsonParser jsonParser = mock(JsonParser.class);
    when(jsonNode.traverse(ObjectReadContext.empty())).thenReturn(jsonParser);
    when(jsonParser.nextToken()).thenThrow(new StreamReadException("Simulated"));
    OpenSearchJsonContent content = new OpenSearchJsonContent(jsonNode);
    OpenSearchParseException exception =
        assertThrows(OpenSearchParseException.class, content::geoValue);
    assertTrue(exception.getMessage().contains("error parsing geo point"));
  }
}
