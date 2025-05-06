/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.OpenSearchParseException;

public class OpenSearchJsonContentTest {
  @Test
  public void testGetValueWithIOException() throws IOException {
    JsonNode jsonNode = mock(JsonNode.class);
    JsonParser jsonParser = mock(JsonParser.class);
    when(jsonNode.traverse()).thenReturn(jsonParser);
    when(jsonParser.nextToken()).thenThrow(new IOException());
    OpenSearchJsonContent content = new OpenSearchJsonContent(jsonNode);
    OpenSearchParseException exception =
        assertThrows(OpenSearchParseException.class, content::geoValue);
    assertTrue(exception.getMessage().contains("error parsing geo point"));
  }
}
