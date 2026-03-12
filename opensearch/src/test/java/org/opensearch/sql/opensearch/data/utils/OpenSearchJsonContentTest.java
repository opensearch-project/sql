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
import com.fasterxml.jackson.databind.node.JsonNodeType;
import java.io.IOException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
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

  @Test
  public void testGeoValueWithIOExceptionAndDebugEnabled() throws IOException {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(OpenSearchJsonContent.class.getName());
    Level previousLevel = loggerConfig.getLevel();
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();
    try {
      JsonNode jsonNode = mock(JsonNode.class);
      JsonParser jsonParser = mock(JsonParser.class);
      when(jsonNode.traverse()).thenReturn(jsonParser);
      when(jsonParser.nextToken()).thenThrow(new IOException());
      OpenSearchJsonContent content = new OpenSearchJsonContent(jsonNode);
      assertThrows(OpenSearchParseException.class, content::geoValue);
    } finally {
      loggerConfig.setLevel(previousLevel);
      ctx.updateLoggers();
    }
  }

  @Test
  public void testLongValueWithInvalidNodeAndDebugEnabled() {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(OpenSearchJsonContent.class.getName());
    Level previousLevel = loggerConfig.getLevel();
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();
    try {
      JsonNode jsonNode = mock(JsonNode.class);
      when(jsonNode.isNumber()).thenReturn(false);
      when(jsonNode.isTextual()).thenReturn(false);
      when(jsonNode.getNodeType()).thenReturn(JsonNodeType.ARRAY);
      OpenSearchJsonContent content = new OpenSearchJsonContent(jsonNode);
      assertThrows(OpenSearchParseException.class, content::longValue);
    } finally {
      loggerConfig.setLevel(previousLevel);
      ctx.updateLoggers();
    }
  }

  @Test
  public void testDoubleValueWithInvalidNodeAndDebugEnabled() {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(OpenSearchJsonContent.class.getName());
    Level previousLevel = loggerConfig.getLevel();
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();
    try {
      JsonNode jsonNode = mock(JsonNode.class);
      when(jsonNode.isNumber()).thenReturn(false);
      when(jsonNode.isTextual()).thenReturn(false);
      when(jsonNode.getNodeType()).thenReturn(JsonNodeType.OBJECT);
      OpenSearchJsonContent content = new OpenSearchJsonContent(jsonNode);
      assertThrows(OpenSearchParseException.class, content::doubleValue);
    } finally {
      loggerConfig.setLevel(previousLevel);
      ctx.updateLoggers();
    }
  }

  @Test
  public void testBooleanValueWithInvalidNodeAndDebugEnabled() {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(OpenSearchJsonContent.class.getName());
    Level previousLevel = loggerConfig.getLevel();
    loggerConfig.setLevel(Level.DEBUG);
    ctx.updateLoggers();
    try {
      JsonNode jsonNode = mock(JsonNode.class);
      when(jsonNode.isBoolean()).thenReturn(false);
      when(jsonNode.isTextual()).thenReturn(false);
      when(jsonNode.getNodeType()).thenReturn(JsonNodeType.ARRAY);
      OpenSearchJsonContent content = new OpenSearchJsonContent(jsonNode);
      assertThrows(OpenSearchParseException.class, content::booleanValue);
    } finally {
      loggerConfig.setLevel(previousLevel);
      ctx.updateLoggers();
    }
  }

  @Test
  public void testLongValueWithInvalidNode() {
    JsonNode jsonNode = mock(JsonNode.class);
    when(jsonNode.isNumber()).thenReturn(false);
    when(jsonNode.isTextual()).thenReturn(false);
    when(jsonNode.getNodeType()).thenReturn(JsonNodeType.ARRAY);
    OpenSearchJsonContent content = new OpenSearchJsonContent(jsonNode);
    OpenSearchParseException exception =
        assertThrows(OpenSearchParseException.class, content::longValue);
    assertTrue(exception.getMessage().contains("node must be a number"));
    assertTrue(exception.getMessage().contains("ARRAY"));
  }

  @Test
  public void testDoubleValueWithInvalidNode() {
    JsonNode jsonNode = mock(JsonNode.class);
    when(jsonNode.isNumber()).thenReturn(false);
    when(jsonNode.isTextual()).thenReturn(false);
    when(jsonNode.getNodeType()).thenReturn(JsonNodeType.OBJECT);
    OpenSearchJsonContent content = new OpenSearchJsonContent(jsonNode);
    OpenSearchParseException exception =
        assertThrows(OpenSearchParseException.class, content::doubleValue);
    assertTrue(exception.getMessage().contains("node must be a number"));
    assertTrue(exception.getMessage().contains("OBJECT"));
  }

  @Test
  public void testBooleanValueWithInvalidNode() {
    JsonNode jsonNode = mock(JsonNode.class);
    when(jsonNode.isBoolean()).thenReturn(false);
    when(jsonNode.isTextual()).thenReturn(false);
    when(jsonNode.getNodeType()).thenReturn(JsonNodeType.ARRAY);
    OpenSearchJsonContent content = new OpenSearchJsonContent(jsonNode);
    OpenSearchParseException exception =
        assertThrows(OpenSearchParseException.class, content::booleanValue);
    assertTrue(exception.getMessage().contains("node must be a boolean"));
    assertTrue(exception.getMessage().contains("ARRAY"));
  }
}
