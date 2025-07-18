/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.opensearch.storage.script.CompoundedScriptEngine.ScriptEngineType.V2;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.storage.serde.SerializationWrapper.LangScriptWrapper;

public class SerializationWrapperTest {

  private static final String script = "VGVzdA=="; // base64 encoded "Test"

  @Test
  public void testWrapAndUnwrapValidScript() {
    String wrapped = SerializationWrapper.wrapWithLangType(V2, script);
    assertNotNull(wrapped);
    assertTrue(wrapped.contains(SerializationWrapper.LANG_TYPE));
    assertTrue(wrapped.contains(SerializationWrapper.SCRIPT));

    LangScriptWrapper unwrapped = SerializationWrapper.unwrapLangType(wrapped);
    assertEquals(V2, unwrapped.langType);
    assertEquals(script, unwrapped.script);
  }

  @Test
  public void testUnwrapWithMissingLangTypeThrowsException() {
    String invalidJson = "{\"script\": \"code...\"}";
    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> SerializationWrapper.unwrapLangType(invalidJson));
    assertTrue(exception.getMessage().contains("Missing required fields"));
  }

  @Test
  public void testUnwrapWithMissingScriptThrowsException() {
    String invalidJson = "{\"langType\": \"v2\"}";
    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> SerializationWrapper.unwrapLangType(invalidJson));
    assertTrue(exception.getMessage().contains("Missing required fields"));
  }

  @Test
  public void testUnwrapWithInvalidJsonThrowsRuntimeException() {
    String malformedJson = "not a json";
    Exception exception =
        assertThrows(
            RuntimeException.class, () -> SerializationWrapper.unwrapLangType(malformedJson));
    assertTrue(exception.getMessage().contains("Failed to unwrap"));
  }
}
