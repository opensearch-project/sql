/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.AccessController;
import java.security.PrivilegedAction;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.sql.opensearch.storage.script.CompoundedScriptEngine.ScriptEngineType;

/** Serialization wrapper that wraps the script language type with encoded script by JSON. */
public class SerializationWrapper {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<LangScriptWrapper> TYPE_REF = new TypeReference<>() {};
  public static final String LANG_TYPE = "langType";
  public static final String SCRIPT = "script";

  /**
   * Serialize the key-value map of langType and script to JSON string
   *
   * @param langType script language type
   * @param script original script
   * @return serialized map JSON string
   */
  public static String wrapWithLangType(ScriptEngineType langType, String script) {
    return AccessController.doPrivileged(
        (PrivilegedAction<String>) () -> {
          try {
            return mapper.writeValueAsString(new LangScriptWrapper(langType, script));
          } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to wrap script with langType: " + langType, e);
          }
        });
  }

  /**
   * Deserialize JSON string to unwrap langType and original script.
   *
   * @param wrapped JSON string to be deserialized
   * @return unwrapped map of langType and script
   */
  public static LangScriptWrapper unwrapLangType(String wrapped) {
    try {
      LangScriptWrapper unwrapped = mapper.readValue(wrapped, TYPE_REF);
      if (unwrapped.langType == null || StringUtils.isBlank(unwrapped.script)) {
        throw new IllegalArgumentException("Missing required fields in language script wrapper.");
      }
      return unwrapped;
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to unwrap script with langType.", e);
    }
  }

  @NoArgsConstructor
  @AllArgsConstructor
  public static class LangScriptWrapper {
    public ScriptEngineType langType;
    public String script;
  }
}
