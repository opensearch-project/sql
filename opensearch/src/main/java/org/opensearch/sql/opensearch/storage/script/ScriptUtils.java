/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script;

import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.type.ExprType;

/**
 * Script Utils.
 */
@UtilityClass
public class ScriptUtils {

  /**
   * Text field doesn't have doc value (exception thrown even when you call "get")
   * Limitation: assume inner field name is always "keyword".
   */
  public static String convertTextToKeyword(String fieldName, ExprType fieldType) {
    if (fieldType == OPENSEARCH_TEXT_KEYWORD) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }
}
