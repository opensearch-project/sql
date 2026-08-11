/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import java.util.Locale;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.data.type.ExprCoreType;

/**
 * Resolves an inline PPL scalar type name (the {@code makeresults data="name:type"} header and the
 * {@code multikv fields col:type} clause) to its {@link ExprCoreType}. An unknown name returns
 * {@code null} so the caller can fall back to string; a UDT name (date/time/timestamp/ip/json) is
 * rejected, since the inline paths only lower to a cast over string values.
 */
public final class PplInlineTypeResolver {

  private PplInlineTypeResolver() {}

  /**
   * @param commandName only used to phrase the rejection message
   * @return the resolved type, or {@code null} when {@code name} is not a known scalar type
   * @throws SyntaxCheckException when {@code name} is an unsupported UDT type
   */
  public static ExprCoreType resolve(String name, String commandName) {
    switch (name.toLowerCase(Locale.ROOT)) {
      case "string":
        return ExprCoreType.STRING;
      case "boolean":
        return ExprCoreType.BOOLEAN;
      case "int":
      case "integer":
        return ExprCoreType.INTEGER;
      case "long":
        return ExprCoreType.LONG;
      case "float":
        return ExprCoreType.FLOAT;
      case "double":
        return ExprCoreType.DOUBLE;
      case "date":
      case "time":
      case "timestamp":
      case "ip":
      case "json":
        throw new SyntaxCheckException(
            commandName + " inline type '" + name + "' is not yet supported; use string and cast");
      default:
        return null;
    }
  }
}
