/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.exception.SemanticCheckException;

/**
 * Parses and validates named-argument style function arguments. The arg shape is {@code
 * Function("=", [StringLiteral(key), value])} — what ANTLR produces for {@code 'key'=value}. Keys
 * are lower-cased on parse; iteration order matches source order.
 *
 * <p><b>Drain semantics.</b> Every extraction method ({@code require}, {@code remove}, {@code
 * requireString}, {@code requireStringIfPresent}, {@code rejectIfPresent}, {@code consumeSilently})
 * removes its key from the collection. After the caller has extracted everything it recognizes,
 * {@code rejectRemaining} sweeps what is left and treats those keys as unknown parameters — so
 * extracted keys must drain out, otherwise they would be re-rejected.
 */
public final class NamedArguments {

  private final Map<String, UnresolvedExpression> arguments;

  private NamedArguments(Map<String, UnresolvedExpression> arguments) {
    this.arguments = arguments;
  }

  /** True iff every arg is a {@code 'key'=value} key-value pair. Empty list returns false. */
  public static boolean isNamedArguments(List<UnresolvedExpression> args) {
    if (args.isEmpty()) {
      return false;
    }
    return args.stream().allMatch(NamedArguments::isKeyValuePair);
  }

  private static boolean isKeyValuePair(UnresolvedExpression arg) {
    if (!(arg instanceof Function fn) || !"=".equals(fn.getFuncName())) {
      return false;
    }
    if (fn.getFuncArgs().size() != 2) {
      return false;
    }
    return fn.getFuncArgs().get(0) instanceof Literal keyLiteral
        && keyLiteral.getType() == DataType.STRING;
  }

  /**
   * Parses the given args into a {@code NamedArguments}. Each arg must match the {@code
   * 'key'=value} shape — a non-matching arg raises {@link SemanticCheckException}. Duplicate keys
   * also raise {@link SemanticCheckException}.
   */
  public static NamedArguments parse(List<UnresolvedExpression> args) {
    Map<String, UnresolvedExpression> arguments = new LinkedHashMap<>();
    for (UnresolvedExpression arg : args) {
      if (!isKeyValuePair(arg)) {
        throw new SemanticCheckException("Named arguments must be of form 'key'=value; got " + arg);
      }
      Function fn = (Function) arg;
      Literal keyLiteral = (Literal) fn.getFuncArgs().get(0);
      String key = keyLiteral.getValue().toString().toLowerCase(Locale.ROOT);
      UnresolvedExpression value = fn.getFuncArgs().get(1);
      if (arguments.put(key, value) != null) {
        throw new SemanticCheckException("Duplicate parameter: " + key);
      }
    }
    return new NamedArguments(arguments);
  }

  /** Removes and returns the value for {@code key}, or {@code null} if not present. */
  public UnresolvedExpression remove(String key) {
    return arguments.remove(key);
  }

  /** Removes and returns the value for {@code key}; throws if absent. */
  public UnresolvedExpression require(String key, String funcName) {
    UnresolvedExpression value = arguments.remove(key);
    if (value == null) {
      throw new SemanticCheckException(
          funcName.toLowerCase(Locale.ROOT) + " requires " + key + " parameter");
    }
    return value;
  }

  /** As {@link #require}, additionally enforcing string-literal type. */
  public Literal requireString(String key, String funcName) {
    return asStringLiteral(require(key, funcName), key);
  }

  /** As {@link #remove}, additionally enforcing string-literal type when present. */
  public Literal requireStringIfPresent(String key) {
    UnresolvedExpression value = arguments.remove(key);
    return value == null ? null : asStringLiteral(value, key);
  }

  private static Literal asStringLiteral(UnresolvedExpression expr, String paramName) {
    if (!(expr instanceof Literal literal) || literal.getType() != DataType.STRING) {
      throw new SemanticCheckException(
          paramName + " must be a string literal (e.g. '1d', '15m'); got " + expr);
    }
    return literal;
  }

  /** If {@code key} is present, throws with the supplied message; otherwise no-op. */
  public void rejectIfPresent(String key, String message) {
    if (arguments.remove(key) != null) {
      throw new SemanticCheckException(message);
    }
  }

  /** Drops the listed keys without inspecting their values. */
  public void consumeSilently(Set<String> keys) {
    for (String key : keys) {
      arguments.remove(key);
    }
  }

  /** Treats any keys still remaining as unsupported parameters. Call last. */
  public void rejectRemaining(String funcName) {
    if (arguments.isEmpty()) {
      return;
    }
    String label = arguments.size() == 1 ? "parameter" : "parameters";
    String unsupported = String.join(", ", arguments.keySet());
    throw new SemanticCheckException(
        funcName.toLowerCase(Locale.ROOT) + " does not accept " + label + ": " + unsupported);
  }

  /** Number of unconsumed keys. Primarily for tests. */
  int size() {
    return arguments.size();
  }
}
