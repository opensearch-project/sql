/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.autocomplete;

import java.util.Map;
import lombok.Builder;
import lombok.Data;

/**
 * Autocomplete artifact bundle containing everything needed for client-side grammar-based
 * autocomplete.
 *
 * <p>This bundle is language-agnostic and can be used for PPL, SQL, or any ANTLR-based language. It
 * contains:
 *
 * <ul>
 *   <li>Serialized ATN data for lexer and parser (for antlr4ng runtime)
 *   <li>Vocabulary and rule names (for token/rule interpretation)
 *   <li>Static catalogs (commands, functions, keywords, snippets)
 *   <li>Token classification mapping (for suggestion categorization)
 * </ul>
 *
 * <p>Frontend uses this bundle to:
 *
 * <ol>
 *   <li>Deserialize ATNs with antlr4ng
 *   <li>Create LexerInterpreter and ParserInterpreter
 *   <li>Use antlr4-c3 to find valid tokens at cursor
 *   <li>Generate suggestions from catalogs
 * </ol>
 */
@Data
@Builder
public class AutocompleteArtifact {

  // ============================================================================
  // Identity & versioning
  // ============================================================================

  /** Bundle version (increment when format changes) */
  private String bundleVersion;

  /**
   * Hash of grammar sources + ANTLR version. Used for cache validation via ETag. Format:
   * "sha256:abc123..."
   */
  private String grammarHash;

  // ============================================================================
  // Lexer ATN & metadata
  // ============================================================================

  /**
   * Serialized lexer ATN as int array. Frontend uses directly: new
   * ATNDeserializer().deserialize(lexerSerializedATN)
   */
  private int[] lexerSerializedATN;

  /** Lexer rule names (e.g., ["SEARCH", "WHERE", "PIPE", ...]) */
  private String[] lexerRuleNames;

  /** Channel names (e.g., ["DEFAULT_TOKEN_CHANNEL", "WHITESPACE", "ERRORCHANNEL"]) */
  private String[] channelNames;

  /** Mode names (e.g., ["DEFAULT_MODE"]) */
  private String[] modeNames;

  // ============================================================================
  // Parser ATN & metadata
  // ============================================================================

  /**
   * Serialized parser ATN as int array. Frontend uses directly: new
   * ATNDeserializer().deserialize(parserSerializedATN)
   */
  private int[] parserSerializedATN;

  /** Parser rule names (e.g., ["root", "pplStatement", "commands", ...]) */
  private String[] parserRuleNames;

  /** Start rule index (usually 0 for "root" rule) */
  private int startRuleIndex;

  // ============================================================================
  // Vocabulary
  // ============================================================================

  /**
   * Literal names from vocabulary. Index = token type. Values are literal tokens with quotes, or
   * null. Example: ["<INVALID>", "'search'", "'where'", "'|'", null, null, ...]
   */
  private String[] literalNames;

  /**
   * Symbolic names from vocabulary. Index = token type. Values are token symbolic names, or null.
   * Example: ["<INVALID>", "SEARCH", "WHERE", "PIPE", "ID", "INTEGER", ...]
   */
  private String[] symbolicNames;

  /**
   * Optional display names (user-friendly token names). If not provided, frontend uses literal or
   * symbolic names.
   */
  private String[] displayNames;

  // ============================================================================
  // Token classification
  // ============================================================================

  /**
   * Mapping from token symbolic name to suggestion category. Used by frontend to classify antlr4-c3
   * token candidates into suggestion types.
   *
   * <p>Example: { "SEARCH": "COMMAND", "WHERE": "COMMAND", "BY": "KEYWORD", "COUNT": "FUNCTION",
   * "AND": "OPERATOR" }
   */
  private Map<String, String> tokenTypeToCategory;
}
