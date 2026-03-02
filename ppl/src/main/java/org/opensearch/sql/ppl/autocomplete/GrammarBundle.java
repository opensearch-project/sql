/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/** Serialized ANTLR grammar data served by {@code GET /_plugins/_ppl/_grammar}. */
@Value
@Builder
public class GrammarBundle {

  /** Bundle format version. */
  @NonNull private String bundleVersion;

  /** ANTLR runtime version used to generate the grammar. */
  @NonNull private String antlrVersion;

  /** SHA-256 hash of the serialized ATN data. Clients may use this to detect grammar changes. */
  @NonNull private String grammarHash;

  /** Serialized lexer ATN as int array (ATNSerializer output). */
  @NonNull private int[] lexerSerializedATN;

  /** Lexer rule names. */
  @NonNull private String[] lexerRuleNames;

  /** Channel names (e.g. DEFAULT_TOKEN_CHANNEL, HIDDEN). */
  @NonNull private String[] channelNames;

  /** Mode names (e.g. DEFAULT_MODE). */
  @NonNull private String[] modeNames;

  /** Serialized parser ATN as int array (ATNSerializer output). */
  @NonNull private int[] parserSerializedATN;

  /** Parser rule names. */
  @NonNull private String[] parserRuleNames;

  /** Start rule index (0 = root rule). */
  private int startRuleIndex;

  /**
   * Literal token names indexed by token type (e.g. "'search'", "'|'"). Elements may be null for
   * tokens with no literal form; clients must handle sparse arrays.
   */
  @NonNull private String[] literalNames;

  /**
   * Symbolic token names indexed by token type (e.g. "SEARCH", "PIPE"). Elements may be null for
   * tokens with no symbolic name; clients must handle sparse arrays.
   */
  @NonNull private String[] symbolicNames;

  /**
   * Autocomplete token dictionary — maps semantic names used by the autocomplete enrichment logic
   * (e.g. "SPACE", "PIPE", "SOURCE") to their token type IDs in this grammar. Clients use this
   * to configure token-aware enrichment without hardcoding token IDs.
   */
  @NonNull private Map<String, Integer> tokenDictionary;

  /**
   * Token type IDs that should be ignored by CodeCompletionCore during candidate collection.
   * These are tokens like functions, operators, and internal tokens that should not appear
   * as direct keyword suggestions (e.g. AVG, COUNT, PIPE operators).
   */
  @NonNull private int[] ignoredTokens;

  /**
   * Parser rule indices that CodeCompletionCore should treat as preferred rules.
   * When these rules are candidate alternatives, CodeCompletionCore reports them as rule
   * candidates instead of expanding into their child tokens. The autocomplete enrichment
   * uses these to trigger semantic suggestions (e.g. suggest fields, suggest tables).
   */
  @NonNull private int[] rulesToVisit;
}
