/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/** Serialized ANTLR grammar data served by {@code GET /_plugins/_ppl/_grammar}. */
@Data
@Builder
public class GrammarBundle {

  /** Bundle format version. */
  @NonNull private String bundleVersion;

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

  /** Literal token names indexed by token type (e.g. "'search'", "'|'"). */
  @NonNull private String[] literalNames;

  /** Symbolic token names indexed by token type (e.g. "SEARCH", "PIPE"). */
  @NonNull private String[] symbolicNames;
}
