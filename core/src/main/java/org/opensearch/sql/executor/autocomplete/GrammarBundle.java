/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.autocomplete;

import lombok.Builder;
import lombok.Data;

/**
 * Grammar bundle containing everything needed for client-side ANTLR-based parsing.
 *
 * <p>Includes serialized ATN data (lexer + parser), vocabulary, and rule/channel/mode names.
 * Language-agnostic — usable for PPL, SQL, or any ANTLR4 grammar.
 */
@Data
@Builder
public class GrammarBundle {

  /** Bundle format version. Increment when the schema changes. */
  private String bundleVersion;

  /** SHA-256 hash of the serialized ATN data. Clients may use this to detect grammar changes. */
  private String grammarHash;

  /** Serialized lexer ATN as int array (ATNSerializer output). */
  private int[] lexerSerializedATN;

  /** Lexer rule names. */
  private String[] lexerRuleNames;

  /** Channel names (e.g. DEFAULT_TOKEN_CHANNEL, HIDDEN). */
  private String[] channelNames;

  /** Mode names (e.g. DEFAULT_MODE). */
  private String[] modeNames;

  /** Serialized parser ATN as int array (ATNSerializer output). */
  private int[] parserSerializedATN;

  /** Parser rule names. */
  private String[] parserRuleNames;

  /** Start rule index (0 = root rule). */
  private int startRuleIndex;

  /** Literal token names indexed by token type (e.g. "'search'", "'|'"). */
  private String[] literalNames;

  /** Symbolic token names indexed by token type (e.g. "SEARCH", "PIPE"). */
  private String[] symbolicNames;
}
