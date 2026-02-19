/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATNSerializer;
import org.opensearch.sql.executor.autocomplete.GrammarBundle;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;

/** Builds the {@link GrammarBundle} for the PPL language from the generated ANTLR lexer/parser. */
public class PPLGrammarBundleBuilder {

  private static final String ANTLR_VERSION = "4.13.2";
  private static final String BUNDLE_VERSION = "1.0";

  public GrammarBundle build() {
    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(CharStreams.fromString(""));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    OpenSearchPPLParser parser = new OpenSearchPPLParser(tokens);

    // ATNSerializer re-serializes the ATN into the int[] format expected by antlr4ng.
    // Do not use lexer.getSerializedATN().chars().toArray() — that yields raw UTF-16 char values
    // which cause "state type 65535 is not valid" errors in the frontend deserializer.
    int[] lexerATN = new ATNSerializer(lexer.getATN()).serialize().toArray();
    int[] parserATN = new ATNSerializer(parser.getATN()).serialize().toArray();

    Vocabulary vocabulary = parser.getVocabulary();
    int maxTokenType = vocabulary.getMaxTokenType();
    String[] literalNames = new String[maxTokenType + 1];
    String[] symbolicNames = new String[maxTokenType + 1];
    for (int i = 0; i <= maxTokenType; i++) {
      literalNames[i] = vocabulary.getLiteralName(i);
      symbolicNames[i] = vocabulary.getSymbolicName(i);
    }

    return GrammarBundle.builder()
        .bundleVersion(BUNDLE_VERSION)
        .grammarHash(computeGrammarHash(lexerATN, parserATN))
        .lexerSerializedATN(lexerATN)
        .parserSerializedATN(parserATN)
        .lexerRuleNames(lexer.getRuleNames())
        .parserRuleNames(parser.getRuleNames())
        .channelNames(lexer.getChannelNames())
        .modeNames(lexer.getModeNames())
        .startRuleIndex(0)
        .literalNames(literalNames)
        .symbolicNames(symbolicNames)
        .build();
  }

  private static String computeGrammarHash(int[] lexerATN, int[] parserATN) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (int v : lexerATN) {
        digest.update((byte) (v >> 8));
        digest.update((byte) v);
      }
      for (int v : parserATN) {
        digest.update((byte) (v >> 8));
        digest.update((byte) v);
      }
      digest.update(ANTLR_VERSION.getBytes(StandardCharsets.UTF_8));
      byte[] hash = digest.digest();
      StringBuilder sb = new StringBuilder("sha256:");
      for (byte b : hash) {
        sb.append(String.format("%02x", b & 0xFF));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
