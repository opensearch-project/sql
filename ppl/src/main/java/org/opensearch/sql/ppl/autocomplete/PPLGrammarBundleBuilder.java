/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import lombok.extern.log4j.Log4j2;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATNSerializer;
import org.opensearch.sql.executor.autocomplete.GrammarBundle;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;

/** Builds the {@link GrammarBundle} for the PPL language from the generated ANTLR lexer/parser. */
@Log4j2
public class PPLGrammarBundleBuilder {

  private static final String ANTLR_VERSION = "4.13.2";
  private static final String BUNDLE_VERSION = "1.0";

  /**
   * Build the PPL grammar bundle.
   *
   * <p>Uses {@link ATNSerializer} to re-serialize the ATN into the int[] format expected by the
   * antlr4ng runtime on the frontend. Do NOT use {@code lexer.getSerializedATN().chars().toArray()}
   * — that yields raw UTF-16 char values which cause "state type 65535 is not valid" errors.
   *
   * @return {@link GrammarBundle} ready for JSON serialization
   */
  public GrammarBundle build() {
    log.info("Building PPL grammar bundle...");

    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(CharStreams.fromString(""));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    OpenSearchPPLParser parser = new OpenSearchPPLParser(tokens);

    int[] lexerATN = new ATNSerializer(lexer.getATN()).serialize().toArray();
    int[] parserATN = new ATNSerializer(parser.getATN()).serialize().toArray();

    log.info("Lexer ATN: {} elements, Parser ATN: {} elements", lexerATN.length, parserATN.length);

    Vocabulary vocabulary = parser.getVocabulary();
    int maxTokenType = vocabulary.getMaxTokenType();
    String[] literalNames = new String[maxTokenType + 1];
    String[] symbolicNames = new String[maxTokenType + 1];
    for (int i = 0; i <= maxTokenType; i++) {
      literalNames[i] = vocabulary.getLiteralName(i);
      symbolicNames[i] = vocabulary.getSymbolicName(i);
    }

    GrammarBundle bundle =
        GrammarBundle.builder()
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

    log.info("Built PPL grammar bundle: {} tokens", symbolicNames.length);
    return bundle;
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
