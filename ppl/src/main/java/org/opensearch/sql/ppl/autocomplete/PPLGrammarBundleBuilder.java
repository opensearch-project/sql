/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATNSerializer;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;

/** Builds the {@link GrammarBundle} for the PPL language from the generated ANTLR lexer/parser. */
public class PPLGrammarBundleBuilder {
  private static final String ANTLR_VERSION =
      org.antlr.v4.runtime.RuntimeMetaData.getRuntimeVersion();
  private static final String BUNDLE_VERSION = "1.0";

  public GrammarBundle build() {
    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(CharStreams.fromString(""));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    OpenSearchPPLParser parser = new OpenSearchPPLParser(tokens);

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
        .antlrVersion(ANTLR_VERSION)
        .grammarHash(computeGrammarHash(lexerATN, parserATN))
        .lexerSerializedATN(lexerATN)
        .parserSerializedATN(parserATN)
        .lexerRuleNames(lexer.getRuleNames())
        .parserRuleNames(parser.getRuleNames())
        .channelNames(lexer.getChannelNames())
        .modeNames(lexer.getModeNames())
        .startRuleIndex(resolveStartRuleIndex(parser.getRuleNames()))
        .literalNames(literalNames)
        .symbolicNames(symbolicNames)
        .tokenDictionary(buildTokenDictionary(vocabulary))
        .ignoredTokens(buildIgnoredTokens())
        .rulesToVisit(buildRulesToVisit(parser.getRuleNames()))
        .build();
  }

  /**
   * Build the token dictionary — semantic name → token type ID mapping. Uses lexer constants
   * since token type IDs are defined by the lexer. The frontend autocomplete enrichment uses
   * these to identify tokens like SPACE, PIPE, SOURCE by name.
   */
  private static Map<String, Integer> buildTokenDictionary(Vocabulary vocabulary) {
    Map<String, Integer> dict = new LinkedHashMap<>();
    // SPACE token may not exist in this grammar (whitespace may be implicitly skipped).
    // Resolve by searching symbolic names; use -1 if not found.
    dict.put("WHITESPACE", OpenSearchPPLLexer.WHITESPACE);
    dict.put("FROM", OpenSearchPPLLexer.FROM);
    dict.put("OPENING_BRACKET", OpenSearchPPLLexer.LT_PRTHS);
    dict.put("CLOSING_BRACKET", OpenSearchPPLLexer.RT_PRTHS);
    dict.put("SEARCH", OpenSearchPPLLexer.SEARCH);
    dict.put("SOURCE", OpenSearchPPLLexer.SOURCE);
    dict.put("PIPE", OpenSearchPPLLexer.PIPE);
    dict.put("ID", OpenSearchPPLLexer.ID);
    dict.put("EQUAL", OpenSearchPPLLexer.EQUAL);
    dict.put("IN", OpenSearchPPLLexer.IN);
    dict.put("COMMA", OpenSearchPPLLexer.COMMA);
    dict.put("BACKTICK_QUOTE", OpenSearchPPLLexer.BQUOTA_STRING);
    dict.put("DOT", OpenSearchPPLLexer.DOT);
    return dict;
  }

  /**
   * Build the list of token type IDs to ignore for autocomplete. Mirrors the frontend
   * getIgnoredTokens() logic: explicitly ignore AS/IN, then ignore two contiguous token ranges
   * minus operatorsToInclude.
   *
   * <p>Range 1 (relevance/internal tokens): MATCH .. ERROR_RECOGNITION — covers relevance
   * functions, search parameters, span literals, IDs, quoted strings, and error tokens.
   *
   * <p>Range 2 (keywords/functions/operators): CASE .. CAST — covers CASE/ELSE, IN, EXISTS,
   * NOT/OR/AND/XOR, TRUE/FALSE, REGEXP, datetime parts, data type keywords, punctuation,
   * aggregate functions, math/text/date functions, and CAST.
   *
   * <p>Tokens in {@code operatorsToInclude} are kept as suggestions even if they fall within
   * an ignored range.
   */
  private static int[] buildIgnoredTokens() {
    // Verify range boundaries match expected token IDs. If the grammar changes and
    // shifts token ordinals, these assertions surface the problem at build time.
    assert OpenSearchPPLParser.MATCH == 427
        : "MATCH token ID shifted — update ignored range start";
    assert OpenSearchPPLParser.ERROR_RECOGNITION == 488
        : "ERROR_RECOGNITION token ID shifted — update ignored range end";
    assert OpenSearchPPLParser.CASE == 142
        : "CASE token ID shifted — update ignored range start";
    assert OpenSearchPPLParser.CAST == 387
        : "CAST token ID shifted — update ignored range end";

    Set<Integer> operatorsToInclude = new HashSet<>(Arrays.asList(
        OpenSearchPPLParser.PIPE, OpenSearchPPLParser.EQUAL, OpenSearchPPLParser.COMMA,
        OpenSearchPPLParser.NOT_EQUAL, OpenSearchPPLParser.LESS, OpenSearchPPLParser.NOT_LESS,
        OpenSearchPPLParser.GREATER, OpenSearchPPLParser.NOT_GREATER,
        OpenSearchPPLParser.OR, OpenSearchPPLParser.AND,
        OpenSearchPPLParser.LT_PRTHS, OpenSearchPPLParser.RT_PRTHS,
        OpenSearchPPLParser.SPAN,
        OpenSearchPPLParser.MATCH, OpenSearchPPLParser.MATCH_PHRASE,
        OpenSearchPPLParser.MATCH_BOOL_PREFIX, OpenSearchPPLParser.MATCH_PHRASE_PREFIX,
        OpenSearchPPLParser.SQUOTA_STRING
    ));

    List<Integer> ignored = new ArrayList<>();
    ignored.add(OpenSearchPPLParser.AS);
    ignored.add(OpenSearchPPLParser.IN);

    // Range 1: MATCH .. ERROR_RECOGNITION
    for (int i = OpenSearchPPLParser.MATCH; i <= OpenSearchPPLParser.ERROR_RECOGNITION; i++) {
      if (!operatorsToInclude.contains(i)) {
        ignored.add(i);
      }
    }

    // Range 2: CASE .. CAST
    for (int i = OpenSearchPPLParser.CASE; i <= OpenSearchPPLParser.CAST; i++) {
      if (!operatorsToInclude.contains(i)) {
        ignored.add(i);
      }
    }

    return ignored.stream().mapToInt(Integer::intValue).toArray();
  }

  /**
   * Build the list of parser rule indices for CodeCompletionCore preferredRules.
   * These rules trigger semantic suggestions (suggest fields, tables, functions, etc.).
   *
   * @throws IllegalStateException if any expected rule name is not found in the parser grammar
   */
  private static int[] buildRulesToVisit(String[] ruleNames) {
    List<String> ruleNamesToVisit = Arrays.asList(
        "statsFunctionName", "takeAggFunction", "integerLiteral", "decimalLiteral",
        "keywordsCanBeId", "renameClasue", "qualifiedName", "tableQualifiedName",
        "wcQualifiedName", "positionFunctionName", "searchableKeyWord", "stringLiteral",
        "searchCommand", "searchComparisonOperator", "comparisonOperator", "sqlLikeJoinType"
    );

    List<String> ruleNamesList = Arrays.asList(ruleNames);
    int[] indices = new int[ruleNamesToVisit.size()];
    for (int i = 0; i < ruleNamesToVisit.size(); i++) {
      String name = ruleNamesToVisit.get(i);
      int idx = ruleNamesList.indexOf(name);
      if (idx < 0) {
        throw new IllegalStateException(
            "Parser rule '" + name + "' not found in grammar — "
                + "was it renamed or removed from OpenSearchPPLParser.g4?");
      }
      indices[i] = idx;
    }
    return indices;
  }

  /** Resolve a token type ID from the vocabulary by symbolic name. Returns -1 if not found. */
  private static int resolveTokenType(Vocabulary vocabulary, String name) {
    for (int i = 0; i <= vocabulary.getMaxTokenType(); i++) {
      if (name.equals(vocabulary.getSymbolicName(i))) {
        return i;
      }
    }
    return -1;
  }

  private static int resolveStartRuleIndex(String[] ruleNames) {
    int idx = Arrays.asList(ruleNames).indexOf("root");
    return Math.max(idx, 0);
  }

  private static String computeGrammarHash(int[] lexerATN, int[] parserATN) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      updateDigest(digest, lexerATN);
      updateDigest(digest, parserATN);
      digest.update(ANTLR_VERSION.getBytes(StandardCharsets.UTF_8));
      byte[] hash = digest.digest();
      // Output is always "sha256:" (7 chars) + 64 hex chars = 71 chars.
      StringBuilder sb = new StringBuilder(71);
      sb.append("sha256:");
      for (byte b : hash) {
        sb.append(String.format("%02x", b & 0xFF));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static void updateDigest(MessageDigest digest, int[] data) {
    for (int v : data) {
      digest.update((byte) (v >>> 24));
      digest.update((byte) (v >>> 16));
      digest.update((byte) (v >>> 8));
      digest.update((byte) (v));
    }
  }
}
