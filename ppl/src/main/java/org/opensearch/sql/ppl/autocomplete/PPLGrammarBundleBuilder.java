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
  private static final Set<String> INTERNAL_NON_LITERAL_TOKENS =
      new HashSet<>(
          Arrays.asList(
              "ID",
              "NUMERIC_ID",
              "ID_DATE_SUFFIX",
              "CLUSTER",
              "TIME_SNAP",
              "SPANLENGTH",
              "DECIMAL_SPANLENGTH",
              "DQUOTA_STRING",
              "SQUOTA_STRING",
              "BQUOTA_STRING",
              "LINE_COMMENT",
              "BLOCK_COMMENT",
              "ERROR_RECOGNITION"));

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
        .grammarHash(
            computeGrammarHash(
                lexerATN,
                parserATN,
                lexer.getRuleNames(),
                parser.getRuleNames(),
                literalNames,
                symbolicNames))
        .lexerSerializedATN(lexerATN)
        .parserSerializedATN(parserATN)
        .lexerRuleNames(lexer.getRuleNames())
        .parserRuleNames(parser.getRuleNames())
        .channelNames(lexer.getChannelNames())
        .modeNames(lexer.getModeNames())
        .startRuleIndex(resolveStartRuleIndex(parser.getRuleNames()))
        .literalNames(literalNames)
        .symbolicNames(symbolicNames)
        .tokenDictionary(buildTokenDictionary())
        .ignoredTokens(buildIgnoredTokens(vocabulary))
        .rulesToVisit(buildRulesToVisit(parser.getRuleNames()))
        .build();
  }

  /**
   * Build the token dictionary — semantic name → token type ID mapping. Uses lexer constants since
   * token type IDs are defined by the lexer. The frontend autocomplete enrichment uses these to
   * identify tokens like PIPE and SOURCE by name.
   */
  private static Map<String, Integer> buildTokenDictionary() {
    Map<String, Integer> dict = new LinkedHashMap<>();
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
   * Build token type IDs to ignore for autocomplete.
   *
   * <p>Only lexical/internal tokens are ignored (identifiers, literals, quoted-string tokens,
   * comments, and error token). User-facing commands/functions/operators are intentionally kept so
   * completion dynamically reflects grammar changes.
   */
  private static int[] buildIgnoredTokens(Vocabulary vocabulary) {
    List<Integer> ignored = new ArrayList<>();

    for (int tokenType = 0; tokenType <= vocabulary.getMaxTokenType(); tokenType++) {
      String symbolicName = vocabulary.getSymbolicName(tokenType);
      if (isLexicalInternalToken(symbolicName)) {
        ignored.add(tokenType);
      }
    }

    return ignored.stream().mapToInt(Integer::intValue).toArray();
  }

  private static boolean isLexicalInternalToken(String symbolicName) {
    if (symbolicName == null) {
      return false;
    }
    return symbolicName.endsWith("_LITERAL") || INTERNAL_NON_LITERAL_TOKENS.contains(symbolicName);
  }

  /**
   * Build the list of parser rule indices for CodeCompletionCore preferredRules. These rules
   * trigger semantic suggestions (suggest fields, tables, functions, etc.).
   *
   * @throws IllegalStateException if any expected rule name is not found in the parser grammar
   */
  private static int[] buildRulesToVisit(String[] ruleNames) {
    List<String> ruleNamesToVisit =
        Arrays.asList(
            "statsFunctionName",
            "takeAggFunction",
            "integerLiteral",
            "decimalLiteral",
            "keywordsCanBeId",
            "renameClasue",
            "qualifiedName",
            "tableQualifiedName",
            "wcQualifiedName",
            "positionFunctionName",
            "searchableKeyWord",
            "stringLiteral",
            "searchCommand",
            "searchComparisonOperator",
            "comparisonOperator",
            "sqlLikeJoinType");

    List<String> ruleNamesList = Arrays.asList(ruleNames);
    int[] indices = new int[ruleNamesToVisit.size()];
    for (int i = 0; i < ruleNamesToVisit.size(); i++) {
      String name = ruleNamesToVisit.get(i);
      int idx = ruleNamesList.indexOf(name);
      if (idx < 0) {
        throw new IllegalStateException(
            "Parser rule '"
                + name
                + "' not found in grammar — "
                + "was it renamed or removed from OpenSearchPPLParser.g4?");
      }
      indices[i] = idx;
    }
    return indices;
  }

  private static int resolveStartRuleIndex(String[] ruleNames) {
    int idx = Arrays.asList(ruleNames).indexOf("root");
    return Math.max(idx, 0);
  }

  private static String computeGrammarHash(
      int[] lexerATN,
      int[] parserATN,
      String[] lexerRuleNames,
      String[] parserRuleNames,
      String[] literalNames,
      String[] symbolicNames) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      updateDigest(digest, lexerATN);
      updateDigest(digest, parserATN);
      updateDigest(digest, lexerRuleNames);
      updateDigest(digest, parserRuleNames);
      updateDigest(digest, literalNames);
      updateDigest(digest, symbolicNames);
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

  private static void updateDigest(MessageDigest digest, String[] data) {
    for (String value : data) {
      if (value == null) {
        digest.update((byte) 0);
      } else {
        digest.update((byte) 1);
        digest.update(value.getBytes(StandardCharsets.UTF_8));
      }
      // field separator to avoid concatenation ambiguities
      digest.update((byte) 0xFF);
    }
  }
}
