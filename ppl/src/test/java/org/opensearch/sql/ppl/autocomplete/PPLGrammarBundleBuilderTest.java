/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;

public class PPLGrammarBundleBuilderTest {

  private static final int EXPECTED_ATN_SERIALIZATION_VERSION = 4;
  private static final Set<String> EXPECTED_IGNORED_NON_LITERAL_SYMBOLS =
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

  private static GrammarBundle bundle;

  @BeforeClass
  public static void buildBundle() {
    bundle = new PPLGrammarBundleBuilder().build();
  }

  @Test
  public void testBuildBundleNotNull() {
    assertNotNull(bundle);
  }

  @Test
  public void testBuildBundleVersionIsSet() {
    assertEquals("1.0", bundle.getBundleVersion());
  }

  @Test
  public void testBuildAntlrVersionIsSet() {
    String version = bundle.getAntlrVersion();
    assertNotNull("antlrVersion should not be null", version);
    assertTrue(
        "antlrVersion should look like a version string, got: " + version,
        version.matches("\\d+\\.\\d+.*"));
  }

  @Test
  public void testBuildGrammarHashHasSha256Format() {
    String hash = bundle.getGrammarHash();
    assertNotNull(hash);
    assertTrue("grammarHash should start with 'sha256:'", hash.startsWith("sha256:"));
    assertEquals("grammarHash should be 71 chars (sha256: + 64 hex)", 71, hash.length());
  }

  @Test
  public void testBuildStartRuleIndexIsZero() {
    assertEquals(0, bundle.getStartRuleIndex());
  }

  @Test
  public void testBuildLexerATNIsNonEmpty() {
    assertNotNull(bundle.getLexerSerializedATN());
    assertTrue(bundle.getLexerSerializedATN().length > 0);
  }

  @Test
  public void testBuildLexerATNIsSerializationVersion4() {
    assertEquals(
        "Lexer ATN must be serialization version 4 for antlr4ng compatibility",
        EXPECTED_ATN_SERIALIZATION_VERSION,
        bundle.getLexerSerializedATN()[0]);
  }

  @Test
  public void testBuildParserATNIsNonEmpty() {
    assertNotNull(bundle.getParserSerializedATN());
    assertTrue(bundle.getParserSerializedATN().length > 0);
  }

  @Test
  public void testBuildParserATNIsSerializationVersion4() {
    assertEquals(
        "Parser ATN must be serialization version 4 for antlr4ng compatibility",
        EXPECTED_ATN_SERIALIZATION_VERSION,
        bundle.getParserSerializedATN()[0]);
  }

  @Test
  public void testBuildLexerRuleNamesAreNonEmpty() {
    assertNotNull(bundle.getLexerRuleNames());
    assertTrue(bundle.getLexerRuleNames().length > 0);
  }

  @Test
  public void testBuildParserRuleNamesAreNonEmpty() {
    assertNotNull(bundle.getParserRuleNames());
    assertTrue(bundle.getParserRuleNames().length > 0);
  }

  @Test
  public void testBuildChannelNamesAreNonEmpty() {
    assertNotNull(bundle.getChannelNames());
    assertTrue(bundle.getChannelNames().length > 0);
  }

  @Test
  public void testBuildModeNamesAreNonEmpty() {
    assertNotNull(bundle.getModeNames());
    assertTrue(bundle.getModeNames().length > 0);
  }

  @Test
  public void testBuildVocabularyIsNonEmpty() {
    assertNotNull(bundle.getLiteralNames());
    assertNotNull(bundle.getSymbolicNames());
    assertTrue(bundle.getLiteralNames().length > 0);
    assertTrue(bundle.getSymbolicNames().length > 0);
  }

  @Test
  public void testBuildIsDeterministic() {
    GrammarBundle second = new PPLGrammarBundleBuilder().build();
    assertEquals(
        "Two builds of the same grammar should produce the same hash",
        bundle.getGrammarHash(),
        second.getGrammarHash());
  }

  @Test
  public void testTokenDictionaryContainsExpectedEntries() {
    Map<String, Integer> dict = bundle.getTokenDictionary();
    assertNotNull(dict);
    assertEquals((Integer) OpenSearchPPLParser.PIPE, dict.get("PIPE"));
    assertEquals((Integer) OpenSearchPPLParser.SOURCE, dict.get("SOURCE"));
    assertEquals((Integer) OpenSearchPPLParser.FROM, dict.get("FROM"));
    assertEquals((Integer) OpenSearchPPLParser.EQUAL, dict.get("EQUAL"));
    assertEquals((Integer) OpenSearchPPLParser.ID, dict.get("ID"));
  }

  @Test
  public void testIgnoredTokensAreNonEmpty() {
    assertNotNull(bundle.getIgnoredTokens());
    assertTrue("ignoredTokens should not be empty", bundle.getIgnoredTokens().length > 0);
  }

  @Test
  public void testRulesToVisitAreNonEmpty() {
    assertNotNull(bundle.getRulesToVisit());
    assertTrue("rulesToVisit should not be empty", bundle.getRulesToVisit().length > 0);
  }

  @Test
  public void testIgnoredTokensContainOnlyLexicalInternalTokens() {
    Set<Integer> ignored = ignoredTokenSet();
    for (Integer tokenType : ignored) {
      String symbol = bundle.getSymbolicNames()[tokenType];
      assertTrue(
          "ignoredTokens should contain only lexical/internal tokens, got: "
              + symbol
              + " ("
              + tokenType
              + ")",
          symbol != null
              && (symbol.endsWith("_LITERAL")
                  || EXPECTED_IGNORED_NON_LITERAL_SYMBOLS.contains(symbol)));
    }
  }

  @Test
  public void testCommandAndKeywordTokensAreNotIgnored() {
    Set<Integer> ignored = ignoredTokenSet();
    assertFalse("LOOKUP should not be ignored", ignored.contains(OpenSearchPPLParser.LOOKUP));
    assertFalse("REPLACE should not be ignored", ignored.contains(OpenSearchPPLParser.REPLACE));
    assertFalse("REVERSE should not be ignored", ignored.contains(OpenSearchPPLParser.REVERSE));
    assertFalse("MVCOMBINE should not be ignored", ignored.contains(OpenSearchPPLParser.MVCOMBINE));
    assertFalse("MVEXPAND should not be ignored", ignored.contains(OpenSearchPPLParser.MVEXPAND));
    assertFalse("LEFT should not be ignored", ignored.contains(OpenSearchPPLParser.LEFT));
    assertFalse("RIGHT should not be ignored", ignored.contains(OpenSearchPPLParser.RIGHT));
    assertFalse("AS should not be ignored", ignored.contains(OpenSearchPPLParser.AS));
    assertFalse("IN should not be ignored", ignored.contains(OpenSearchPPLParser.IN));
  }

  @Test
  public void testExpressionFunctionTokensAreNotIgnored() {
    Set<Integer> ignored = ignoredTokenSet();
    assertFalse("MVAPPEND should not be ignored", ignored.contains(OpenSearchPPLParser.MVAPPEND));
    assertFalse("MVJOIN should not be ignored", ignored.contains(OpenSearchPPLParser.MVJOIN));
    assertFalse("MVINDEX should not be ignored", ignored.contains(OpenSearchPPLParser.MVINDEX));
  }

  @Test
  public void testNewerGrammarKeywordsAreNotIgnoredWhenPresent() {
    // These tokens exist in newer grammar variants (for example graph lookup support).
    // Keep this test tolerant so it works across branches with different grammar revisions.
    assertTokenNotIgnoredIfPresent("GRAPHLOOKUP");
    assertTokenNotIgnoredIfPresent("START_FIELD");
    assertTokenNotIgnoredIfPresent("FROM_FIELD");
    assertTokenNotIgnoredIfPresent("TO_FIELD");
    assertTokenNotIgnoredIfPresent("MAX_DEPTH");
    assertTokenNotIgnoredIfPresent("DEPTH_FIELD");
    assertTokenNotIgnoredIfPresent("DIRECTION");
    assertTokenNotIgnoredIfPresent("UNI");
    assertTokenNotIgnoredIfPresent("BI");
    assertTokenNotIgnoredIfPresent("SUPPORT_ARRAY");
    assertTokenNotIgnoredIfPresent("BATCH_MODE");
    assertTokenNotIgnoredIfPresent("USE_PIT");
  }

  private static Set<Integer> ignoredTokenSet() {
    Set<Integer> ignored = new HashSet<>();
    for (int tokenType : bundle.getIgnoredTokens()) {
      ignored.add(tokenType);
    }
    return ignored;
  }

  private static void assertTokenNotIgnoredIfPresent(String symbolicTokenName) {
    int tokenType = tokenTypeBySymbolicName(symbolicTokenName);
    if (tokenType >= 0) {
      assertFalse(symbolicTokenName + " should not be ignored", ignoredTokenSet().contains(tokenType));
    }
  }

  private static int tokenTypeBySymbolicName(String symbolicTokenName) {
    String[] symbols = bundle.getSymbolicNames();
    for (int i = 0; i < symbols.length; i++) {
      if (symbolicTokenName.equals(symbols[i])) {
        return i;
      }
    }
    return -1;
  }
}
