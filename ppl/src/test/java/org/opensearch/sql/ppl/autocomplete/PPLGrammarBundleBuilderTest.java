/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

public class PPLGrammarBundleBuilderTest {

  private static final int EXPECTED_ATN_SERIALIZATION_VERSION = 4;

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
}
