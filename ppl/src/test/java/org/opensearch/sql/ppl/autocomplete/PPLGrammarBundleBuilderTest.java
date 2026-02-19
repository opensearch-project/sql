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
import org.opensearch.sql.executor.autocomplete.GrammarBundle;

public class PPLGrammarBundleBuilderTest {

  private static GrammarBundle bundle;

  @BeforeClass
  public static void buildBundle() {
    bundle = new PPLGrammarBundleBuilder().build();
  }

  @Test
  public void bundleIsNotNull() {
    assertNotNull(bundle);
  }

  @Test
  public void bundleVersionIsSet() {
    assertEquals("1.0", bundle.getBundleVersion());
  }

  @Test
  public void grammarHashHasExpectedFormat() {
    String hash = bundle.getGrammarHash();
    assertNotNull(hash);
    assertTrue("grammarHash should start with 'sha256:'", hash.startsWith("sha256:"));
    assertTrue("grammarHash should be 71 chars (sha256: + 64 hex)", hash.length() == 71);
  }

  @Test
  public void startRuleIndexIsZero() {
    assertEquals(0, bundle.getStartRuleIndex());
  }

  @Test
  public void lexerATNIsNonEmpty() {
    assertNotNull(bundle.getLexerSerializedATN());
    assertTrue(bundle.getLexerSerializedATN().length > 0);
  }

  @Test
  public void parserATNIsNonEmpty() {
    assertNotNull(bundle.getParserSerializedATN());
    assertTrue(bundle.getParserSerializedATN().length > 0);
  }

  @Test
  public void lexerRuleNamesAreNonEmpty() {
    assertNotNull(bundle.getLexerRuleNames());
    assertTrue(bundle.getLexerRuleNames().length > 0);
  }

  @Test
  public void parserRuleNamesAreNonEmpty() {
    assertNotNull(bundle.getParserRuleNames());
    assertTrue(bundle.getParserRuleNames().length > 0);
  }

  @Test
  public void channelNamesAreNonEmpty() {
    assertNotNull(bundle.getChannelNames());
    assertTrue(bundle.getChannelNames().length > 0);
  }

  @Test
  public void modeNamesAreNonEmpty() {
    assertNotNull(bundle.getModeNames());
    assertTrue(bundle.getModeNames().length > 0);
  }

  @Test
  public void vocabularyIsNonEmpty() {
    assertNotNull(bundle.getLiteralNames());
    assertNotNull(bundle.getSymbolicNames());
    assertTrue(bundle.getLiteralNames().length > 0);
    assertTrue(bundle.getSymbolicNames().length > 0);
  }

  @Test
  public void buildIsDeterministic() {
    GrammarBundle second = new PPLGrammarBundleBuilder().build();
    assertEquals(
        "Two builds of the same grammar should produce the same hash",
        bundle.getGrammarHash(),
        second.getGrammarHash());
  }
}
