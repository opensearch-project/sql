/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PPLGrammarBundleExporterTest {

  private static final Set<String> EXPECTED_FIELDS =
      new HashSet<>(
          Arrays.asList(
              "bundleVersion",
              "antlrVersion",
              "grammarHash",
              "startRuleIndex",
              "lexerSerializedATN",
              "lexerRuleNames",
              "channelNames",
              "modeNames",
              "parserSerializedATN",
              "parserRuleNames",
              "literalNames",
              "symbolicNames",
              "tokenDictionary",
              "ignoredTokens",
              "rulesToVisit"));

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSerializeBundleUsesRestSchema() {
    GrammarBundle bundle = PPLGrammarBundleBuilder.getBundle();
    JSONObject json = new JSONObject(PPLGrammarBundleExporter.serializeBundle(bundle));

    assertEquals(15, json.length());
    assertEquals(EXPECTED_FIELDS, json.keySet());
    assertEquals(bundle.getBundleVersion(), json.getString("bundleVersion"));
    assertEquals(bundle.getAntlrVersion(), json.getString("antlrVersion"));
    assertEquals(bundle.getGrammarHash(), json.getString("grammarHash"));
    assertEquals(bundle.getStartRuleIndex(), json.getInt("startRuleIndex"));
    assertEquals(
        bundle.getLexerSerializedATN().length, json.getJSONArray("lexerSerializedATN").length());
    assertEquals(bundle.getLexerRuleNames().length, json.getJSONArray("lexerRuleNames").length());
    assertEquals(bundle.getChannelNames().length, json.getJSONArray("channelNames").length());
    assertEquals(bundle.getModeNames().length, json.getJSONArray("modeNames").length());
    assertEquals(
        bundle.getParserSerializedATN().length, json.getJSONArray("parserSerializedATN").length());
    assertEquals(bundle.getParserRuleNames().length, json.getJSONArray("parserRuleNames").length());
    assertEquals(bundle.getLiteralNames().length, json.getJSONArray("literalNames").length());
    assertEquals(bundle.getSymbolicNames().length, json.getJSONArray("symbolicNames").length());
    assertEquals(
        bundle.getTokenDictionary().size(), json.getJSONObject("tokenDictionary").length());
    assertEquals(bundle.getIgnoredTokens().length, json.getJSONArray("ignoredTokens").length());
    assertEquals(bundle.getRulesToVisit().length, json.getJSONArray("rulesToVisit").length());
  }

  @Test
  public void testSerializeBundlePreservesSparseVocabularyNulls() {
    GrammarBundle bundle = PPLGrammarBundleBuilder.getBundle();
    JSONObject json = new JSONObject(PPLGrammarBundleExporter.serializeBundle(bundle));

    assertSparseArrayEquals(bundle.getLiteralNames(), json.getJSONArray("literalNames"));
    assertSparseArrayEquals(bundle.getSymbolicNames(), json.getJSONArray("symbolicNames"));
  }

  @Test
  public void testSerializeBundleIsDeterministic() {
    GrammarBundle bundle = PPLGrammarBundleBuilder.getBundle();

    assertEquals(
        PPLGrammarBundleExporter.serializeBundle(bundle),
        PPLGrammarBundleExporter.serializeBundle(bundle));
  }

  @Test
  public void testWriteBundleCreatesParentsAndReplacesOutput() throws IOException {
    GrammarBundle bundle = PPLGrammarBundleBuilder.getBundle();
    Path root = temporaryFolder.getRoot().toPath();
    Path output = root.resolve("nested/grammar/ppl.json");

    PPLGrammarBundleExporter.writeBundle(output, bundle);
    byte[] first = Files.readAllBytes(output);
    Files.writeString(output, "stale", StandardCharsets.UTF_8);
    PPLGrammarBundleExporter.writeBundle(output, bundle);

    assertArrayEquals(first, Files.readAllBytes(output));
    assertEquals(
        bundle.getGrammarHash(),
        new JSONObject(Files.readString(output, StandardCharsets.UTF_8)).getString("grammarHash"));
    try (Stream<Path> siblings = Files.list(output.getParent())) {
      assertFalse(
          siblings.anyMatch(
              path ->
                  path.getFileName().toString().startsWith(".ppl.json.")
                      && path.getFileName().toString().endsWith(".tmp")));
    }
  }

  @Test
  public void testMainRejectsInvalidArguments() throws IOException {
    assertInvalidArguments(new String[0]);
    assertInvalidArguments(new String[] {""});
    assertInvalidArguments(new String[] {"first.json", "second.json"});
  }

  @Test
  public void testWriteBundleFailsWhenParentIsAFile() throws IOException {
    Path parent = temporaryFolder.newFile("not-a-directory").toPath();
    Path output = parent.resolve("ppl.json");

    try {
      PPLGrammarBundleExporter.writeBundle(output, PPLGrammarBundleBuilder.getBundle());
      fail("Expected write to fail when the output parent is a file");
    } catch (IOException expected) {
      assertFalse(Files.exists(output));
    }
  }

  private static void assertSparseArrayEquals(String[] expected, JSONArray actual) {
    assertEquals(expected.length, actual.length());
    boolean foundNull = false;
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        foundNull = true;
        assertTrue("Expected null vocabulary entry at index " + i, actual.isNull(i));
      } else {
        assertEquals(expected[i], actual.getString(i));
      }
    }
    assertTrue("Expected at least one sparse vocabulary entry", foundNull);
  }

  private static void assertInvalidArguments(String[] args) throws IOException {
    try {
      PPLGrammarBundleExporter.main(args);
      fail("Expected invalid arguments to fail");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("output path"));
    }
  }
}
