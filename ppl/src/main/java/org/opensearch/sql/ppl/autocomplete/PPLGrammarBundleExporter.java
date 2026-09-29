/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import org.json.JSONStringer;

/** Exports the generated PPL grammar bundle without starting an OpenSearch cluster. */
public final class PPLGrammarBundleExporter {

  private PPLGrammarBundleExporter() {}

  /**
   * Writes the PPL grammar bundle to the output path supplied as the sole argument.
   *
   * @param args exactly one output file path
   */
  public static void main(String[] args) throws IOException {
    if (args.length != 1 || args[0].trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Expected exactly one non-empty output path argument for the PPL grammar bundle");
    }

    writeBundle(Path.of(args[0]), PPLGrammarBundleBuilder.getBundle());
  }

  static void writeBundle(Path outputPath, GrammarBundle bundle) throws IOException {
    Path output = outputPath.toAbsolutePath().normalize();
    Path parent = output.getParent();
    if (output.getFileName() == null || parent == null) {
      throw new IllegalArgumentException("Output path must identify a file: " + outputPath);
    }

    Files.createDirectories(parent);
    Path temporary =
        Files.createTempFile(parent, "." + output.getFileName().toString() + ".", ".tmp");
    boolean moved = false;
    try {
      Files.writeString(
          temporary,
          serializeBundle(bundle),
          StandardCharsets.UTF_8,
          StandardOpenOption.TRUNCATE_EXISTING);
      moveIntoPlace(temporary, output);
      moved = true;
    } finally {
      if (!moved) {
        Files.deleteIfExists(temporary);
      }
    }
  }

  static String serializeBundle(GrammarBundle bundle) {
    JSONStringer json = new JSONStringer();
    json.object();

    json.key("bundleVersion").value(bundle.getBundleVersion());
    json.key("antlrVersion").value(bundle.getAntlrVersion());
    json.key("grammarHash").value(bundle.getGrammarHash());
    json.key("startRuleIndex").value(bundle.getStartRuleIndex());

    writeIntArray(json, "lexerSerializedATN", bundle.getLexerSerializedATN());
    writeStringArray(json, "lexerRuleNames", bundle.getLexerRuleNames());
    writeStringArray(json, "channelNames", bundle.getChannelNames());
    writeStringArray(json, "modeNames", bundle.getModeNames());

    writeIntArray(json, "parserSerializedATN", bundle.getParserSerializedATN());
    writeStringArray(json, "parserRuleNames", bundle.getParserRuleNames());

    writeStringArray(json, "literalNames", bundle.getLiteralNames());
    writeStringArray(json, "symbolicNames", bundle.getSymbolicNames());

    json.key("tokenDictionary").object();
    for (Map.Entry<String, Integer> entry : bundle.getTokenDictionary().entrySet()) {
      json.key(entry.getKey()).value(entry.getValue());
    }
    json.endObject();
    writeIntArray(json, "ignoredTokens", bundle.getIgnoredTokens());
    writeIntArray(json, "rulesToVisit", bundle.getRulesToVisit());

    json.endObject();
    return json.toString();
  }

  private static void writeIntArray(JSONStringer json, String fieldName, int[] values) {
    json.key(fieldName).array();
    for (int value : values) {
      json.value(value);
    }
    json.endArray();
  }

  private static void writeStringArray(JSONStringer json, String fieldName, String[] values) {
    json.key(fieldName).array();
    for (String value : values) {
      json.value(value);
    }
    json.endArray();
  }

  private static void moveIntoPlace(Path temporary, Path output) throws IOException {
    try {
      Files.move(
          temporary, output, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(temporary, output, StandardCopyOption.REPLACE_EXISTING);
    }
  }
}
