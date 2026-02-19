/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.autocomplete;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import lombok.extern.log4j.Log4j2;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Vocabulary;

/**
 * Utility class for extracting ANTLR grammar artifacts (ATN, vocabulary, rule names) from generated
 * parser/lexer classes.
 *
 * <p>This class handles the low-level details of:
 *
 * <ul>
 *   <li>Converting ANTLR's Java String ATN format to int[] for JSON transfer
 *   <li>Extracting vocabulary (literal and symbolic names)
 *   <li>Extracting rule names via public ANTLR APIs
 *   <li>Computing grammar hash for versioning
 * </ul>
 *
 * <p>Language-specific builders (PPL, SQL) use this class to build their autocomplete bundles.
 */
@Log4j2
public class GrammarArtifactBuilder {

  /**
   * Extract literal names from vocabulary.
   *
   * <p>Returns array where index = token type, value = literal token (with quotes) or null.
   *
   * <p>Example: ["<INVALID>", "'search'", "'where'", "'|'", null, ...]
   *
   * @param vocabulary Parser vocabulary
   * @return Array of literal names
   */
  public static String[] extractLiteralNames(Vocabulary vocabulary) {
    int maxTokenType = vocabulary.getMaxTokenType();
    String[] names = new String[maxTokenType + 1];

    for (int i = 0; i <= maxTokenType; i++) {
      String literal = vocabulary.getLiteralName(i);
      // Keep nulls as nulls (no literal representation)
      names[i] = literal;
    }

    log.debug("Extracted {} literal names", names.length);
    return names;
  }

  /**
   * Extract symbolic names from vocabulary.
   *
   * <p>Returns array where index = token type, value = symbolic token name or null.
   *
   * <p>Example: ["<INVALID>", "SEARCH", "WHERE", "PIPE", "ID", ...]
   *
   * @param vocabulary Parser vocabulary
   * @return Array of symbolic names
   */
  public static String[] extractSymbolicNames(Vocabulary vocabulary) {
    int maxTokenType = vocabulary.getMaxTokenType();
    String[] names = new String[maxTokenType + 1];

    for (int i = 0; i <= maxTokenType; i++) {
      String symbolic = vocabulary.getSymbolicName(i);
      // Keep nulls as nulls (no symbolic name)
      names[i] = symbolic;
    }

    log.debug("Extracted {} symbolic names", names.length);
    return names;
  }

  /**
   * Extract rule names from parser.
   *
   * <p>Parser.getRuleNames() is public API.
   *
   * @param parser Parser instance
   * @return Array of rule names
   */
  public static String[] extractParserRuleNames(Parser parser) {
    String[] ruleNames = parser.getRuleNames();
    log.debug("Extracted {} parser rule names", ruleNames.length);
    return ruleNames;
  }

  /**
   * Extract rule names from lexer.
   *
   * <p>Lexer.getRuleNames() is public API (Lexer extends Recognizer).
   *
   * @param lexer Lexer instance
   * @return Array of lexer rule names
   */
  public static String[] extractLexerRuleNames(Lexer lexer) {
    String[] ruleNames = lexer.getRuleNames();
    log.debug("Extracted {} lexer rule names", ruleNames.length);
    return ruleNames;
  }

  /**
   * Extract channel names from lexer.
   *
   * <p>ANTLR 4.x exposes channel names via getChannelNames() method in generated lexers.
   * This method dynamically extracts the actual channel names from the lexer instance.
   *
   * @param lexer Lexer instance
   * @return Array of channel names
   */
  public static String[] extractChannelNames(Lexer lexer) {
    String[] channelNames = lexer.getChannelNames();
    log.debug("Extracted {} channel names from lexer", channelNames.length);
    return channelNames;
  }

  /**
   * Extract mode names from lexer.
   *
   * <p>ANTLR 4.x exposes mode names via getModeNames() method in generated lexers.
   * This method dynamically extracts the actual mode names from the lexer instance.
   *
   * @param lexer Lexer instance
   * @return Array of mode names
   */
  public static String[] extractModeNames(Lexer lexer) {
    String[] modeNames = lexer.getModeNames();
    log.debug("Extracted {} mode names from lexer", modeNames.length);
    return modeNames;
  }

  /**
   * Compute grammar hash from ATN data (recommended).
   *
   * <p>This method hashes the serialized ATN arrays directly, which:
   *
   * <ul>
   *   <li>Always available at runtime (no classpath dependencies)
   *   <li>Reflects the actual artifact being served
   *   <li>Changes when grammar changes (ATN structure changes)
   * </ul>
   *
   * @param lexerATN Serialized lexer ATN as int array
   * @param parserATN Serialized parser ATN as int array
   * @param antlrVersion ANTLR tool version (e.g., "4.13.2")
   * @return Hash string in format "sha256:abc123..."
   */
  public static String computeGrammarHash(int[] lexerATN, int[] parserATN, String antlrVersion) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");

      // Hash lexer ATN data
      for (int value : lexerATN) {
        digest.update((byte) (value >> 8));
        digest.update((byte) value);
      }

      // Hash parser ATN data
      for (int value : parserATN) {
        digest.update((byte) (value >> 8));
        digest.update((byte) value);
      }

      // Hash ANTLR version to detect generator changes
      digest.update(antlrVersion.getBytes(StandardCharsets.UTF_8));

      // Compute hash
      byte[] hashBytes = digest.digest();
      String result = "sha256:" + bytesToHex(hashBytes);

      log.info("Computed grammar hash from ATN data: {}", result);
      return result;

    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is required by Java specification, this should never happen
      throw new IllegalStateException("SHA-256 algorithm not available", e);
    }
  }


  /**
   * Convert byte array to hex string.
   *
   * @param bytes Input bytes
   * @return Hex string (lowercase)
   */
  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(String.format("%02x", b & 0xFF));
    }
    return sb.toString();
  }
}
