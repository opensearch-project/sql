/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.autocomplete;

import lombok.extern.log4j.Log4j2;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATNSerializer;
import org.opensearch.sql.executor.autocomplete.AutocompleteArtifact;
import org.opensearch.sql.executor.autocomplete.GrammarArtifactBuilder;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLLexer;
import org.opensearch.sql.ppl.antlr.parser.OpenSearchPPLParser;

/**
 * Builds autocomplete artifact bundle for PPL language.
 *
 * <p>This class:
 *
 * <ul>
 *   <li>Extracts ATN and vocabulary from generated PPL parser/lexer
 *   <li>Builds PPL-specific catalogs (commands, functions, keywords)
 *   <li>Creates token-to-category mapping for suggestion classification
 *   <li>Computes grammar hash for versioning
 * </ul>
 *
 * <p>The resulting bundle is served via REST API and cached on frontend for client-side
 * autocomplete.
 */
@Log4j2
public class PPLAutocompleteArtifactBuilder {

  private static final String ANTLR_VERSION = "4.13.2";
  private static final String BUNDLE_VERSION = "1.0";
  private static final String RUNTIME_TARGET = "antlr4ng@3.x";

  /**
   * Build complete autocomplete artifact for PPL using optimized encoding.
   *
   * <p>This version uses:
   *
   * <ul>
   *   <li>Base64-encoded ATN (5-15x smaller than JSON int arrays)
   *   <li>Newline-packed string arrays (smaller than JSON arrays)
   *   <li>NO tokenTypeToCategory map (derive from naming conventions)
   * </ul>
   *
   * @return AutocompleteArtifact ready for JSON serialization
   */
  public AutocompleteArtifact buildArtifact() {
    log.info("Building optimized PPL autocomplete artifact...");

    // Create parser and lexer instances (with dummy input)
    OpenSearchPPLLexer lexer = new OpenSearchPPLLexer(CharStreams.fromString(""));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    OpenSearchPPLParser parser = new OpenSearchPPLParser(tokens);

    // Do NOT use lexer.getSerializedATN().chars().toArray() — that gives
    // the raw UTF-16 char encoding which has offset values and causes
    // "state type 65535 is not valid" errors in the frontend deserializer.
    int[] lexerATNArray = new ATNSerializer(lexer.getATN()).serialize().toArray();
    int[] parserATNArray = new ATNSerializer(parser.getATN()).serialize().toArray();

    log.info("ATN data ready:");
    log.info("  Lexer ATN: {} elements", lexerATNArray.length);
    log.info("  Parser ATN: {} elements", parserATNArray.length);

    // Extract vocabulary
    Vocabulary vocabulary = parser.getVocabulary();
    String[] literalNames = GrammarArtifactBuilder.extractLiteralNames(vocabulary);
    String[] symbolicNames = GrammarArtifactBuilder.extractSymbolicNames(vocabulary);

    // Extract rule names
    String[] parserRuleNames = GrammarArtifactBuilder.extractParserRuleNames(parser);
    String[] lexerRuleNames = GrammarArtifactBuilder.extractLexerRuleNames(lexer);

    // Extract channel/mode names from lexer
    String[] channelNames = GrammarArtifactBuilder.extractChannelNames(lexer);
    String[] modeNames = GrammarArtifactBuilder.extractModeNames(lexer);

    // Compute grammar hash from ATN data (always available, no classpath dependency)
    String grammarHash =
        GrammarArtifactBuilder.computeGrammarHash(lexerATNArray, parserATNArray, ANTLR_VERSION);

    // Build minimal artifact with only essential fields
    AutocompleteArtifact artifact =
        AutocompleteArtifact.builder()
            .bundleVersion(BUNDLE_VERSION)
            .grammarHash(grammarHash)
            // ATN data (required for interpreters)
            .lexerSerializedATN(lexerATNArray)
            .parserSerializedATN(parserATNArray)
            // Rule/channel/mode names (required for interpreters)
            .lexerRuleNames(lexerRuleNames)
            .parserRuleNames(parserRuleNames)
            .channelNames(channelNames)
            .modeNames(modeNames)
            .startRuleIndex(0) // "root" rule
            // Vocabulary (required for token display)
            .literalNames(literalNames)
            .symbolicNames(symbolicNames)
            // NO tokenTypeToCategory - removed for size (derive from conventions)
            .tokenTypeToCategory(null)
            .build();

    log.info(
        "Built optimized PPL artifact: {} tokens, lexer ATN: {} elements, parser ATN: {} elements",
        symbolicNames.length,
        lexerATNArray.length,
        parserATNArray.length);
    log.info(
        "ATN transfer size: ~{}KB (JSON int[]), ~{}KB (gzipped)",
        (lexerATNArray.length + parserATNArray.length) * 4 / 1024,
        (lexerATNArray.length + parserATNArray.length) * 2 / 1024);

    return artifact;
  }

}
