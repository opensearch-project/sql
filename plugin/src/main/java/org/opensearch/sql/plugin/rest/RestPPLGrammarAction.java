/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.rest.RestRequest.Method.GET;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.executor.autocomplete.AutocompleteArtifact;
import org.opensearch.sql.ppl.autocomplete.PPLAutocompleteArtifactBuilder;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST handler for PPL grammar metadata endpoint.
 *
 * <p>Endpoint: GET /_plugins/_ppl/_grammar
 *
 * <p>Returns grammar data for client-side parsing, autocomplete, syntax highlighting, etc:
 *
 * <ul>
 *   <li>Serialized ANTLR ATN data (lexer + parser)
 *   <li>Vocabulary (literal and symbolic names)
 *   <li>Rule names (parser and lexer)
 *   <li>Channel and mode names
 * </ul>
 *
 * <p>The bundle is cached in memory and versioned via ETag (grammar hash). Clients should cache
 * locally and only refetch when ETag changes.
 *
 * <p>Example request:
 *
 * <pre>
 * GET /_plugins/_ppl/_grammar
 * If-None-Match: "sha256:abc123..."
 * </pre>
 *
 * <p>Response headers:
 *
 * <pre>
 * HTTP/1.1 200 OK
 * ETag: "sha256:abc123..."
 * Cache-Control: public, max-age=3600
 * Content-Type: application/json
 * </pre>
 *
 * <p>Or if client has latest:
 *
 * <pre>
 * HTTP/1.1 304 Not Modified
 * ETag: "sha256:abc123..."
 * </pre>
 */
@Log4j2
public class RestPPLGrammarAction extends BaseRestHandler {

  private static final String ENDPOINT_PATH = "/_plugins/_ppl/_grammar";

  // Lazy-initialized singleton artifact (built once per JVM lifecycle)
  private volatile AutocompleteArtifact cachedArtifact;
  private final Object artifactLock = new Object();

  @Override
  public String getName() {
    return "ppl_grammar_action";
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(new Route(GET, ENDPOINT_PATH));
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
      throws IOException {

    log.debug("Received PPL grammar request");

    return channel -> {
      try {
        // Get or build artifact (lazy initialization)
        AutocompleteArtifact artifact = getOrBuildArtifact();

        String grammarHash = artifact.getGrammarHash();
        String ifNoneMatch = request.header("If-None-Match");

        // Strip quotes from ETag if present ("sha256:..." → sha256:...)
        if (ifNoneMatch != null && ifNoneMatch.startsWith("\"") && ifNoneMatch.endsWith("\"")) {
          ifNoneMatch = ifNoneMatch.substring(1, ifNoneMatch.length() - 1);
        }

        // Return 304 Not Modified if client has latest version
        if (grammarHash.equals(ifNoneMatch)) {
          log.debug("Client has latest grammar (ETag match), returning 304");
          BytesRestResponse response =
              new BytesRestResponse(RestStatus.NOT_MODIFIED, "application/json", "");
          response.addHeader("ETag", "\"" + grammarHash + "\"");
          response.addHeader("Cache-Control", "public, max-age=3600");
          channel.sendResponse(response);
          return;
        }

        // Serialize artifact to JSON
        log.debug("Serializing grammar to JSON (hash: {})", grammarHash);
        XContentBuilder builder = channel.newBuilder();
        serializeArtifact(builder, artifact);

        BytesRestResponse response = new BytesRestResponse(RestStatus.OK, builder);

        // Add caching headers
        response.addHeader("ETag", "\"" + grammarHash + "\"");
        response.addHeader("Cache-Control", "public, max-age=3600");
        response.addHeader("Content-Type", "application/json; charset=UTF-8");

        log.info("Returning PPL grammar (size: {} bytes)", response.content().length());
        channel.sendResponse(response);

      } catch (Exception e) {
        log.error("Error building or serializing PPL grammar", e);
        channel.sendResponse(new BytesRestResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, e));
      }
    };
  }

  /**
   * Get cached artifact or build it if not initialized.
   *
   * <p>Thread-safe lazy initialization with double-checked locking.
   */
  private AutocompleteArtifact getOrBuildArtifact() {
    // First check without lock (common case: already initialized)
    if (cachedArtifact != null) {
      return cachedArtifact;
    }

    // Acquire lock for initialization
    synchronized (artifactLock) {
      // Double-check after acquiring lock
      if (cachedArtifact == null) {
        log.info("Building PPL grammar artifact (first request)...");
        long startTime = System.currentTimeMillis();

        PPLAutocompleteArtifactBuilder builder = new PPLAutocompleteArtifactBuilder();
        cachedArtifact = builder.buildArtifact();

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Built PPL grammar in {}ms (hash: {})", elapsed, cachedArtifact.getGrammarHash());
      }
      return cachedArtifact;
    }
  }

  /**
   * Invalidate cached artifact (for testing or grammar updates).
   *
   * <p>Note: This only affects the current node. In a multi-node cluster, each node maintains its
   * own cache.
   */
  public void invalidateCache() {
    synchronized (artifactLock) {
      log.info("Invalidating cached PPL grammar");
      cachedArtifact = null;
    }
  }

  /**
   * Manually serialize AutocompleteArtifact to JSON.
   *
   * <p>Outputs ATN data and arrays as standard JSON arrays.
   */
  private void serializeArtifact(XContentBuilder builder, AutocompleteArtifact artifact)
      throws IOException {
    builder.startObject();

    // Identity & versioning
    builder.field("bundleVersion", artifact.getBundleVersion());
    builder.field("grammarHash", artifact.getGrammarHash());
    builder.field("startRuleIndex", artifact.getStartRuleIndex());

    // Lexer ATN & metadata
    if (artifact.getLexerSerializedATN() != null) {
      builder.field("lexerSerializedATN", artifact.getLexerSerializedATN());
      log.debug("Lexer ATN: {} elements", artifact.getLexerSerializedATN().length);
    }

    if (artifact.getLexerRuleNames() != null) {
      builder.field("lexerRuleNames", artifact.getLexerRuleNames());
    }

    if (artifact.getChannelNames() != null) {
      builder.field("channelNames", artifact.getChannelNames());
    }

    if (artifact.getModeNames() != null) {
      builder.field("modeNames", artifact.getModeNames());
    }

    // Parser ATN & metadata
    if (artifact.getParserSerializedATN() != null) {
      builder.field("parserSerializedATN", artifact.getParserSerializedATN());
      log.debug("Parser ATN: {} elements", artifact.getParserSerializedATN().length);
    }

    if (artifact.getParserRuleNames() != null) {
      builder.field("parserRuleNames", artifact.getParserRuleNames());
    }

    // Vocabulary
    if (artifact.getLiteralNames() != null) {
      builder.field("literalNames", artifact.getLiteralNames());
    }

    if (artifact.getSymbolicNames() != null) {
      builder.field("symbolicNames", artifact.getSymbolicNames());
    }

    builder.endObject();
  }
}
