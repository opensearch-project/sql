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
import org.opensearch.sql.executor.autocomplete.GrammarBundle;
import org.opensearch.sql.ppl.autocomplete.PPLGrammarBundleBuilder;
import org.opensearch.transport.client.node.NodeClient;

/** REST handler for {@code GET /_plugins/_ppl/_grammar}. */
@Log4j2
public class RestPPLGrammarAction extends BaseRestHandler {

  private static final String ENDPOINT_PATH = "/_plugins/_ppl/_grammar";

  // Lazy-initialized singleton bundle (built once per JVM lifecycle)
  private volatile GrammarBundle cachedBundle;
  private final Object bundleLock = new Object();

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

    return channel -> {
      try {
        GrammarBundle bundle = getOrBuildBundle();

        XContentBuilder builder = channel.newBuilder();
        serializeBundle(builder, bundle);

        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));

      } catch (Exception e) {
        log.error("Error building or serializing PPL grammar", e);
        channel.sendResponse(new BytesRestResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, e));
      }
    };
  }

  // Thread-safe lazy initialization with double-checked locking.
  private GrammarBundle getOrBuildBundle() {
    if (cachedBundle != null) {
      return cachedBundle;
    }
    synchronized (bundleLock) {
      // double-check after acquiring lock
      if (cachedBundle == null) {
        log.info("Building PPL grammar bundle (first request)...");
        long startTime = System.currentTimeMillis();

        PPLGrammarBundleBuilder builder = new PPLGrammarBundleBuilder();
        cachedBundle = builder.build();

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Built PPL grammar in {}ms (hash: {})", elapsed, cachedBundle.getGrammarHash());
      }
      return cachedBundle;
    }
  }

  /** Invalidate the cached bundle, forcing a rebuild on the next request. */
  public void invalidateCache() {
    synchronized (bundleLock) {
      log.info("Invalidating cached PPL grammar bundle");
      cachedBundle = null;
    }
  }

  private void serializeBundle(XContentBuilder builder, GrammarBundle bundle)
      throws IOException {
    builder.startObject();

    // Identity & versioning
    builder.field("bundleVersion", bundle.getBundleVersion());
    builder.field("grammarHash", bundle.getGrammarHash());
    builder.field("startRuleIndex", bundle.getStartRuleIndex());

    // Lexer ATN & metadata
    if (bundle.getLexerSerializedATN() != null) {
      builder.field("lexerSerializedATN", bundle.getLexerSerializedATN());
    }

    if (bundle.getLexerRuleNames() != null) {
      builder.field("lexerRuleNames", bundle.getLexerRuleNames());
    }

    if (bundle.getChannelNames() != null) {
      builder.field("channelNames", bundle.getChannelNames());
    }

    if (bundle.getModeNames() != null) {
      builder.field("modeNames", bundle.getModeNames());
    }

    // Parser ATN & metadata
    if (bundle.getParserSerializedATN() != null) {
      builder.field("parserSerializedATN", bundle.getParserSerializedATN());
    }

    if (bundle.getParserRuleNames() != null) {
      builder.field("parserRuleNames", bundle.getParserRuleNames());
    }

    // Vocabulary
    if (bundle.getLiteralNames() != null) {
      builder.field("literalNames", bundle.getLiteralNames());
    }

    if (bundle.getSymbolicNames() != null) {
      builder.field("symbolicNames", bundle.getSymbolicNames());
    }

    builder.endObject();
  }
}
