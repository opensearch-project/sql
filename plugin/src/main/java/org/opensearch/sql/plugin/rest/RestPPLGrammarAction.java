/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.rest.RestRequest.Method.GET;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.ppl.autocomplete.GrammarBundle;
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
        cachedBundle = buildBundle();
      }
      return cachedBundle;
    }
  }

  /** Constructs the grammar bundle. Override in tests to inject a custom or failing builder. */
  @VisibleForTesting
  protected GrammarBundle buildBundle() {
    return new PPLGrammarBundleBuilder().build();
  }

  /** Invalidate the cached bundle, forcing a rebuild on the next request. */
  @VisibleForTesting
  public void invalidateCache() {
    synchronized (bundleLock) {
      cachedBundle = null;
    }
  }

  private void serializeBundle(XContentBuilder builder, GrammarBundle bundle) throws IOException {
    builder.startObject();

    // Identity & versioning
    builder.field("bundleVersion", bundle.getBundleVersion());
    builder.field("antlrVersion", bundle.getAntlrVersion());
    builder.field("grammarHash", bundle.getGrammarHash());
    builder.field("startRuleIndex", bundle.getStartRuleIndex());

    // Lexer ATN & metadata
    builder.field("lexerSerializedATN", bundle.getLexerSerializedATN());
    builder.field("lexerRuleNames", bundle.getLexerRuleNames());
    builder.field("channelNames", bundle.getChannelNames());
    builder.field("modeNames", bundle.getModeNames());

    // Parser ATN & metadata
    builder.field("parserSerializedATN", bundle.getParserSerializedATN());
    builder.field("parserRuleNames", bundle.getParserRuleNames());

    // Vocabulary
    builder.field("literalNames", bundle.getLiteralNames());
    builder.field("symbolicNames", bundle.getSymbolicNames());

    builder.endObject();
  }
}
