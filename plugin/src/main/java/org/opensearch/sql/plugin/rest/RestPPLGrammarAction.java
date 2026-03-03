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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.sql.ppl.autocomplete.GrammarBundle;
import org.opensearch.sql.ppl.autocomplete.PPLGrammarBundleBuilder;
import org.opensearch.transport.client.node.NodeClient;

/*
 * REST handler for {@code GET /_plugins/_ppl/_grammar}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@Log4j2
public class RestPPLGrammarAction extends BaseRestHandler {

  private static final String ENDPOINT_PATH = "/_plugins/_ppl/_grammar";

  // Lazy-initialized singleton bundle (built once per JVM lifecycle)
  private GrammarBundle cachedBundle;

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
        authorizeRequest(
            client,
            new ActionListener<>() {
              @Override
              public void onResponse(TransportPPLQueryResponse ignored) {
                try {
                  GrammarBundle bundle = getOrBuildBundle();
                  XContentBuilder builder = channel.newBuilder();
                  serializeBundle(builder, bundle);
                  channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                } catch (Exception e) {
                  log.error("Error building or serializing PPL grammar", e);
                  sendErrorResponse(channel, e);
                }
              }

              @Override
              public void onFailure(Exception e) {
                log.error("PPL grammar authorization failed", e);
                sendErrorResponse(channel, e);
              }
            });
      } catch (Exception e) {
        log.error("Error authorizing PPL grammar request", e);
        sendErrorResponse(channel, e);
      }
    };
  }

  @VisibleForTesting
  protected void authorizeRequest(
      NodeClient client, ActionListener<TransportPPLQueryResponse> listener) {
    client.execute(
        PPLQueryAction.INSTANCE, new TransportPPLQueryRequest("", null, ENDPOINT_PATH), listener);
  }

  private void sendErrorResponse(RestChannel channel, Exception e) {
    try {
      channel.sendResponse(new BytesRestResponse(channel, e));
    } catch (IOException ioException) {
      log.error("Failed to send PPL grammar error response", ioException);
    }
  }

  // Thread-safe lazy initialization.
  private synchronized GrammarBundle getOrBuildBundle() {
    if (cachedBundle == null) {
      cachedBundle = buildBundle();
    }
    return cachedBundle;
  }

  /** Constructs the grammar bundle. Override in tests to inject a custom or failing builder. */
  @VisibleForTesting
  protected GrammarBundle buildBundle() {
    return new PPLGrammarBundleBuilder().build();
  }

  /** Invalidate the cached bundle, forcing a rebuild on the next request. */
  @VisibleForTesting
  protected synchronized void invalidateCache() {
    cachedBundle = null;
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

    // Autocomplete configuration
    builder.field("tokenDictionary", bundle.getTokenDictionary());
    builder.field("ignoredTokens", bundle.getIgnoredTokens());
    builder.field("rulesToVisit", bundle.getRulesToVisit());

    builder.endObject();
  }
}
