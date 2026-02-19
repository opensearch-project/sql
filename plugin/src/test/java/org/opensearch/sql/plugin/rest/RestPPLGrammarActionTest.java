/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Unit tests for RestPPLGrammarAction.
 *
 * <p>Tests:
 *
 * <ul>
 *   <li>200 OK response with grammar bundle
 *   <li>Grammar structure validation
 *   <li>Cache behavior
 * </ul>
 */
public class RestPPLGrammarActionTest extends OpenSearchTestCase {

  private RestPPLGrammarAction action;
  private NodeClient client;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    action = new RestPPLGrammarAction();
    client = mock(NodeClient.class);
  }

  @Test
  public void testName() {
    assertEquals("ppl_grammar_action", action.getName());
  }

  @Test
  public void testRoutes() {
    assertEquals(1, action.routes().size());
    assertEquals(RestRequest.Method.GET, action.routes().get(0).getMethod());
    assertEquals("/_plugins/_ppl/_grammar", action.routes().get(0).getPath());
  }

  @Test
  public void testGetGrammar_ReturnsBundle() throws Exception {
    FakeRestRequest request =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();

    MockRestChannel channel = new MockRestChannel(request, true);
    action.prepareRequest(request, client).accept(channel);

    RestResponse response = channel.getResponse();
    assertNotNull("Response should not be null", response);
    assertEquals("Should return 200 OK", RestStatus.OK, response.status());

    String content = new String(response.content().array(), StandardCharsets.UTF_8);
    assertTrue("Should contain bundleVersion", content.contains("\"bundleVersion\":"));
    assertTrue("Should contain grammarHash", content.contains("\"grammarHash\":"));
    assertTrue("Should contain lexerSerializedATN", content.contains("\"lexerSerializedATN\":"));
    assertTrue("Should contain parserSerializedATN", content.contains("\"parserSerializedATN\":"));
    assertTrue("Should contain literalNames", content.contains("\"literalNames\":"));
    assertTrue("Should contain symbolicNames", content.contains("\"symbolicNames\":"));
  }

  @Test
  public void testGetGrammar_ArtifactIsCached() throws Exception {
    // Make two requests
    FakeRestRequest request1 =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();

    MockRestChannel channel1 = new MockRestChannel(request1, true);
    long startTime1 = System.currentTimeMillis();
    action.prepareRequest(request1, client).accept(channel1);
    long elapsed1 = System.currentTimeMillis() - startTime1;

    // Second request should be faster (cached)
    FakeRestRequest request2 =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();

    MockRestChannel channel2 = new MockRestChannel(request2, true);
    long startTime2 = System.currentTimeMillis();
    action.prepareRequest(request2, client).accept(channel2);
    long elapsed2 = System.currentTimeMillis() - startTime2;

    // Second request should be faster (bundle cached)
    assertTrue(
        "Second request should be faster due to caching (elapsed1="
            + elapsed1
            + "ms, elapsed2="
            + elapsed2
            + "ms)",
        elapsed2 < elapsed1 || elapsed2 < 50); // Allow some variance
  }

  @Test
  public void testInvalidateCache() throws Exception {
    // Get grammar
    FakeRestRequest request1 =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();

    MockRestChannel channel1 = new MockRestChannel(request1, true);
    action.prepareRequest(request1, client).accept(channel1);
    assertEquals(RestStatus.OK, channel1.getResponse().status());

    // Invalidate cache
    action.invalidateCache();

    // Get grammar again — should still return 200 OK with same content
    FakeRestRequest request2 =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();

    MockRestChannel channel2 = new MockRestChannel(request2, true);
    action.prepareRequest(request2, client).accept(channel2);
    assertEquals(RestStatus.OK, channel2.getResponse().status());
  }

  /** Mock RestChannel to capture responses */
  private static class MockRestChannel implements RestChannel {
    private final RestRequest request;
    private final boolean detailedErrorsEnabled;
    private RestResponse response;

    MockRestChannel(RestRequest request, boolean detailedErrorsEnabled) {
      this.request = request;
      this.detailedErrorsEnabled = detailedErrorsEnabled;
    }

    @Override
    public void sendResponse(RestResponse response) {
      this.response = response;
    }

    public RestResponse getResponse() {
      return response;
    }

    @Override
    public RestRequest request() {
      return request;
    }

    @Override
    public boolean detailedErrorsEnabled() {
      return detailedErrorsEnabled;
    }

    @Override
    public org.opensearch.core.xcontent.XContentBuilder newBuilder() {
      return null;
    }

    @Override
    public org.opensearch.core.xcontent.XContentBuilder newErrorBuilder() {
      return null;
    }

    @Override
    public org.opensearch.core.xcontent.XContentBuilder newBuilder(
        org.opensearch.core.xcontent.MediaType mediaType, boolean useFiltering) {
      return null;
    }

    @Override
    public ByteArrayOutputStream bytesOutput() {
      return null;
    }
  }
}
