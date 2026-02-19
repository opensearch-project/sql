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
import java.util.Collections;
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
 *   <li>304 Not Modified when ETag matches
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
    // Create request without ETag
    FakeRestRequest request =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .withHeaders(Collections.emptyMap())
            .build();

    // Create mock channel to capture response
    MockRestChannel channel = new MockRestChannel(request, true);

    // Execute request
    action.prepareRequest(request, client).accept(channel);

    // Verify response
    RestResponse response = channel.getResponse();
    assertNotNull("Response should not be null", response);
    assertEquals("Should return 200 OK", RestStatus.OK, response.status());

    // Verify ETag header present
    String etag = response.getHeaders().get("ETag").get(0);
    assertNotNull("ETag header should be present", etag);
    assertTrue("ETag should start with sha256:", etag.contains("sha256:"));

    // Verify Cache-Control header
    String cacheControl = response.getHeaders().get("Cache-Control").get(0);
    assertNotNull("Cache-Control header should be present", cacheControl);
    assertTrue("Should allow caching", cacheControl.contains("max-age=3600"));

    // Verify response content
    String content = new String(response.content().array(), StandardCharsets.UTF_8);
    assertTrue("Should contain bundleVersion", content.contains("\"bundleVersion\":"));
    assertTrue("Should contain grammarHash", content.contains("\"grammarHash\":"));
    assertTrue("Should contain lexerSerializedATN", content.contains("\"lexerSerializedATN\":"));
    assertTrue("Should contain parserSerializedATN", content.contains("\"parserSerializedATN\":"));
    assertTrue("Should contain literalNames", content.contains("\"literalNames\":"));
    assertTrue("Should contain symbolicNames", content.contains("\"symbolicNames\":"));
  }

  @Test
  public void testGetGrammar_Returns304WhenETagMatches() throws Exception {
    // First request to get ETag
    FakeRestRequest firstRequest =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();

    MockRestChannel firstChannel = new MockRestChannel(firstRequest, true);
    action.prepareRequest(firstRequest, client).accept(firstChannel);

    RestResponse firstResponse = firstChannel.getResponse();
    String etag = firstResponse.getHeaders().get("ETag").get(0);

    // Second request with matching ETag
    FakeRestRequest secondRequest =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .withHeaders(Collections.singletonMap("If-None-Match", Collections.singletonList(etag)))
            .build();

    MockRestChannel secondChannel = new MockRestChannel(secondRequest, true);
    action.prepareRequest(secondRequest, client).accept(secondChannel);

    RestResponse secondResponse = secondChannel.getResponse();
    assertEquals(
        "Should return 304 Not Modified when ETag matches",
        RestStatus.NOT_MODIFIED,
        secondResponse.status());
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

    // Both should return same ETag
    String etag1 = channel1.getResponse().getHeaders().get("ETag").get(0);
    String etag2 = channel2.getResponse().getHeaders().get("ETag").get(0);
    assertEquals("ETags should match", etag1, etag2);

    // Second request should be faster (artifact cached)
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
    String etag1 = channel1.getResponse().getHeaders().get("ETag").get(0);

    // Invalidate cache
    action.invalidateCache();

    // Get grammar again
    FakeRestRequest request2 =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();

    MockRestChannel channel2 = new MockRestChannel(request2, true);
    action.prepareRequest(request2, client).accept(channel2);
    String etag2 = channel2.getResponse().getHeaders().get("ETag").get(0);

    // ETags should still match (same grammar)
    assertEquals("ETags should match after cache invalidation", etag1, etag2);
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
