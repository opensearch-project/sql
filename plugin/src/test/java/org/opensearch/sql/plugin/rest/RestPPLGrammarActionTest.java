/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

/** Unit tests for {@link RestPPLGrammarAction}. */
public class RestPPLGrammarActionTest {

  private RestPPLGrammarAction action;
  private NodeClient client;

  @Before
  public void setUp() {
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
    action.handleRequest(request, channel, client);

    RestResponse response = channel.getResponse();
    assertNotNull("Response should not be null", response);
    assertEquals("Should return 200 OK", RestStatus.OK, response.status());

    String content = response.content().utf8ToString();
    assertTrue(content.contains("\"bundleVersion\":\"1.0\""));
    assertTrue(content.contains("\"grammarHash\":\"sha256:"));
    assertTrue(content.contains("\"startRuleIndex\":0"));
    assertTrue(content.contains("\"lexerSerializedATN\":"));
    assertTrue(content.contains("\"parserSerializedATN\":"));
    assertTrue(content.contains("\"lexerRuleNames\":"));
    assertTrue(content.contains("\"parserRuleNames\":"));
    assertTrue(content.contains("\"literalNames\":"));
    assertTrue(content.contains("\"symbolicNames\":"));
  }

  @Test
  public void testGetGrammar_BundleIsCached() throws Exception {
    FakeRestRequest request1 =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();
    MockRestChannel channel1 = new MockRestChannel(request1, true);
    action.handleRequest(request1, channel1, client);

    FakeRestRequest request2 =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();
    MockRestChannel channel2 = new MockRestChannel(request2, true);
    action.handleRequest(request2, channel2, client);

    String content1 = channel1.getResponse().content().utf8ToString();
    String content2 = channel2.getResponse().content().utf8ToString();
    assertEquals("Consecutive requests should return identical content", content1, content2);
  }

  @Test
  public void testInvalidateCache() throws Exception {
    FakeRestRequest request1 =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();
    MockRestChannel channel1 = new MockRestChannel(request1, true);
    action.handleRequest(request1, channel1, client);
    assertEquals(RestStatus.OK, channel1.getResponse().status());

    action.invalidateCache();

    FakeRestRequest request2 =
        new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_plugins/_ppl/_grammar")
            .build();
    MockRestChannel channel2 = new MockRestChannel(request2, true);
    action.handleRequest(request2, channel2, client);
    assertEquals(RestStatus.OK, channel2.getResponse().status());

    String content1 = channel1.getResponse().content().utf8ToString();
    String content2 = channel2.getResponse().content().utf8ToString();
    assertEquals("Grammar hash should be identical after cache invalidation and rebuild", content1, content2);
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
    public boolean detailedErrorStackTraceEnabled() {
      return false;
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
      return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
      return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    @Override
    public XContentBuilder newBuilder(MediaType mediaType, boolean useFiltering) throws IOException {
      return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    @Override
    public XContentBuilder newBuilder(
        MediaType requestContentType, MediaType responseContentType, boolean useFiltering)
        throws IOException {
      return XContentBuilder.builder(XContentType.JSON.xContent());
    }

    @Override
    public BytesStreamOutput bytesOutput() {
      return null;
    }
  }
}
