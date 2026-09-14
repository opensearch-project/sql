/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.rest.RestRequest;

public class RestPPLQueryActionTest {

  @Test
  public void async_submit_and_job_lifecycle_are_detached_from_http_channel() {
    assertTrue(isAsync(post("{\"query\":\"source=logs\",\"keep_alive\":\"5m\"}")));
    assertTrue(
        isAsync(
            post("{\"query\":\"source=logs\"," + "\"wait_for_completion_timeout\":\"100ms\"}")));
    assertTrue(isAsync(request(RestRequest.Method.GET, "")));
    assertTrue(isAsync(request(RestRequest.Method.DELETE, "")));
  }

  @Test
  public void existing_synchronous_post_remains_channel_cancellable() {
    assertFalse(isAsync(post("{\"query\":\"source=logs\"}")));
    assertFalse(isAsync(post("{\"query\":\"source=logs\",\"async\":true}")));
    assertFalse(
        isAsync(
            post(
                "{\"query\":\"source=logs\",\"profile\":true,"
                    + "\"wait_for_completion_timeout\":\"1s\"}")));
  }

  private static boolean isAsync(RestRequest request) {
    return RestPPLQueryAction.isAsynchronousLifecycleRequest(request);
  }

  private static RestRequest post(String content) {
    return request(RestRequest.Method.POST, content);
  }

  private static RestRequest request(RestRequest.Method method, String content) {
    RestRequest request = mock(RestRequest.class);
    when(request.method()).thenReturn(method);
    when(request.content()).thenReturn(new BytesArray(content));
    when(request.rawPath()).thenReturn("/_plugins/_ppl");
    return request;
  }
}
