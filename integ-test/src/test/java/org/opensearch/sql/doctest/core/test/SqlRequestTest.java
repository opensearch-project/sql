/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.test;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;
import org.opensearch.sql.doctest.core.request.SqlRequest;
import org.opensearch.sql.doctest.core.request.SqlRequest.UrlParam;

/**
 * Test cases for {@link SqlRequest}
 */
public class SqlRequestTest {

  @Test
  public void requestShouldIncludeAllFields() throws IOException {
    String method = "POST";
    String endpoint = QUERY_API_ENDPOINT;
    String body = "{\"query\":\"SELECT * FROM accounts\"}";
    String key = "format";
    String value = "jdbc";
    UrlParam param = new UrlParam(key, value);

    RestClient client = mock(RestClient.class);
    SqlRequest sqlRequest = new SqlRequest(method, endpoint, body, param);
    sqlRequest.send(client);

    ArgumentCaptor<Request> argument = ArgumentCaptor.forClass(Request.class);
    verify(client).performRequest(argument.capture());
    Request actual = argument.getValue();
    assertThat(actual.getMethod(), is(method));
    assertThat(actual.getEndpoint(), is(endpoint));
    assertThat(actual.getParameters(), hasEntry(key, value));
    assertThat(body(actual), is(body));
  }

  @Test(expected = IllegalArgumentException.class)
  public void badUrlParamShouldThrowException() {
    new UrlParam("test");
  }

  private String body(Request request) throws IOException {
    InputStream content = request.getEntity().getContent();
    return CharStreams.toString(new InputStreamReader(content, Charsets.UTF_8));
  }

}
