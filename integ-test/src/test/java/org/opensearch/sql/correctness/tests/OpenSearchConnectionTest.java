/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.tests;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.sql.correctness.runner.connection.OpenSearchConnection;

/** Tests for {@link OpenSearchConnection} */
@RunWith(MockitoJUnitRunner.class)
public class OpenSearchConnectionTest {

  @Mock private RestClient client;

  private OpenSearchConnection conn;

  @Before
  public void setUp() throws IOException {
    conn = new OpenSearchConnection("jdbc:opensearch://localhost:12345", client);

    Response response = mock(Response.class);
    when(client.performRequest(any(Request.class))).thenReturn(response);
    when(response.getStatusLine())
        .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 2, 0), 200, ""));
  }

  @Test
  public void testCreateTable() throws IOException {
    conn.create("test", "mapping");

    Request actual = captureActualArg();
    assertEquals("PUT", actual.getMethod());
    assertEquals("/test", actual.getEndpoint());
    assertEquals("mapping", getBody(actual));
  }

  @Test
  public void testInsertData() throws IOException {
    conn.insert(
        "test", new String[] {"name"}, Arrays.asList(new String[] {"John"}, new String[] {"Hank"}));

    Request actual = captureActualArg();
    assertEquals("POST", actual.getMethod());
    assertEquals("/test/_bulk?refresh=true", actual.getEndpoint());
    assertEquals(
        "{\"index\":{}}\n{\"name\":\"John\"}\n{\"index\":{}}\n{\"name\":\"Hank\"}\n",
        getBody(actual));
  }

  @Test
  public void testInsertNullData() throws IOException {
    conn.insert(
        "test",
        new String[] {"name", "age"},
        Arrays.asList(new Object[] {null, 30}, new Object[] {"Hank", null}));

    Request actual = captureActualArg();
    assertEquals("POST", actual.getMethod());
    assertEquals("/test/_bulk?refresh=true", actual.getEndpoint());
    assertEquals(
        "{\"index\":{}}\n{\"age\":30}\n{\"index\":{}}\n{\"name\":\"Hank\"}\n", getBody(actual));
  }

  @Test
  public void testDropTable() throws IOException {
    conn.drop("test");

    Request actual = captureActualArg();
    assertEquals("DELETE", actual.getMethod());
    assertEquals("/test", actual.getEndpoint());
  }

  private Request captureActualArg() throws IOException {
    ArgumentCaptor<Request> argCap = ArgumentCaptor.forClass(Request.class);
    verify(client).performRequest(argCap.capture());
    return argCap.getValue();
  }

  private String getBody(Request request) throws IOException {
    InputStream inputStream = request.getEntity().getContent();
    return CharStreams.toString(new InputStreamReader(inputStream));
  }
}
