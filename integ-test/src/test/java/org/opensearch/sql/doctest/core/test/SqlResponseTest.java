/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core.test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.junit.Test;
import org.opensearch.client.Response;
import org.opensearch.sql.doctest.core.response.SqlResponse;

/**
 * Test cases for {@link SqlResponse}
 */
public class SqlResponseTest {

  @Test
  public void responseBodyShouldRetainNewLine() throws IOException {
    Response response = mock(Response.class);
    HttpEntity entity = mock(HttpEntity.class);
    String expected = "123\nabc\n";
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream(expected.getBytes()));

    SqlResponse sqlResponse = new SqlResponse(response);
    String actual = sqlResponse.body();
    assertThat(actual, is(expected));
  }

}
