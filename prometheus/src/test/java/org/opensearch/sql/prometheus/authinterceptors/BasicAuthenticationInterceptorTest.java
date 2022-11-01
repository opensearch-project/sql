/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.authinterceptors;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import lombok.SneakyThrows;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.Request;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.internal.matchers.Null;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BasicAuthenticationInterceptorTest {

  @Mock
  private Interceptor.Chain chain;

  @Captor
  ArgumentCaptor<Request> requestArgumentCaptor;

  @Test
  void testConstructors() {
    Assertions.assertThrows(NullPointerException.class, () ->
        new BasicAuthenticationInterceptor(null, "test"));
    Assertions.assertThrows(NullPointerException.class, () ->
        new BasicAuthenticationInterceptor("testAdmin", null));
  }


  @Test
  @SneakyThrows
  void testIntercept() {
    when(chain.request()).thenReturn(new Request.Builder()
            .url("http://localhost:9090")
            .build());
    BasicAuthenticationInterceptor basicAuthenticationInterceptor
        = new BasicAuthenticationInterceptor("testAdmin", "testPassword");
    basicAuthenticationInterceptor.intercept(chain);
    verify(chain).proceed(requestArgumentCaptor.capture());
    Request request = requestArgumentCaptor.getValue();
    Assertions.assertEquals(
        Collections.singletonList(Credentials.basic("testAdmin", "testPassword")),
        request.headers("Authorization"));
  }

}
