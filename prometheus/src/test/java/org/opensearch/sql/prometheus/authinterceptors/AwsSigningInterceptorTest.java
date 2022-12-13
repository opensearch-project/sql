/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.authinterceptors;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import lombok.SneakyThrows;
import okhttp3.Interceptor;
import okhttp3.Request;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AwsSigningInterceptorTest {

  @Mock
  private Interceptor.Chain chain;

  @Captor
  ArgumentCaptor<Request> requestArgumentCaptor;

  @Test
  void testConstructors() {
    Assertions.assertThrows(NullPointerException.class, () ->
        new AwsSigningInterceptor(null, "secretKey", "us-east-1", "aps"));
    Assertions.assertThrows(NullPointerException.class, () ->
        new AwsSigningInterceptor("accessKey", null, "us-east-1", "aps"));
    Assertions.assertThrows(NullPointerException.class, () ->
        new AwsSigningInterceptor("accessKey", "secretKey", null, "aps"));
    Assertions.assertThrows(NullPointerException.class, () ->
        new AwsSigningInterceptor("accessKey", "secretKey", "us-east-1", null));
  }

  @Test
  @SneakyThrows
  void testIntercept() {
    when(chain.request()).thenReturn(new Request.Builder()
        .url("http://localhost:9090")
        .build());
    AwsSigningInterceptor awsSigningInterceptor
        = new AwsSigningInterceptor("testAccessKey", "testSecretKey", "us-east-1", "aps");
    awsSigningInterceptor.intercept(chain);
    verify(chain).proceed(requestArgumentCaptor.capture());
    Request request = requestArgumentCaptor.getValue();
    Assertions.assertNotNull(request.headers("Authorization"));
    Assertions.assertNotNull(request.headers("x-amz-date"));
    Assertions.assertNotNull(request.headers("host"));
  }

}
