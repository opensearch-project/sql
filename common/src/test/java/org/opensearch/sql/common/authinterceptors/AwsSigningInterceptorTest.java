/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.common.authinterceptors;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import lombok.SneakyThrows;
import okhttp3.Interceptor;
import okhttp3.Request;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AwsSigningInterceptorTest {

  @Mock private Interceptor.Chain chain;

  @Captor ArgumentCaptor<Request> requestArgumentCaptor;

  @Mock private STSAssumeRoleSessionCredentialsProvider stsAssumeRoleSessionCredentialsProvider;

  @Test
  void testConstructors() {
    Assertions.assertThrows(
        NullPointerException.class, () -> new AwsSigningInterceptor(null, "us-east-1", "aps"));
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new AwsSigningInterceptor(
                getStaticAWSCredentialsProvider("accessKey", "secretKey"), null, "aps"));
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new AwsSigningInterceptor(
                getStaticAWSCredentialsProvider("accessKey", "secretKey"), "us-east-1", null));
  }

  @Test
  @SneakyThrows
  void testIntercept() {
    Mockito.when(chain.request())
        .thenReturn(new Request.Builder().url("http://localhost:9090").build());
    AwsSigningInterceptor awsSigningInterceptor =
        new AwsSigningInterceptor(
            getStaticAWSCredentialsProvider("testAccessKey", "testSecretKey"), "us-east-1", "aps");
    awsSigningInterceptor.intercept(chain);
    Mockito.verify(chain).proceed(requestArgumentCaptor.capture());
    Request request = requestArgumentCaptor.getValue();
    Assertions.assertNotNull(request.headers("Authorization"));
    Assertions.assertNotNull(request.headers("x-amz-date"));
    Assertions.assertNotNull(request.headers("host"));
  }

  @Test
  @SneakyThrows
  void testSTSCredentialsProviderInterceptor() {
    Mockito.when(chain.request())
        .thenReturn(new Request.Builder().url("http://localhost:9090").build());
    Mockito.when(stsAssumeRoleSessionCredentialsProvider.getCredentials())
        .thenReturn(getAWSSessionCredentials());
    AwsSigningInterceptor awsSigningInterceptor =
        new AwsSigningInterceptor(stsAssumeRoleSessionCredentialsProvider, "us-east-1", "aps");
    awsSigningInterceptor.intercept(chain);
    Mockito.verify(chain).proceed(requestArgumentCaptor.capture());
    Request request = requestArgumentCaptor.getValue();
    Assertions.assertNotNull(request.headers("Authorization"));
    Assertions.assertNotNull(request.headers("x-amz-date"));
    Assertions.assertNotNull(request.headers("host"));
    Assertions.assertEquals("session_token", request.headers("x-amz-security-token").get(0));
  }

  private AWSCredentialsProvider getStaticAWSCredentialsProvider(
      String accessKey, String secretKey) {
    return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
  }

  private AWSSessionCredentials getAWSSessionCredentials() {
    return new AWSSessionCredentials() {
      @Override
      public String getSessionToken() {
        return "session_token";
      }

      @Override
      public String getAWSAccessKeyId() {
        return "access_key";
      }

      @Override
      public String getAWSSecretKey() {
        return "secret_key";
      }
    };
  }
}
