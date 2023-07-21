/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.common.authinterceptors;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.babbel.mobile.android.commons.okhttpawssigner.OkHttpAwsV4Signer;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import lombok.NonNull;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AwsSigningInterceptor implements Interceptor {

  private OkHttpAwsV4Signer okHttpAwsV4Signer;

  private AWSCredentialsProvider awsCredentialsProvider;

  private static final Logger LOG = LogManager.getLogger();

  /**
   * AwsSigningInterceptor which intercepts http requests
   * and adds required headers for sigv4 authentication.
   *
   * @param awsCredentialsProvider awsCredentialsProvider.
   * @param region region.
   * @param serviceName serviceName.
   */
  public AwsSigningInterceptor(@NonNull AWSCredentialsProvider awsCredentialsProvider,
                               @NonNull String region, @NonNull String serviceName) {
    this.okHttpAwsV4Signer = new OkHttpAwsV4Signer(region, serviceName);
    this.awsCredentialsProvider = awsCredentialsProvider;
  }

  @Override
  public Response intercept(Interceptor.Chain chain) throws IOException {
    Request request = chain.request();

    DateTimeFormatter timestampFormat = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
        .withZone(ZoneId.of("GMT"));


    Request.Builder newRequestBuilder = request.newBuilder()
        .addHeader("x-amz-date", timestampFormat.format(ZonedDateTime.now()))
        .addHeader("host", request.url().host());

    AWSCredentials awsCredentials = awsCredentialsProvider.getCredentials();
    if (awsCredentialsProvider instanceof STSAssumeRoleSessionCredentialsProvider) {
      newRequestBuilder.addHeader("x-amz-security-token",
          ((STSAssumeRoleSessionCredentialsProvider) awsCredentialsProvider)
              .getCredentials()
              .getSessionToken());
    }
    Request newRequest = newRequestBuilder.build();
    Request signed = okHttpAwsV4Signer.sign(newRequest,
        awsCredentials.getAWSAccessKeyId(), awsCredentials.getAWSSecretKey());
    return chain.proceed(signed);
  }

}
