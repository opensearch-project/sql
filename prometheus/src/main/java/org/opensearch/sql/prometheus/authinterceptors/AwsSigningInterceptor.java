/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.authinterceptors;

import com.babbel.mobile.android.commons.okhttpawssigner.OkHttpAwsV4Signer;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import lombok.NonNull;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

public class AwsSigningInterceptor implements Interceptor {

  private OkHttpAwsV4Signer okHttpAwsV4Signer;

  private String accessKey;

  private String secretKey;

  /**
   * AwsSigningInterceptor which intercepts http requests
   * and adds required headers for sigv4 authentication.
   *
   * @param accessKey accessKey.
   * @param secretKey secretKey.
   * @param region region.
   * @param serviceName serviceName.
   */
  public AwsSigningInterceptor(@NonNull String accessKey, @NonNull String secretKey,
                               @NonNull String region, @NonNull String serviceName) {
    this.okHttpAwsV4Signer = new OkHttpAwsV4Signer(region, serviceName);
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  @Override
  public Response intercept(Interceptor.Chain chain) throws IOException {
    Request request = chain.request();

    DateTimeFormatter timestampFormat = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
        .withZone(ZoneId.of("GMT"));

    Request newRequest = request.newBuilder()
        .addHeader("x-amz-date", timestampFormat.format(ZonedDateTime.now()))
        .addHeader("host", request.url().host())
        .build();
    Request signed = okHttpAwsV4Signer.sign(newRequest, accessKey, secretKey);
    return chain.proceed(signed);
  }

}
