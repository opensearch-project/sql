/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.common.authinterceptors;

import java.io.IOException;
import lombok.NonNull;
import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

public class BasicAuthenticationInterceptor implements Interceptor {

  private String credentials;

  public BasicAuthenticationInterceptor(@NonNull String username, @NonNull String password) {
    this.credentials = Credentials.basic(username, password);
  }

  @Override
  public Response intercept(Interceptor.Chain chain) throws IOException {
    Request request = chain.request();
    Request authenticatedRequest =
        request.newBuilder().header("Authorization", credentials).build();
    return chain.proceed(authenticatedRequest);
  }
}
