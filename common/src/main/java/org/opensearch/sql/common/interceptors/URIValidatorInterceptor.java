/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.common.interceptors;

import java.io.IOException;
import java.util.List;
import lombok.NonNull;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.URIValidationUtils;

public class URIValidatorInterceptor implements Interceptor {

  private final List<String> denyHostList;

  public URIValidatorInterceptor(@NonNull List<String> denyHostList) {
    this.denyHostList = denyHostList;
  }

  @NotNull
  @Override
  public Response intercept(Interceptor.Chain chain) throws IOException {
    Request request = chain.request();
    String host = request.url().host();
    boolean isValidHost = URIValidationUtils.validateURIHost(host, denyHostList);
    if (isValidHost) {
      return chain.proceed(request);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Disallowed hostname in the uri. Validate with %s config",
              Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST.getKeyValue()));
    }
  }
}
