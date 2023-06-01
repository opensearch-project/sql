/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.common.authinterceptors.credentialsprovider;

import java.io.IOException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class handles client configuration for AWS ES internal service calls.
 */
public class InternalAuthCredentialsClient {

  private static final Logger LOG = LogManager.getLogger();

  private static final int TIMEOUT_MILLISECONDS = 5000;
  private static final int SOCKET_TIMEOUT_MILLISECONDS = 70000;

  private static final CloseableHttpClient HTTP_CLIENT;

  static {
    HTTP_CLIENT = createHttpClient();
  }

  /**
  * Get AWSCredentials from Fire Flower for given policyType.
  */
  public InternalAwsCredentials getAwsCredentials(String policyType) {
    try {
      InternalAwsCredentials internalAwsCredentials = getInternalAwsCredentials(policyType);
      return !internalAwsCredentials.isEmpty() ? internalAwsCredentials : null;
    } catch (IOException e) {
      LOG.error("Could not fetch AWS credentials", e);
      return null;
    }
  }

  private static CloseableHttpClient createHttpClient() {
    RequestConfig config = RequestConfig.custom()
        .setConnectTimeout(TIMEOUT_MILLISECONDS)
        .setConnectionRequestTimeout(TIMEOUT_MILLISECONDS)
        .setSocketTimeout(SOCKET_TIMEOUT_MILLISECONDS)
        .build();

    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setDefaultMaxPerRoute(5);

    return HttpClientBuilder.create()
        .setDefaultRequestConfig(config)
        .setConnectionManager(connectionManager)
        .setRetryHandler(new DefaultHttpRequestRetryHandler())
        .build();
  }

  private InternalAwsCredentials getInternalAwsCredentials(String policyType) throws IOException {
    return (new InternalAuthCredentialsApiRequest(HTTP_CLIENT, policyType)).execute();
  }
}