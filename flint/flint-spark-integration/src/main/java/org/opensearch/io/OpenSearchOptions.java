/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.io;

import java.util.Map;

public class OpenSearchOptions {

  public static final String INDEX_NAME = "index";

  public static final String HOST = "host";

  public static final String PORT = "port";

  private final Map<String, String> options;

  public OpenSearchOptions(Map<String, String> options) {
    this.options = options;
  }

  public String getIndexName() {
    return options.get(INDEX_NAME);
  }

  public String getHost() {
    return options.get(HOST);
  }

  public int getPort() {
    return Integer.parseInt(options.get(PORT));
  }
}
