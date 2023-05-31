/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import java.io.Serializable;
import java.util.Map;

/**
 * Flint Options include all the flint related configuration.
 */
public class FlintOptions implements Serializable {

  private final Map<String, String> options;

  public static final String HOST = "host";

  public static final String PORT = "port";

  public static final String REGION = "region";

  public static final String DEFAULT_REGION = "us-west-2";

  public static final String SCHEME = "scheme";

  public static final String AUTH = "auth";

  public static final String NONE_AUTH = "false";

  public static final String SIGV4_AUTH = "sigv4";

  /**
   * Used by {@link org.opensearch.flint.core.storage.OpenSearchScrollReader}
   */
  public static final String SCROLL_SIZE = "read.scroll_size";
  public static final int DEFAULT_SCROLL_SIZE = 100;

  public static final String REFRESH_POLICY = "write.refresh_policy";
  /**
   * NONE("false")
   *
   * IMMEDIATE("true")
   *
   * WAIT_UNTIL("wait_for")
   */
  public static final String DEFAULT_REFRESH_POLICY = "false";

  public FlintOptions(Map<String, String> options) {
    this.options = options;
  }

  public String getHost() {
    return options.getOrDefault(HOST, "localhost");
  }

  public int getPort() {
    return Integer.parseInt(options.getOrDefault(PORT, "9200"));
  }

  public int getScrollSize() {
    return Integer.parseInt(options.getOrDefault(SCROLL_SIZE, String.valueOf(DEFAULT_SCROLL_SIZE)));
  }

  public String getRefreshPolicy() {return options.getOrDefault(REFRESH_POLICY, DEFAULT_REFRESH_POLICY);}

  public String getRegion() {
    return options.getOrDefault(REGION, DEFAULT_REGION);
  }

  public String getScheme() {
    return options.getOrDefault(SCHEME, "http");
  }

  public String getAuth() {
    return options.getOrDefault(AUTH, NONE_AUTH);
  }
}
