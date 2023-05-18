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
  /**
   * Used by {@link org.opensearch.flint.core.storage.OpenSearchScrollReader}
   */
  public static final String SCROLL_SIZE = "scroll_size";
  public static final int DEFAULT_SCROLL_SIZE = 100;

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
}
