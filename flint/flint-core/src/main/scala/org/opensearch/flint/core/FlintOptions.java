/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import java.io.Serializable;
import java.util.Map;
import java.util.NoSuchElementException;

public class FlintOptions implements Serializable {
  private Map<String, String> options;

  public static final String INDEX_NAME = "index";
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

  public String getIndexName() {
    if(options.containsKey("path")) {
      return options.get("path");
    } else if(options.containsKey(INDEX_NAME)) {
      return options.get(INDEX_NAME);
    } else {
      throw new NoSuchElementException("index or path not found");
    }
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
