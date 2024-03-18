/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Model to store flint index options. Currently added fields which are required, and we can extend
 * this in the future.
 */
public class FlintIndexOptions {

  public static final String AUTO_REFRESH = "auto_refresh";
  public static final String INCREMENTAL_REFRESH = "incremental_refresh";
  public static final String CHECKPOINT_LOCATION = "checkpoint_location";
  public static final String WATERMARK_DELAY = "watermark_delay";
  private final Map<String, String> options = new HashMap<>();

  public void setOption(String key, String value) {
    options.put(key, value);
  }

  public Optional<String> getOption(String key) {
    return Optional.ofNullable(options.get(key));
  }

  public boolean autoRefresh() {
    return Boolean.parseBoolean(getOption(AUTO_REFRESH).orElse("false"));
  }

  public Map<String, String> getProvidedOptions() {
    return new HashMap<>(options);
  }
}
