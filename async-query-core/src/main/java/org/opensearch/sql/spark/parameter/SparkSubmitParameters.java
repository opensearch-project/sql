/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DEFAULT_CLASS_NAME;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Setter;

/** Define Spark Submit Parameters. */
public class SparkSubmitParameters {
  public static final String SPACE = " ";
  public static final String EQUALS = "=";

  @Setter private String className = DEFAULT_CLASS_NAME;
  private final Map<String, String> config = new LinkedHashMap<>();

  /** Extra parameters to append finally */
  @Setter private String extraParameters;

  public void setConfigItem(String key, String value) {
    config.put(key, value);
  }

  public void deleteConfigItem(String key) {
    config.remove(key);
  }

  public String getConfigItem(String key) {
    return config.get(key);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(" --class ");
    stringBuilder.append(this.className);
    stringBuilder.append(SPACE);
    for (String key : config.keySet()) {
      stringBuilder.append(" --conf ");
      stringBuilder.append(key);
      stringBuilder.append(EQUALS);
      stringBuilder.append(config.get(key));
      stringBuilder.append(SPACE);
    }

    if (extraParameters != null) {
      stringBuilder.append(extraParameters);
    }
    return stringBuilder.toString();
  }
}
