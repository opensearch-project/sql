/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import java.util.Locale;
import java.util.Map;
import lombok.Data;

@Data
public class FlintIndexMetadata {
  public static final String PROPERTIES_KEY = "properties";
  public static final String ENV_KEY = "env";
  public static final String OPTIONS_KEY = "options";

  public static final String SERVERLESS_EMR_JOB_ID = "SERVERLESS_EMR_JOB_ID";
  public static final String AUTO_REFRESH = "auto_refresh";
  public static final String AUTO_REFRESH_DEFAULT = "false";

  private final String jobId;
  private final boolean autoRefresh;

  public static FlintIndexMetadata fromMetatdata(Map<String, Object> metaMap) {
    Map<String, Object> propertiesMap = (Map<String, Object>) metaMap.get(PROPERTIES_KEY);
    Map<String, Object> envMap = (Map<String, Object>) propertiesMap.get(ENV_KEY);
    Map<String, Object> options = (Map<String, Object>) metaMap.get(OPTIONS_KEY);
    String jobId = (String) envMap.get(SERVERLESS_EMR_JOB_ID);

    boolean autoRefresh =
        !((String) options.getOrDefault(AUTO_REFRESH, AUTO_REFRESH_DEFAULT))
            .toLowerCase(Locale.ROOT)
            .equalsIgnoreCase(AUTO_REFRESH_DEFAULT);
    return new FlintIndexMetadata(jobId, autoRefresh);
  }
}
