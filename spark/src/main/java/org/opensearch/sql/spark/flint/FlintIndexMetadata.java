/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.flint;

import java.util.Optional;
import lombok.Builder;
import lombok.Data;
import org.opensearch.sql.spark.dispatcher.model.FlintIndexOptions;

@Data
@Builder
public class FlintIndexMetadata {

  public static final String META_KEY = "_meta";
  public static final String LATEST_ID_KEY = "latestId";
  public static final String KIND_KEY = "kind";
  public static final String INDEXED_COLUMNS_KEY = "indexedColumns";
  public static final String NAME_KEY = "name";
  public static final String OPTIONS_KEY = "options";
  public static final String SOURCE_KEY = "source";
  public static final String VERSION_KEY = "version";
  public static final String PROPERTIES_KEY = "properties";
  public static final String ENV_KEY = "env";
  public static final String SERVERLESS_EMR_JOB_ID = "SERVERLESS_EMR_JOB_ID";
  public static final String APP_ID = "SERVERLESS_EMR_VIRTUAL_CLUSTER_ID";

  private final String opensearchIndexName;
  private final String jobId;
  private final String appId;
  private final String latestId;
  private final FlintIndexOptions flintIndexOptions;

  public Optional<String> getLatestId() {
    return Optional.ofNullable(latestId);
  }
}
