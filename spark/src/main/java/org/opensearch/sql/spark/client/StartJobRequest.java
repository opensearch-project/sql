/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * This POJO carries all the fields required for emr serverless job submission. Used as model in
 * {@link EMRServerlessClient} interface.
 */
@Data
@EqualsAndHashCode
public class StartJobRequest {

  public static final Long DEFAULT_JOB_TIMEOUT = 120L;

  private final String query;
  private final String jobName;
  private final String applicationId;
  private final String executionRoleArn;
  private final String sparkSubmitParams;
  private final Map<String, String> tags;

  /** true if it is Spark Structured Streaming job. */
  private final boolean isStructuredStreaming;

  private final String resultIndex;

  public Long executionTimeout() {
    return isStructuredStreaming ? 0L : DEFAULT_JOB_TIMEOUT;
  }
}
