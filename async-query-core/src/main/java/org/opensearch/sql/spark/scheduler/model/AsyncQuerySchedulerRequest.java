/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.model;

import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opensearch.sql.spark.rest.model.LangType;

/** Represents a job request for a scheduled task. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AsyncQuerySchedulerRequest {
  protected String accountId;
  // Scheduler jobid is the opensearch index name until we support multiple jobs per index
  protected String jobId;
  protected String dataSource;
  protected String scheduledQuery;
  protected LangType queryLang;
  protected Object schedule;
  protected boolean enabled;
  protected Instant lastUpdateTime;
  protected Instant enabledTime;
  protected Long lockDurationSeconds;
  protected Double jitter;
}
