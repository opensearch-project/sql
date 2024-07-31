/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.model;

import java.io.IOException;
import java.time.Instant;
import lombok.Builder;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;

/** Represents a job request to refresh index. */
@Builder
public class OpenSearchRefreshIndexJobRequest implements ScheduledJobParameter {
  // Constant fields for JSON serialization
  public static final String JOB_NAME_FIELD = "jobName";
  public static final String JOB_TYPE_FIELD = "jobType";
  public static final String LAST_UPDATE_TIME_FIELD = "lastUpdateTime";
  public static final String LAST_UPDATE_TIME_FIELD_READABLE = "last_update_time_field";
  public static final String SCHEDULE_FIELD = "schedule";
  public static final String ENABLED_TIME_FIELD = "enabledTime";
  public static final String ENABLED_TIME_FIELD_READABLE = "enabled_time_field";
  public static final String LOCK_DURATION_SECONDS = "lockDurationSeconds";
  public static final String JITTER = "jitter";
  public static final String ENABLED_FIELD = "enabled";

  // name is doc id
  private final String jobName;
  private final String jobType;
  private final Schedule schedule;
  private final boolean enabled;
  private final Instant lastUpdateTime;
  private final Instant enabledTime;
  private final Long lockDurationSeconds;
  private final Double jitter;

  @Override
  public String getName() {
    return jobName;
  }

  public String getJobType() {
    return jobType;
  }

  @Override
  public Schedule getSchedule() {
    return schedule;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public Instant getLastUpdateTime() {
    return lastUpdateTime;
  }

  @Override
  public Instant getEnabledTime() {
    return enabledTime;
  }

  @Override
  public Long getLockDurationSeconds() {
    return lockDurationSeconds;
  }

  @Override
  public Double getJitter() {
    return jitter;
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params)
      throws IOException {
    builder.startObject();
    builder.field(JOB_NAME_FIELD, getName()).field(ENABLED_FIELD, isEnabled());
    if (getSchedule() != null) {
      builder.field(SCHEDULE_FIELD, getSchedule());
    }
    if (getJobType() != null) {
      builder.field(JOB_TYPE_FIELD, getJobType());
    }
    if (getEnabledTime() != null) {
      builder.timeField(
          ENABLED_TIME_FIELD, ENABLED_TIME_FIELD_READABLE, getEnabledTime().toEpochMilli());
    }
    builder.timeField(
        LAST_UPDATE_TIME_FIELD,
        LAST_UPDATE_TIME_FIELD_READABLE,
        getLastUpdateTime().toEpochMilli());
    if (this.lockDurationSeconds != null) {
      builder.field(LOCK_DURATION_SECONDS, this.lockDurationSeconds);
    }
    if (this.jitter != null) {
      builder.field(JITTER, this.jitter);
    }
    builder.endObject();
    return builder;
  }
}
