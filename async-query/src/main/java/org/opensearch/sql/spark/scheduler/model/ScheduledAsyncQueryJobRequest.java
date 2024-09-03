/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.model;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.sql.spark.rest.model.LangType;

/** Represents a job request to refresh index. */
@Data
@EqualsAndHashCode(callSuper = true)
public class ScheduledAsyncQueryJobRequest extends AsyncQuerySchedulerRequest
    implements ScheduledJobParameter {
  // Constant fields for JSON serialization
  public static final String ACCOUNT_ID_FIELD = "accountId";
  public static final String JOB_ID_FIELD = "jobId";
  public static final String DATA_SOURCE_NAME_FIELD = "dataSource";
  public static final String SCHEDULED_QUERY_FIELD = "scheduledQuery";
  public static final String QUERY_LANG_FIELD = "queryLang";
  public static final String LAST_UPDATE_TIME_FIELD = "lastUpdateTime";
  public static final String SCHEDULE_FIELD = "schedule";
  public static final String ENABLED_TIME_FIELD = "enabledTime";
  public static final String LOCK_DURATION_SECONDS = "lockDurationSeconds";
  public static final String JITTER = "jitter";
  public static final String ENABLED_FIELD = "enabled";
  private final Schedule schedule;

  @Builder
  public ScheduledAsyncQueryJobRequest(
      String accountId,
      String jobId,
      String dataSource,
      String scheduledQuery,
      LangType queryLang,
      Schedule schedule, // Use the OpenSearch Schedule type
      boolean enabled,
      Instant lastUpdateTime,
      Instant enabledTime,
      Long lockDurationSeconds,
      Double jitter) {
    super(
        accountId,
        jobId,
        dataSource,
        scheduledQuery,
        queryLang,
        schedule,
        enabled,
        lastUpdateTime,
        enabledTime,
        lockDurationSeconds,
        jitter);
    this.schedule = schedule;
  }

  @Override
  public String getName() {
    return getJobId();
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
  public Schedule getSchedule() {
    return schedule;
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
    if (getAccountId() != null) {
      builder.field(ACCOUNT_ID_FIELD, getAccountId());
    }
    builder.field(JOB_ID_FIELD, getJobId()).field(ENABLED_FIELD, isEnabled());
    if (getDataSource() != null) {
      builder.field(DATA_SOURCE_NAME_FIELD, getDataSource());
    }
    if (getScheduledQuery() != null) {
      builder.field(SCHEDULED_QUERY_FIELD, getScheduledQuery());
    }
    if (getQueryLang() != null) {
      builder.field(QUERY_LANG_FIELD, getQueryLang());
    }
    if (getSchedule() != null) {
      builder.field(SCHEDULE_FIELD, getSchedule());
    }
    if (getEnabledTime() != null) {
      builder.field(ENABLED_TIME_FIELD, getEnabledTime().toEpochMilli());
    }
    builder.field(LAST_UPDATE_TIME_FIELD, getLastUpdateTime().toEpochMilli());
    if (this.lockDurationSeconds != null) {
      builder.field(LOCK_DURATION_SECONDS, this.lockDurationSeconds);
    }
    if (this.jitter != null) {
      builder.field(JITTER, this.jitter);
    }
    builder.endObject();
    return builder;
  }

  public static ScheduledAsyncQueryJobRequest fromAsyncQuerySchedulerRequest(
      AsyncQuerySchedulerRequest request) {
    return ScheduledAsyncQueryJobRequest.builder()
        .accountId(request.getAccountId())
        .jobId(request.getJobId())
        .dataSource(request.getDataSource())
        .scheduledQuery(request.getScheduledQuery())
        .queryLang(request.getQueryLang())
        .enabled(request.isEnabled())
        .lastUpdateTime(
            request.getLastUpdateTime() != null ? request.getLastUpdateTime() : Instant.now())
        .enabledTime(request.getEnabledTime())
        .lockDurationSeconds(request.getLockDurationSeconds())
        .jitter(request.getJitter())
        .schedule(
            parseSchedule(
                request.getSchedule())) // This is specific to ScheduledAsyncQueryJobRequest
        .build();
  }

  public static Schedule parseSchedule(Object rawSchedule) {
    return new IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES);
  }
}
