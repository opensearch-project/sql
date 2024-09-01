/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.sql.spark.rest.model.LangType;

public class ScheduledAsyncQueryJobRequestTest {

  @Test
  public void testBuilderAndGetterMethods() {
    Instant now = Instant.now();
    IntervalSchedule schedule = new IntervalSchedule(now, 1, ChronoUnit.MINUTES);

    ScheduledAsyncQueryJobRequest jobRequest =
        ScheduledAsyncQueryJobRequest.builder()
            .accountId("testAccount")
            .jobId("testJob")
            .dataSource("testDataSource")
            .scheduledQuery("SELECT * FROM test")
            .queryLang(LangType.SQL)
            .schedule(schedule)
            .enabled(true)
            .lastUpdateTime(now)
            .enabledTime(now)
            .lockDurationSeconds(60L)
            .jitter(0.1)
            .build();

    assertEquals("testAccount", jobRequest.getAccountId());
    assertEquals("testJob", jobRequest.getJobId());
    assertEquals("testJob", jobRequest.getName());
    assertEquals("testDataSource", jobRequest.getDataSource());
    assertEquals("SELECT * FROM test", jobRequest.getScheduledQuery());
    assertEquals(LangType.SQL, jobRequest.getQueryLang());
    assertEquals(schedule, jobRequest.getSchedule());
    assertTrue(jobRequest.isEnabled());
    assertEquals(now, jobRequest.getLastUpdateTime());
    assertEquals(now, jobRequest.getEnabledTime());
    assertEquals(60L, jobRequest.getLockDurationSeconds());
    assertEquals(0.1, jobRequest.getJitter());
  }

  @Test
  public void testToXContent() throws IOException {
    Instant now = Instant.now();
    IntervalSchedule schedule = new IntervalSchedule(now, 1, ChronoUnit.MINUTES);

    ScheduledAsyncQueryJobRequest request =
        ScheduledAsyncQueryJobRequest.builder()
            .accountId("testAccount")
            .jobId("testJob")
            .dataSource("testDataSource")
            .scheduledQuery("SELECT * FROM test")
            .queryLang(LangType.SQL)
            .schedule(schedule)
            .enabled(true)
            .enabledTime(now)
            .lastUpdateTime(now)
            .lockDurationSeconds(60L)
            .jitter(0.1)
            .build();

    XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
    request.toXContent(builder, EMPTY_PARAMS);
    String jsonString = builder.toString();

    assertTrue(jsonString.contains("\"accountId\" : \"testAccount\""));
    assertTrue(jsonString.contains("\"jobId\" : \"testJob\""));
    assertTrue(jsonString.contains("\"dataSource\" : \"testDataSource\""));
    assertTrue(jsonString.contains("\"scheduledQuery\" : \"SELECT * FROM test\""));
    assertTrue(jsonString.contains("\"queryLang\" : \"SQL\""));
    assertTrue(jsonString.contains("\"start_time\" : " + now.toEpochMilli()));
    assertTrue(jsonString.contains("\"period\" : 1"));
    assertTrue(jsonString.contains("\"unit\" : \"Minutes\""));
    assertTrue(jsonString.contains("\"enabled\" : true"));
    assertTrue(jsonString.contains("\"lastUpdateTime\" : " + now.toEpochMilli()));
    assertTrue(jsonString.contains("\"enabledTime\" : " + now.toEpochMilli()));
    assertTrue(jsonString.contains("\"lockDurationSeconds\" : 60"));
    assertTrue(jsonString.contains("\"jitter\" : 0.1"));
  }
}
