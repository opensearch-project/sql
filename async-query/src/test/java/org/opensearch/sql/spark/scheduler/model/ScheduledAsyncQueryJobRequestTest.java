/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.model;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
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

  @Test
  public void testFromAsyncQuerySchedulerRequest() {
    Instant now = Instant.now();
    AsyncQuerySchedulerRequest request = new AsyncQuerySchedulerRequest();
    request.setJobId("testJob");
    request.setAccountId("testAccount");
    request.setDataSource("testDataSource");
    request.setScheduledQuery("SELECT * FROM test");
    request.setQueryLang(LangType.SQL);
    request.setSchedule("1 minutes");
    request.setEnabled(true);
    request.setLastUpdateTime(now);
    request.setLockDurationSeconds(60L);
    request.setJitter(0.1);

    ScheduledAsyncQueryJobRequest jobRequest =
        ScheduledAsyncQueryJobRequest.fromAsyncQuerySchedulerRequest(request);

    assertEquals("testJob", jobRequest.getJobId());
    assertEquals("testAccount", jobRequest.getAccountId());
    assertEquals("testDataSource", jobRequest.getDataSource());
    assertEquals("SELECT * FROM test", jobRequest.getScheduledQuery());
    assertEquals(LangType.SQL, jobRequest.getQueryLang());
    assertEquals(new IntervalSchedule(now, 1, ChronoUnit.MINUTES), jobRequest.getSchedule());
    assertTrue(jobRequest.isEnabled());
    assertEquals(60L, jobRequest.getLockDurationSeconds());
    assertEquals(0.1, jobRequest.getJitter());
  }

  @Test
  public void testFromAsyncQuerySchedulerRequestWithInvalidSchedule() {
    AsyncQuerySchedulerRequest request = new AsyncQuerySchedulerRequest();
    request.setJobId("testJob");
    request.setSchedule(new Object()); // Set schedule to a non-String object

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> {
              ScheduledAsyncQueryJobRequest.fromAsyncQuerySchedulerRequest(request);
            });

    assertEquals("Schedule must be a String object for parsing.", exception.getMessage());
  }

  @Test
  public void testEqualsAndHashCode() {
    Instant now = Instant.now();
    IntervalSchedule schedule = new IntervalSchedule(now, 1, ChronoUnit.MINUTES);

    ScheduledAsyncQueryJobRequest request1 =
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

    // Test toString
    String toString = request1.toString();
    assertTrue(toString.contains("accountId=testAccount"));
    assertTrue(toString.contains("jobId=testJob"));
    assertTrue(toString.contains("dataSource=testDataSource"));
    assertTrue(toString.contains("scheduledQuery=SELECT * FROM test"));
    assertTrue(toString.contains("queryLang=SQL"));
    assertTrue(toString.contains("enabled=true"));
    assertTrue(toString.contains("lockDurationSeconds=60"));
    assertTrue(toString.contains("jitter=0.1"));

    ScheduledAsyncQueryJobRequest request2 =
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

    assertEquals(request1, request2);
    assertEquals(request1.hashCode(), request2.hashCode());

    ScheduledAsyncQueryJobRequest request3 =
        ScheduledAsyncQueryJobRequest.builder()
            .accountId("differentAccount")
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

    assertNotEquals(request1, request3);
    assertNotEquals(request1.hashCode(), request3.hashCode());
  }
}
