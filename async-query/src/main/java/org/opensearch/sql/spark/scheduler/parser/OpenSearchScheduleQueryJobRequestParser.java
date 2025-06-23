/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.parser;

import java.io.IOException;
import java.time.Instant;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.scheduler.model.ScheduledAsyncQueryJobRequest;

public class OpenSearchScheduleQueryJobRequestParser {

  private static Instant parseInstantValue(XContentParser parser) throws IOException {
    if (XContentParser.Token.VALUE_NULL.equals(parser.currentToken())) {
      return null;
    }
    if (parser.currentToken().isValue()) {
      return Instant.ofEpochMilli(parser.longValue());
    }
    XContentParserUtils.throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
    return null;
  }

  public static ScheduledJobParser getJobParser() {
    return (parser, id, jobDocVersion) -> {
      ScheduledAsyncQueryJobRequest.ScheduledAsyncQueryJobRequestBuilder builder =
          ScheduledAsyncQueryJobRequest.scheduledAsyncQueryJobRequestBuilder();
      XContentParserUtils.ensureExpectedToken(
          XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

      while (!parser.nextToken().equals(XContentParser.Token.END_OBJECT)) {
        String fieldName = parser.currentName();
        parser.nextToken();
        switch (fieldName) {
          case ScheduledAsyncQueryJobRequest.ACCOUNT_ID_FIELD:
            builder.accountId(parser.text());
            break;
          case ScheduledAsyncQueryJobRequest.JOB_ID_FIELD:
            builder.jobId(parser.text());
            break;
          case ScheduledAsyncQueryJobRequest.DATA_SOURCE_NAME_FIELD:
            builder.dataSource(parser.text());
            break;
          case ScheduledAsyncQueryJobRequest.SCHEDULED_QUERY_FIELD:
            builder.scheduledQuery(parser.text());
            break;
          case ScheduledAsyncQueryJobRequest.QUERY_LANG_FIELD:
            builder.queryLang(LangType.fromString(parser.text()));
            break;
          case ScheduledAsyncQueryJobRequest.ENABLED_FIELD:
            builder.enabled(parser.booleanValue());
            break;
          case ScheduledAsyncQueryJobRequest.ENABLED_TIME_FIELD:
            builder.enabledTime(parseInstantValue(parser));
            break;
          case ScheduledAsyncQueryJobRequest.LAST_UPDATE_TIME_FIELD:
            builder.lastUpdateTime(parseInstantValue(parser));
            break;
          case ScheduledAsyncQueryJobRequest.SCHEDULE_FIELD:
            builder.schedule(ScheduleParser.parse(parser));
            break;
          case ScheduledAsyncQueryJobRequest.LOCK_DURATION_SECONDS:
            builder.lockDurationSeconds(parser.longValue());
            break;
          case ScheduledAsyncQueryJobRequest.JITTER:
            builder.jitter(parser.doubleValue());
            break;
          default:
            XContentParserUtils.throwUnknownToken(parser.currentToken(), parser.getTokenLocation());
        }
      }
      return builder.build();
    };
  }
}
