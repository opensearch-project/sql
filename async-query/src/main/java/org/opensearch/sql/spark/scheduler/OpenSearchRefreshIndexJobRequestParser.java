/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler;

import java.io.IOException;
import java.time.Instant;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.schedule.ScheduleParser;
import org.opensearch.sql.spark.scheduler.model.OpenSearchRefreshIndexJobRequest;

public class OpenSearchRefreshIndexJobRequestParser {

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
      OpenSearchRefreshIndexJobRequest.OpenSearchRefreshIndexJobRequestBuilder builder =
          OpenSearchRefreshIndexJobRequest.builder();
      XContentParserUtils.ensureExpectedToken(
          XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

      while (!parser.nextToken().equals(XContentParser.Token.END_OBJECT)) {
        String fieldName = parser.currentName();
        parser.nextToken();
        switch (fieldName) {
          case OpenSearchRefreshIndexJobRequest.JOB_NAME_FIELD:
            builder.jobName(parser.text());
            break;
          case OpenSearchRefreshIndexJobRequest.JOB_TYPE_FIELD:
            builder.jobType(parser.text());
            break;
          case OpenSearchRefreshIndexJobRequest.ENABLED_FIELD:
            builder.enabled(parser.booleanValue());
            break;
          case OpenSearchRefreshIndexJobRequest.ENABLED_TIME_FIELD:
            builder.enabledTime(parseInstantValue(parser));
            break;
          case OpenSearchRefreshIndexJobRequest.LAST_UPDATE_TIME_FIELD:
            builder.lastUpdateTime(parseInstantValue(parser));
            break;
          case OpenSearchRefreshIndexJobRequest.SCHEDULE_FIELD:
            builder.schedule(ScheduleParser.parse(parser));
            break;
          case OpenSearchRefreshIndexJobRequest.LOCK_DURATION_SECONDS:
            builder.lockDurationSeconds(parser.longValue());
            break;
          case OpenSearchRefreshIndexJobRequest.JITTER:
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
