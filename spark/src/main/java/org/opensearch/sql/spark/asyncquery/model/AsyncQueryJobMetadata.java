/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import com.google.gson.Gson;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/** This class models all the metadata required for a job. */
@Data
@AllArgsConstructor
public class AsyncQueryJobMetadata {
  private String jobId;
  private String applicationId;

  @Override
  public String toString() {
    return new Gson().toJson(this);
  }

  /**
   * Converts JobMetadata to XContentBuilder.
   *
   * @param metadata metadata.
   * @return XContentBuilder {@link XContentBuilder}
   * @throws Exception Exception.
   */
  public static XContentBuilder convertToXContent(AsyncQueryJobMetadata metadata) throws Exception {
    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    builder.field("jobId", metadata.getJobId());
    builder.field("applicationId", metadata.getApplicationId());
    builder.endObject();
    return builder;
  }

  /**
   * Converts json string to DataSourceMetadata.
   *
   * @param json jsonstring.
   * @return jobmetadata {@link AsyncQueryJobMetadata}
   * @throws java.io.IOException IOException.
   */
  public static AsyncQueryJobMetadata toJobMetadata(String json) throws IOException {
    try (XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json)) {
      return toJobMetadata(parser);
    }
  }

  /**
   * Convert xcontent parser to JobMetadata.
   *
   * @param parser parser.
   * @return JobMetadata {@link AsyncQueryJobMetadata}
   * @throws IOException IOException.
   */
  public static AsyncQueryJobMetadata toJobMetadata(XContentParser parser) throws IOException {
    String jobId = null;
    String applicationId = null;
    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
      String fieldName = parser.currentName();
      parser.nextToken();
      switch (fieldName) {
        case "jobId":
          jobId = parser.textOrNull();
          break;
        case "applicationId":
          applicationId = parser.textOrNull();
          break;
        default:
          throw new IllegalArgumentException("Unknown field: " + fieldName);
      }
    }
    if (jobId == null || applicationId == null) {
      throw new IllegalArgumentException("jobId and applicationId are required fields.");
    }
    return new AsyncQueryJobMetadata(jobId, applicationId);
  }
}
