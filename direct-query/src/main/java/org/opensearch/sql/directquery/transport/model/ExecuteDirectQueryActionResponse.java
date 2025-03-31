/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.directquery.transport.model.datasource.DataSourceResult;
import org.opensearch.sql.directquery.transport.model.datasource.PrometheusResult;

@RequiredArgsConstructor
public class ExecuteDirectQueryActionResponse extends ActionResponse {

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @Getter private final String queryId;
  @Getter private final Map<String, DataSourceResult> results;
  @Getter private final String sessionId;

  /**
   * Constructor for creating a response from raw result strings.
   *
   * @param queryId ID of the executed query
   * @param result Raw JSON result string
   * @param sessionId Session ID for the query
   * @param dataSources Data source information
   * @throws IOException If there is an error parsing the results
   */
  public ExecuteDirectQueryActionResponse(
      String queryId, String result, String sessionId, String dataSourceName, String dataSourceType)
      throws IOException {
    this.queryId = queryId;
    this.sessionId = sessionId;
    this.results = parseResult(result, dataSourceName, dataSourceType);
  }

  public ExecuteDirectQueryActionResponse(StreamInput in) throws IOException {
    super(in);
    queryId = in.readString();

    // Read the number of data source results
    int resultCount = in.readInt();
    results = new HashMap<>(resultCount);

    // Read each data source result
    for (int i = 0; i < resultCount; i++) {
      String dataSourceId = in.readString();
      String dataSourceType = in.readString();
      String resultJson = in.readString();

      // Create appropriate DataSourceResult based on type - using privileged action
      DataSourceResult result;
      switch (dataSourceType) {
        case "prometheus":
          result =
              AccessController.doPrivileged(
                  (PrivilegedAction<PrometheusResult>)
                      () -> {
                        try {
                          return OBJECT_MAPPER.readValue(resultJson, PrometheusResult.class);
                        } catch (IOException e) {
                          throw new RuntimeException("Failed to deserialize Prometheus result", e);
                        }
                      });
          break;
          // Add cases for other data source types as they're implemented
        default:
          throw new IOException("Unsupported data source type: " + dataSourceType);
      }

      results.put(dataSourceId, result);
    }

    sessionId = in.readOptionalString();
  }

  @Override
  public void writeTo(StreamOutput streamOutput) throws IOException {
    streamOutput.writeString(queryId);

    // Write the number of data source results
    streamOutput.writeInt(results.size());

    // Write each data source result
    for (Map.Entry<String, DataSourceResult> entry : results.entrySet()) {
      streamOutput.writeString(entry.getKey()); // data source ID

      DataSourceResult result = entry.getValue();
      if (result instanceof PrometheusResult) {
        streamOutput.writeString("prometheus");
      }
      // Add other types as they're implemented
      else {
        throw new IOException("Unsupported DataSourceResult type: " + result.getClass().getName());
      }

      // Serialize the data source result to JSON - using privileged action
      final String serializedResult =
          AccessController.doPrivileged(
              (PrivilegedAction<String>)
                  () -> {
                    try {
                      return OBJECT_MAPPER.writeValueAsString(result);
                    } catch (IOException e) {
                      throw new RuntimeException("Failed to serialize result", e);
                    }
                  });
      streamOutput.writeString(serializedResult);
    }

    streamOutput.writeOptionalString(sessionId);
  }

  /**
   * Parse the raw JSON result string into appropriate DataSourceResult objects. This method
   * supports different data source types by determining the type from the JSON content or explicit
   * type information.
   *
   * @param rawResult Raw JSON result string to parse
   * @param dataSourceName Name of the data source that generated this result
   * @return Map of data source names to their respective typed result objects
   * @throws IOException If there is an error parsing the JSON or creating result objects
   */
  private Map<String, DataSourceResult> parseResult(
      String rawResult, String dataSourceName, String dataSourceType) throws IOException {
    Map<String, DataSourceResult> parsedResults = new HashMap<>();

    try {
      // Add type to JSON if it doesn't already have it
      final String resultWithType;
      if (!rawResult.contains("\"type\":")) {
        resultWithType = addTypeFieldToJson(rawResult, dataSourceType);
      } else {
        resultWithType = rawResult;
      }

      // Use AccessController.doPrivileged() to handle reflection security restrictions
      DataSourceResult result =
          AccessController.doPrivileged(
              (PrivilegedAction<DataSourceResult>)
                  () -> {
                    try {
                      // Parse based on the determined data source type
                      switch (dataSourceType.toLowerCase()) {
                        case "prometheus":
                          return OBJECT_MAPPER.readValue(resultWithType, PrometheusResult.class);
                          // Add cases for other data source types as they're implemented
                        default:
                          throw new IOException("Unsupported data source type: " + dataSourceType);
                      }
                    } catch (IOException e) {
                      throw new RuntimeException(
                          "Failed to parse result for data source type: " + dataSourceType, e);
                    }
                  });

      parsedResults.put(dataSourceName, result);
    } catch (RuntimeException e) {
      // Unwrap the cause if it's our RuntimeException wrapper
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException(
          "Failed to parse result from " + dataSourceName + ": " + e.getMessage(), e);
    }

    return parsedResults;
  }

  /**
   * Adds a type field to the JSON string for proper polymorphic deserialization.
   *
   * @param rawJson The raw JSON string without a type field
   * @param type The type to add
   * @return Modified JSON string with type field
   */
  private String addTypeFieldToJson(String rawJson, String type) {
    return rawJson.replaceFirst("\\{", "{\"type\":\"" + type + "\",");
  }
}
