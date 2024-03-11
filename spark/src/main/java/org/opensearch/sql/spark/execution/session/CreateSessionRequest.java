/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.util.Map;
import lombok.Data;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.JobType;

@Data
public class CreateSessionRequest {
  private final String clusterName;
  private final String applicationId;
  private final String executionRoleArn;
  private final SparkSubmitParameters.Builder sparkSubmitParametersBuilder;
  private final Map<String, String> tags;
  private final String resultIndex;
  private final String datasourceName;

  public StartJobRequest getStartJobRequest(String sessionId) {
    return new InteractiveSessionStartJobRequest(
        "select 1",
        clusterName + ":" + JobType.INTERACTIVE.getText() + ":" + sessionId,
        applicationId,
        executionRoleArn,
        sparkSubmitParametersBuilder.build().toString(),
        tags,
        resultIndex);
  }

  static class InteractiveSessionStartJobRequest extends StartJobRequest {
    public InteractiveSessionStartJobRequest(
        String query,
        String jobName,
        String applicationId,
        String executionRoleArn,
        String sparkSubmitParams,
        Map<String, String> tags,
        String resultIndex) {
      super(
          query,
          jobName,
          applicationId,
          executionRoleArn,
          sparkSubmitParams,
          tags,
          false,
          resultIndex);
    }

    /** Interactive query keep running. */
    @Override
    public Long executionTimeout() {
      return 0L;
    }
  }
}
