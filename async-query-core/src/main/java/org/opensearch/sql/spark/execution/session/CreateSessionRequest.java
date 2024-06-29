/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.util.Map;
import lombok.Data;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilder;

@Data
public class CreateSessionRequest {
  private final String clusterName;
  private final String accountId;
  private final String applicationId;
  private final String executionRoleArn;
  private final SparkSubmitParametersBuilder sparkSubmitParametersBuilder;
  private final Map<String, String> tags;
  private final String resultIndex;
  private final String datasourceName;

  public StartJobRequest getStartJobRequest(String sessionId) {
    return new InteractiveSessionStartJobRequest(
        clusterName + ":" + JobType.INTERACTIVE.getText() + ":" + sessionId,
        accountId,
        applicationId,
        executionRoleArn,
        sparkSubmitParametersBuilder.toString(),
        tags,
        resultIndex);
  }

  static class InteractiveSessionStartJobRequest extends StartJobRequest {
    public InteractiveSessionStartJobRequest(
        String jobName,
        String accountId,
        String applicationId,
        String executionRoleArn,
        String sparkSubmitParams,
        Map<String, String> tags,
        String resultIndex) {
      super(
          jobName,
          accountId,
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
