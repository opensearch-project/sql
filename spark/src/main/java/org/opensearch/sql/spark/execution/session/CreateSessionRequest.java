/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import java.util.Map;
import lombok.Data;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.StartJobRequest;

@Data
public class CreateSessionRequest {
  private final String jobName;
  private final String applicationId;
  private final String executionRoleArn;
  private final SparkSubmitParameters.Builder sparkSubmitParametersBuilder;
  private final Map<String, String> tags;
  private final String resultIndex;
  private final String datasourceName;

  public StartJobRequest getStartJobRequest() {
    return new StartJobRequest(
        "select 1",
        jobName,
        applicationId,
        executionRoleArn,
        sparkSubmitParametersBuilder.build().toString(),
        tags,
        false,
        resultIndex);
  }
}
