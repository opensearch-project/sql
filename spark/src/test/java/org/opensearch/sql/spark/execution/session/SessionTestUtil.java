package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.constants.TestConstants.TEST_CLUSTER_NAME;
import static org.opensearch.sql.spark.constants.TestConstants.TEST_DATASOURCE_NAME;

import java.util.HashMap;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;

public class SessionTestUtil {

  public static CreateSessionRequest createSessionRequest() {
    return new CreateSessionRequest(
        TEST_CLUSTER_NAME,
        "appId",
        "arn",
        SparkSubmitParameters.Builder.builder(),
        new HashMap<>(),
        "resultIndex",
        TEST_DATASOURCE_NAME);
  }
}
