/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INTEGRATION_JAR;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_APPLICATION_JAR;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_INDEX_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STEP_ID_FIELD;

import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.helper.EMRHelper;
import org.opensearch.sql.spark.helper.FlintHelper;
import org.opensearch.sql.spark.response.SparkResponse;

public class EmrClientImpl implements SparkClient {
  private final Client client;
  private final EMRHelper emr;
  private final FlintHelper flint;
  private final String field = STEP_ID_FIELD;
  private static final Logger logger = LogManager.getLogger(EmrClientImpl.class);

  /**
   * Constructor for EMR Client Implementation.
   *
   * @param client  Opensearch client
   * @param emr     EMR helper
   * @param flint   Opensearch args for flint integration jar
   */
  public EmrClientImpl(
      Client client, EMRHelper emr, FlintHelper flint) {
    this.client = client;
    this.emr = emr;
    this.flint = flint;
  }

  @Override
  public JSONObject sql(String query) throws IOException {
    return new SparkResponse(
        client, runEmrApplication(query), field).getResultFromOpensearchIndex();
  }

  @VisibleForTesting
  String runEmrApplication(String query) {

    HadoopJarStepConfig stepConfig = new HadoopJarStepConfig()
        .withJar("command-runner.jar")
        .withArgs("spark-submit",
            "--class","org.opensearch.sql.SQLJob",
            "--jars",FLINT_INTEGRATION_JAR,
            SPARK_APPLICATION_JAR,
            query,
            SPARK_INDEX_NAME,
            flint.getFlintHost(),
            flint.getFlintPort(),
            flint.getFlintScheme(),
            flint.getFlintAuth(),
            flint.getFlintRegion()
        );

    StepConfig emrstep = new StepConfig()
        .withName("Spark Application")
        .withActionOnFailure(ActionOnFailure.CONTINUE)
        .withHadoopJarStep(stepConfig);

    AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
        .withJobFlowId(emr.getEmrCluster())
        .withSteps(emrstep);

    AddJobFlowStepsResult result = emr.addStep(request);
    logger.info("Spark application step IDs: " + result.getStepIds());

    String stepId = result.getStepIds().get(0);
    DescribeStepRequest stepRequest = new DescribeStepRequest()
        .withClusterId(emr.getEmrCluster())
        .withStepId(stepId);

    waitForStepExecution(stepRequest);

    return stepId;
  }

  @SneakyThrows
  private void waitForStepExecution(DescribeStepRequest stepRequest) {
    // Wait for the step to complete
    boolean completed = false;
    while (!completed) {
      // Get the step status
      StepStatus statusDetail = emr.getStepStatus(stepRequest);
      // Check if the step has completed
      if (statusDetail.getState().equals("COMPLETED")) {
        completed = true;
        logger.info("EMR step completed successfully.");
      } else if (statusDetail.getState().equals("FAILED")
          || statusDetail.getState().equals("CANCELLED")) {
        logger.error("EMR step failed or cancelled.");
        throw new RuntimeException("Spark SQL application failed.");
      } else {
        // Sleep for some time before checking the status again
        Thread.sleep(2500);
      }
    }
  }
}
