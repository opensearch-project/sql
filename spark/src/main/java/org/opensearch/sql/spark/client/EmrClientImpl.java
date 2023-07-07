/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_INDEX_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_SQL_APPLICATION_JAR;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
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
import org.opensearch.sql.spark.helper.FlintHelper;
import org.opensearch.sql.spark.response.SparkResponse;

public class EmrClientImpl implements SparkClient {
  private final AmazonElasticMapReduce emr;
  private final String emrCluster;
  private final FlintHelper flint;
  private final String sparkApplicationJar;
  private static final Logger logger = LogManager.getLogger(EmrClientImpl.class);
  private SparkResponse sparkResponse;

  /**
   * Constructor for EMR Client Implementation.
   *
   * @param emr     EMR helper
   * @param flint   Opensearch args for flint integration jar
   * @param sparkResponse Response object to help with retrieving results from Opensearch index
   */
  public EmrClientImpl(AmazonElasticMapReduce emr, String emrCluster, FlintHelper flint,
                       SparkResponse sparkResponse, String sparkApplicationJar) {
    this.emr = emr;
    this.emrCluster = emrCluster;
    this.flint = flint;
    this.sparkResponse = sparkResponse;
    this.sparkApplicationJar =
        sparkApplicationJar == null ? SPARK_SQL_APPLICATION_JAR : sparkApplicationJar;
  }

  @Override
  public JSONObject sql(String query) throws IOException {
    runEmrApplication(query);
    return sparkResponse.getResultFromOpensearchIndex();
  }

  @VisibleForTesting
  void runEmrApplication(String query) {

    HadoopJarStepConfig stepConfig = new HadoopJarStepConfig()
        .withJar("command-runner.jar")
        .withArgs("spark-submit",
            "--class","org.opensearch.sql.SQLJob",
            "--jars",
            flint.getFlintIntegrationJar(),
            sparkApplicationJar,
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
        .withJobFlowId(emrCluster)
        .withSteps(emrstep);

    AddJobFlowStepsResult result = emr.addJobFlowSteps(request);
    logger.info("EMR step ID: " + result.getStepIds());

    String stepId = result.getStepIds().get(0);
    DescribeStepRequest stepRequest = new DescribeStepRequest()
        .withClusterId(emrCluster)
        .withStepId(stepId);

    waitForStepExecution(stepRequest);
    sparkResponse.setValue(stepId);
  }

  @SneakyThrows
  private void waitForStepExecution(DescribeStepRequest stepRequest) {
    // Wait for the step to complete
    boolean completed = false;
    while (!completed) {
      // Get the step status
      StepStatus statusDetail = emr.describeStep(stepRequest).getStep().getStatus();
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
