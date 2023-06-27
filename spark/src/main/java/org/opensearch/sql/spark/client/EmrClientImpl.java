/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INTEGRATION_JAR;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_APPLICATION_JAR;
import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_INDEX_NAME;
import static org.opensearch.sql.spark.data.constants.SparkConstants.STEP_ID_FIELD;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.response.SparkResponse;

public class EmrClientImpl implements SparkClient {
  private final Client client;

  private static final Logger logger = LogManager.getLogger(EmrClientImpl.class);

  private final String emrCluster;
  private final String emrAccessKey;
  private final String emrSecretKey;
  private final String emrRegion;
  private final String field = STEP_ID_FIELD;

  private final String flintHost;
  private final String flintPort;
  private final String flintScheme;
  private final String flintAuth;
  private final String flintRegion;

  /**
   * Constructor for EMR Client Implementation.
   *
   * @param client        Opensearch client
   * @param cluster       EMR cluster id
   * @param emrRegion     EMR region
   * @param emrAccessKey  EMR access key
   * @param emrSecretKey  EMR secret key
   * @param flintHost     Opensearch host for flint integration jar
   * @param flintPort     Opensearch port for flint integration jar
   * @param flintScheme   Opensearch scheme for flint integration jar
   * @param flintAuth     Opensearch auth for flint integration jar
   * @param flintRegion   Opensearch region for flint integration jar
   */
  public EmrClientImpl(
      Client client, String cluster, String emrRegion, String emrAccessKey,
      String emrSecretKey, String flintHost, String flintPort, String flintScheme,
      String flintAuth, String flintRegion) {
    this.client = client;
    this.emrCluster = cluster;
    this.emrRegion = emrRegion;
    this.emrAccessKey = emrAccessKey;
    this.emrSecretKey = emrSecretKey;
    this.flintHost = flintHost;
    this.flintPort = flintPort;
    this.flintScheme = flintScheme;
    this.flintAuth = flintAuth;
    this.flintRegion = flintRegion;
  }

  @Override
  public JSONObject sql(String query) throws IOException {
    return new SparkResponse(
        client, runEmrApplication(query), field).getResultFromOpensearchIndex();
  }

  private String runEmrApplication(String query) {
    AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(emrAccessKey, emrSecretKey)))
        .withRegion(emrRegion)
        .build();

    HadoopJarStepConfig stepConfig = new HadoopJarStepConfig()
        .withJar("command-runner.jar")
        .withArgs("spark-submit",
            "--class","org.opensearch.sql.SQLJob",
            "--jars",FLINT_INTEGRATION_JAR,
            SPARK_APPLICATION_JAR,
            query,
            SPARK_INDEX_NAME,
            flintHost,
            flintPort,
            flintScheme,
            flintAuth,
            flintRegion
        );

    StepConfig emrstep = new StepConfig()
        .withName("Spark Application")
        .withActionOnFailure(ActionOnFailure.CONTINUE)
        .withHadoopJarStep(stepConfig);

    AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
        .withJobFlowId(emrCluster)
        .withSteps(emrstep);

    AddJobFlowStepsResult result = emrClient.addJobFlowSteps(request);
    logger.info("Spark application step IDs: " + result.getStepIds());

    String stepId = result.getStepIds().get(0);
    DescribeStepRequest stepRequest = new DescribeStepRequest()
        .withClusterId(emrCluster)
        .withStepId(stepId);

    waitForStepExecution(emrClient, stepRequest);

    // Close the EMR client
    emrClient.shutdown();
    return stepId;
  }

  private void waitForStepExecution(AmazonElasticMapReduce emrClient, DescribeStepRequest stepRequest) {
    // Wait for the step to complete
    boolean completed = false;
    while (!completed) {
      // Get the step status
      DescribeStepResult stepResult = emrClient.describeStep(stepRequest);
      StepStatus statusDetail = stepResult.getStep().getStatus();
      // Check if the step has completed
      if (statusDetail.getState().equals("COMPLETED")) {
        completed = true;
        logger.info("EMR step completed successfully.");
      } else if (statusDetail.getState().equals("FAILED")
          || statusDetail.getState().equals("CANCELLED")) {
        completed = true;
        logger.error("EMR step failed or cancelled.");
      } else {
        // Sleep for some time before checking the status again
        try {
          Thread.sleep(2500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

}