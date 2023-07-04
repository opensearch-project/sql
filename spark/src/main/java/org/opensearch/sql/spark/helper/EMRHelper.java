package org.opensearch.sql.spark.helper;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import lombok.Getter;

public class EMRHelper {
  @Getter
  private final String emrCluster;
  @Getter
  private AmazonElasticMapReduce emrClient;

  /** Arguments required for connecting to EMR cluster.
   *
   * @param emrCluster    EMR cluster id
   * @param emrAccessKey  AWS access key
   * @param emrSecretKey  AWS secret key
   * @param emrRegion     AWS region
   */
  public EMRHelper(String emrCluster, String emrAccessKey, String emrSecretKey, String emrRegion) {
    this.emrCluster = emrCluster;
    this.emrClient = AmazonElasticMapReduceClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(emrAccessKey, emrSecretKey)))
        .withRegion(emrRegion)
        .build();
  }

  public AddJobFlowStepsResult addStep(AddJobFlowStepsRequest request) {
    return emrClient.addJobFlowSteps(request);
  }

  public StepStatus getStepStatus(DescribeStepRequest stepRequest) {
    return emrClient.describeStep(stepRequest).getStep().getStatus();
  }

  public void close() {
    emrClient.shutdown();
  }

}
