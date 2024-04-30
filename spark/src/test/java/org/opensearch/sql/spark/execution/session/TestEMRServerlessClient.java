package org.opensearch.sql.spark.execution.session;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import org.junit.Assert;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;

public class TestEMRServerlessClient implements EMRServerlessClient {

  private int startJobRunCalled = 0;
  private int cancelJobRunCalled = 0;

  private StartJobRequest startJobRequest;

  @Override
  public String startJobRun(StartJobRequest startJobRequest) {
    this.startJobRequest = startJobRequest;
    startJobRunCalled++;
    return "jobId";
  }

  @Override
  public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
    return null;
  }

  @Override
  public CancelJobRunResult cancelJobRun(
      String applicationId, String jobId, boolean allowExceptionPropagation) {
    cancelJobRunCalled++;
    return null;
  }

  public void startJobRunCalled(int expectedTimes) {
    Assert.assertEquals(expectedTimes, startJobRunCalled);
  }

  public void cancelJobRunCalled(int expectedTimes) {
    Assert.assertEquals(expectedTimes, cancelJobRunCalled);
  }

  public void assertJobNameOfLastRequest(String expectedJobName) {
    Assert.assertEquals(expectedJobName, startJobRequest.getJobName());
  }
}
