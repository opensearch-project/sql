/* Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_JOB_NAME;
import static org.opensearch.sql.spark.constants.TestConstants.QUERY;
import static org.opensearch.sql.spark.constants.TestConstants.SPARK_SUBMIT_PARAMETERS;

import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EmrServerlessClientImplTest {
  @Mock private AWSEMRServerless emrServerless;

  @Test
  void testStartJobRun() {
    StartJobRunResult response = new StartJobRunResult();
    when(emrServerless.startJobRun(any())).thenReturn(response);

    EmrServerlessClientImpl emrServerlessClient = new EmrServerlessClientImpl(emrServerless);
    emrServerlessClient.startJobRun(
        QUERY, EMRS_JOB_NAME, EMRS_APPLICATION_ID, EMRS_EXECUTION_ROLE, SPARK_SUBMIT_PARAMETERS);
  }

  @Test
  void testGetJobRunState() {
    JobRun jobRun = new JobRun();
    jobRun.setState("Running");
    GetJobRunResult response = new GetJobRunResult();
    response.setJobRun(jobRun);
    when(emrServerless.getJobRun(any())).thenReturn(response);
    EmrServerlessClientImpl emrServerlessClient = new EmrServerlessClientImpl(emrServerless);
    emrServerlessClient.getJobRunResult(EMRS_APPLICATION_ID, "123");
  }
}
