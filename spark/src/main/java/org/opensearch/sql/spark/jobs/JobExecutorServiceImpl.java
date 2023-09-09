/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.jobs;

import static org.opensearch.sql.common.setting.Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG;

import com.amazonaws.services.emrserverless.model.JobRunState;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher;
import org.opensearch.sql.spark.functions.response.DefaultSparkSqlFunctionResponseHandle;
import org.opensearch.sql.spark.jobs.exceptions.JobNotFoundException;
import org.opensearch.sql.spark.jobs.model.JobExecutionResponse;
import org.opensearch.sql.spark.jobs.model.JobMetadata;
import org.opensearch.sql.spark.rest.model.CreateJobRequest;
import org.opensearch.sql.spark.rest.model.CreateJobResponse;

/** JobExecutorService implementation of {@link JobExecutorService}. */
@AllArgsConstructor
public class JobExecutorServiceImpl implements JobExecutorService {
  private JobMetadataStorageService jobMetadataStorageService;
  private SparkQueryDispatcher sparkQueryDispatcher;
  private Settings settings;
  private Boolean isJobExecutionEnabled;

  public JobExecutorServiceImpl() {
    this.isJobExecutionEnabled = Boolean.FALSE;
  }

  public JobExecutorServiceImpl(
      JobMetadataStorageService jobMetadataStorageService,
      SparkQueryDispatcher sparkQueryDispatcher,
      Settings settings) {
    this.isJobExecutionEnabled = Boolean.TRUE;
    this.jobMetadataStorageService = jobMetadataStorageService;
    this.sparkQueryDispatcher = sparkQueryDispatcher;
    this.settings = settings;
  }

  @Override
  public CreateJobResponse createJob(CreateJobRequest createJobRequest) {
    validateSparkExecutionEngineSettings();
    String sparkExecutionEngineConfigString =
        settings.getSettingValue(SPARK_EXECUTION_ENGINE_CONFIG);
    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        AccessController.doPrivileged(
            (PrivilegedAction<SparkExecutionEngineConfig>)
                () ->
                    SparkExecutionEngineConfig.toSparkExecutionEngineConfig(
                        sparkExecutionEngineConfigString));
    String jobId =
        sparkQueryDispatcher.dispatch(
            sparkExecutionEngineConfig.getApplicationId(),
            createJobRequest.getQuery(),
            sparkExecutionEngineConfig.getExecutionRoleARN());
    jobMetadataStorageService.storeJobMetadata(
        new JobMetadata(jobId, sparkExecutionEngineConfig.getApplicationId()));
    return new CreateJobResponse(jobId);
  }

  @Override
  public JobExecutionResponse getJobResults(String jobId) {
    validateSparkExecutionEngineSettings();
    Optional<JobMetadata> jobMetadata = jobMetadataStorageService.getJobMetadata(jobId);
    if (jobMetadata.isPresent()) {
      JSONObject jsonObject =
          sparkQueryDispatcher.getQueryResponse(
              jobMetadata.get().getApplicationId(), jobMetadata.get().getJobId());
      if (JobRunState.SUCCESS.toString().equals(jsonObject.getString("status"))) {
        DefaultSparkSqlFunctionResponseHandle sparkSqlFunctionResponseHandle =
            new DefaultSparkSqlFunctionResponseHandle(jsonObject);
        List<ExprValue> result = new ArrayList<>();
        while (sparkSqlFunctionResponseHandle.hasNext()) {
          result.add(sparkSqlFunctionResponseHandle.next());
        }
        return new JobExecutionResponse(
            JobRunState.SUCCESS.toString(), sparkSqlFunctionResponseHandle.schema(), result);
      } else {
        return new JobExecutionResponse(jsonObject.getString("status"), null, null);
      }
    }
    throw new JobNotFoundException(String.format("JobId: %s not found", jobId));
  }

  private void validateSparkExecutionEngineSettings() {
    if (!isJobExecutionEnabled) {
      throw new IllegalArgumentException(
          String.format(
              "Job APIs are disabled as %s is not configured in cluster settings. "
                  + "Please configure the setting and restart the domain to enable JobAPIs",
              SPARK_EXECUTION_ENGINE_CONFIG.getKeyValue()));
    }
  }
}
