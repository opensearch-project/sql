/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.jobs;

import java.util.Optional;
import org.opensearch.sql.spark.jobs.model.JobMetadata;

public interface JobMetadataStorageService {

  void storeJobMetadata(JobMetadata jobMetadata);

  Optional<JobMetadata> getJobMetadata(String jobId);
}
