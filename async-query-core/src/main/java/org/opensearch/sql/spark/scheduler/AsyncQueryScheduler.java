package org.opensearch.sql.spark.scheduler;

import org.opensearch.sql.spark.scheduler.model.AsyncQuerySchedulerRequest;

/** Scheduler interface for scheduling asynchronous query jobs. */
public interface AsyncQueryScheduler {

  /** Schedules a new job. */
  void scheduleJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest);

  /** Updates an existing job with new parameters. */
  void updateJob(AsyncQuerySchedulerRequest asyncQuerySchedulerRequest);

  /** Unschedules a job by marking it as disabled and updating its last update time. */
  void unscheduleJob(String jobId);

  /** Removes a job. */
  void removeJob(String jobId);
}
