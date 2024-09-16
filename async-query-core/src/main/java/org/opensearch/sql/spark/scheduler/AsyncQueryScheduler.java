/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler;

import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.scheduler.model.AsyncQuerySchedulerRequest;

/** Scheduler interface for scheduling asynchronous query jobs. */
public interface AsyncQueryScheduler {

  /**
   * Schedules a new job in the system. This method creates a new job entry based on the provided
   * request parameters.
   *
   * <p>Use cases: - Creating a new periodic query execution - Setting up a scheduled data refresh
   * task
   *
   * @param asyncQuerySchedulerRequest The request containing job configuration details
   * @param asyncQueryRequestContext The request context passed to AsyncQueryExecutorService
   * @throws IllegalArgumentException if a job with the same name already exists
   * @throws RuntimeException if there's an error during job creation
   */
  void scheduleJob(
      AsyncQuerySchedulerRequest asyncQuerySchedulerRequest,
      AsyncQueryRequestContext asyncQueryRequestContext);

  /**
   * Updates an existing job with new parameters. This method modifies the configuration of an
   * already scheduled job.
   *
   * <p>Use cases: - Changing the schedule of an existing job - Modifying query parameters of a
   * scheduled job - Updating resource allocations for a job
   *
   * @param asyncQuerySchedulerRequest The request containing updated job configuration
   * @param asyncQueryRequestContext The request context passed to AsyncQueryExecutorService
   * @throws IllegalArgumentException if the job to be updated doesn't exist
   * @throws RuntimeException if there's an error during the update process
   */
  void updateJob(
      AsyncQuerySchedulerRequest asyncQuerySchedulerRequest,
      AsyncQueryRequestContext asyncQueryRequestContext);

  /**
   * Unschedules a job by marking it as disabled and updating its last update time. This method is
   * used when you want to temporarily stop a job from running but keep its configuration and
   * history in the system.
   *
   * <p>Use cases: - Pausing a job that's causing issues without losing its configuration -
   * Temporarily disabling a job during maintenance or high-load periods - Allowing for easy
   * re-enabling of the job in the future
   *
   * @param jobId The unique identifier of the job to unschedule
   * @param asyncQueryRequestContext The request context passed to AsyncQueryExecutorService
   * @throws IllegalArgumentException if the job to be unscheduled doesn't exist
   * @throws RuntimeException if there's an error during the unschedule process
   */
  void unscheduleJob(String jobId, AsyncQueryRequestContext asyncQueryRequestContext);

  /**
   * Removes a job completely from the scheduler. This method permanently deletes the job and all
   * its associated data from the system.
   *
   * <p>Use cases: - Cleaning up jobs that are no longer needed - Removing obsolete or erroneously
   * created jobs - Freeing up resources by deleting unused job configurations
   *
   * @param jobId The unique identifier of the job to remove
   * @param asyncQueryRequestContext The request context passed to AsyncQueryExecutorService
   * @throws IllegalArgumentException if the job to be removed doesn't exist
   * @throws RuntimeException if there's an error during the remove process
   */
  void removeJob(String jobId, AsyncQueryRequestContext asyncQueryRequestContext);
}
