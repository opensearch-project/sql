/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.metrics;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public enum MetricName {
  REQ_TOTAL("request_total"),
  REQ_COUNT_TOTAL("request_count"),
  FAILED_REQ_COUNT_SYS("failed_request_count_syserr"),
  FAILED_REQ_COUNT_CUS("failed_request_count_cuserr"),
  FAILED_REQ_COUNT_CB("failed_request_count_cb"),
  DEFAULT_CURSOR_REQUEST_TOTAL("default_cursor_request_total"),
  DEFAULT_CURSOR_REQUEST_COUNT_TOTAL("default_cursor_request_count"),
  CIRCUIT_BREAKER("circuit_breaker"),
  DEFAULT("default"),

  PPL_REQ_TOTAL("ppl_request_total"),
  PPL_REQ_COUNT_TOTAL("ppl_request_count"),
  PPL_FAILED_REQ_COUNT_SYS("ppl_failed_request_count_syserr"),
  PPL_FAILED_REQ_COUNT_CUS("ppl_failed_request_count_cuserr"),
  DATASOURCE_CREATION_REQ_COUNT("datasource_create_request_count"),
  DATASOURCE_GET_REQ_COUNT("datasource_get_request_count"),
  DATASOURCE_PUT_REQ_COUNT("datasource_put_request_count"),
  DATASOURCE_PATCH_REQ_COUNT("datasource_patch_request_count"),
  DATASOURCE_DELETE_REQ_COUNT("datasource_delete_request_count"),
  DATASOURCE_FAILED_REQ_COUNT_SYS("datasource_failed_request_count_syserr"),
  DATASOURCE_FAILED_REQ_COUNT_CUS("datasource_failed_request_count_cuserr"),
  ASYNC_QUERY_CREATE_API_REQUEST_COUNT("async_query_create_api_request_count"),
  ASYNC_QUERY_GET_API_REQUEST_COUNT("async_query_get_api_request_count"),
  ASYNC_QUERY_CANCEL_API_REQUEST_COUNT("async_query_cancel_api_request_count"),
  ASYNC_QUERY_GET_API_FAILED_REQ_COUNT_SYS("async_query_get_api_failed_request_count_syserr"),
  ASYNC_QUERY_GET_API_FAILED_REQ_COUNT_CUS("async_query_get_api_failed_request_count_cuserr"),
  ASYNC_QUERY_CREATE_API_FAILED_REQ_COUNT_SYS("async_query_create_api_failed_request_count_syserr"),
  ASYNC_QUERY_CREATE_API_FAILED_REQ_COUNT_CUS("async_query_create_api_failed_request_count_cuserr"),
  ASYNC_QUERY_CANCEL_API_FAILED_REQ_COUNT_SYS("async_query_cancel_api_failed_request_count_syserr"),
  ASYNC_QUERY_CANCEL_API_FAILED_REQ_COUNT_CUS("async_query_cancel_api_failed_request_count_cuserr"),
  EMR_START_JOB_REQUEST_FAILURE_COUNT("emr_start_job_request_failure_count"),
  EMR_GET_JOB_RESULT_FAILURE_COUNT("emr_get_job_request_failure_count"),
  EMR_CANCEL_JOB_REQUEST_FAILURE_COUNT("emr_cancel_job_request_failure_count"),
  EMR_STREAMING_QUERY_JOBS_CREATION_COUNT("emr_streaming_jobs_creation_count"),
  EMR_INTERACTIVE_QUERY_JOBS_CREATION_COUNT("emr_interactive_jobs_creation_count"),
  EMR_BATCH_QUERY_JOBS_CREATION_COUNT("emr_batch_jobs_creation_count");

  private String name;

  MetricName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static List<String> getNames() {
    return Arrays.stream(MetricName.values()).map(v -> v.name).collect(Collectors.toList());
  }

  private static Set<MetricName> NUMERICAL_METRIC =
      new ImmutableSet.Builder<MetricName>()
          .add(PPL_REQ_TOTAL)
          .add(PPL_REQ_COUNT_TOTAL)
          .add(PPL_FAILED_REQ_COUNT_SYS)
          .add(PPL_FAILED_REQ_COUNT_CUS)
          .add(DATASOURCE_CREATION_REQ_COUNT)
          .add(DATASOURCE_DELETE_REQ_COUNT)
          .add(DATASOURCE_FAILED_REQ_COUNT_CUS)
          .add(DATASOURCE_GET_REQ_COUNT)
          .add(DATASOURCE_PATCH_REQ_COUNT)
          .add(DATASOURCE_FAILED_REQ_COUNT_SYS)
          .add(DATASOURCE_PUT_REQ_COUNT)
          .add(EMR_GET_JOB_RESULT_FAILURE_COUNT)
          .add(EMR_CANCEL_JOB_REQUEST_FAILURE_COUNT)
          .add(EMR_START_JOB_REQUEST_FAILURE_COUNT)
          .add(EMR_INTERACTIVE_QUERY_JOBS_CREATION_COUNT)
          .add(EMR_STREAMING_QUERY_JOBS_CREATION_COUNT)
          .add(EMR_BATCH_QUERY_JOBS_CREATION_COUNT)
          .add(ASYNC_QUERY_CREATE_API_FAILED_REQ_COUNT_CUS)
          .add(ASYNC_QUERY_CREATE_API_FAILED_REQ_COUNT_SYS)
          .add(ASYNC_QUERY_CANCEL_API_FAILED_REQ_COUNT_CUS)
          .add(ASYNC_QUERY_CANCEL_API_FAILED_REQ_COUNT_SYS)
          .add(ASYNC_QUERY_GET_API_FAILED_REQ_COUNT_CUS)
          .add(ASYNC_QUERY_GET_API_FAILED_REQ_COUNT_SYS)
          .add(ASYNC_QUERY_CREATE_API_REQUEST_COUNT)
          .add(ASYNC_QUERY_GET_API_REQUEST_COUNT)
          .add(ASYNC_QUERY_CANCEL_API_REQUEST_COUNT)
          .build();

  public boolean isNumerical() {
    return this == REQ_TOTAL
        || this == REQ_COUNT_TOTAL
        || this == FAILED_REQ_COUNT_SYS
        || this == FAILED_REQ_COUNT_CUS
        || this == FAILED_REQ_COUNT_CB
        || this == DEFAULT
        || this == DEFAULT_CURSOR_REQUEST_TOTAL
        || this == DEFAULT_CURSOR_REQUEST_COUNT_TOTAL
        || NUMERICAL_METRIC.contains(this);
  }
}
