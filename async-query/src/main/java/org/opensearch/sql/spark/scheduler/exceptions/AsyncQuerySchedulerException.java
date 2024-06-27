/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.scheduler.exceptions;

public class AsyncQuerySchedulerException extends RuntimeException {
  public AsyncQuerySchedulerException(String message) {
    super(message);
  }

  public AsyncQuerySchedulerException(Throwable cause) {
    super(cause);
  }

  public AsyncQuerySchedulerException(String message, Throwable cause) {
    super(message, cause);
  }
}
