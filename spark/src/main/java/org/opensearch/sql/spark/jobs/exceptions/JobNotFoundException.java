/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.jobs.exceptions;

/** JobNotFoundException. */
public class JobNotFoundException extends RuntimeException {
  public JobNotFoundException(String message) {
    super(message);
  }
}
