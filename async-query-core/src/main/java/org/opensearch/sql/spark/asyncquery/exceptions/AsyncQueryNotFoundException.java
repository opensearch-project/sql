/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery.exceptions;

/** AsyncQueryNotFoundException. */
public class AsyncQueryNotFoundException extends RuntimeException {
  public AsyncQueryNotFoundException(String message) {
    super(message);
  }
}
