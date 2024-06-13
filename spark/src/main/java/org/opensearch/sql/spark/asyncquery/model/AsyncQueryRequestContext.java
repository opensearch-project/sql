/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

/** Context interface to provide additional request related information */
public interface AsyncQueryRequestContext {
  Object getAttribute(String name);
}
