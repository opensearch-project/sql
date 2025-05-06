/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

/** Interface to abstract session config */
public interface SessionConfigSupplier {
  Long getSessionInactivityTimeoutMillis();
}
