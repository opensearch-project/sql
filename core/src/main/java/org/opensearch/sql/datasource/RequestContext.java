/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

/**
 * Context interface to provide additional request related information. It is introduced to allow
 * async-query-core library user to pass request context information to implementations of data
 * accessors.
 */
public interface RequestContext {
  Object getAttribute(String name);
}
