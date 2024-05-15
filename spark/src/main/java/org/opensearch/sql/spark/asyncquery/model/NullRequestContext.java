/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

/** An implementation of RequestContext for where context is not required */
public class NullRequestContext implements RequestContext {
  @Override
  public Object getAttribute(String name) {
    return null;
  }
}
