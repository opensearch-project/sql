/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

/** Enum for Index Action in the given query.* */
public enum IndexQueryActionType {
  CREATE,
  REFRESH,
  DESCRIBE,
  SHOW,
  DROP,
  VACUUM,
  ALTER,
  RECOVER
}
