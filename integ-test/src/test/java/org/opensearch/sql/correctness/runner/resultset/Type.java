/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.runner.resultset;

import lombok.Data;

/** Column type in schema */
@Data
public class Type {

  /** Column name */
  private final String name;

  /** Column type */
  private final String type;
}
