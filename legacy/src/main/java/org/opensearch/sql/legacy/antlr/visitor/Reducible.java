/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.visitor;

import java.util.List;

/** Abstraction for anything that can be reduced and used by {@link AntlrSqlParseTreeVisitor}. */
public interface Reducible {

  /**
   * Reduce current and others to generate a new one
   *
   * @param others others
   * @return reduction
   */
  <T extends Reducible> T reduce(List<T> others);
}
