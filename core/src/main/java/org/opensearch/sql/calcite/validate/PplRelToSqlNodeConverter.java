/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;

/**
 * An extension of {@link RelToSqlConverter} to convert a relation algebra tree, translated from a
 * PPL query, into a SQL statement.
 *
 * <p>This converter is used during the validation phase to convert RelNode back to SqlNode for
 * validation and type checking using Calcite's SqlValidator.
 */
public class PplRelToSqlNodeConverter extends RelToSqlConverter {
  /**
   * Creates a RelToSqlConverter for PPL.
   *
   * @param dialect the SQL dialect to use for conversion
   */
  public PplRelToSqlNodeConverter(SqlDialect dialect) {
    super(dialect);
  }
}
