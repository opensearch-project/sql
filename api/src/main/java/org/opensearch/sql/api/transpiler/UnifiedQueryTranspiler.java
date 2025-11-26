/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.transpiler;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

/**
 * Transpiles Calcite logical plans ({@link RelNode}) into SQL strings for various target databases.
 * Uses Calcite's {@link RelToSqlConverter} to perform the conversion, respecting the specified SQL
 * dialect and formatting options.
 */
public class UnifiedQueryTranspiler {

  /** Target SQL dialect */
  private final SqlDialect dialect;

  private UnifiedQueryTranspiler(Builder builder) {
    this.dialect = builder.dialect;
  }

  /**
   * Converts a Calcite logical plan to a SQL string using the configured target dialect.
   *
   * @param plan the logical plan to convert (must not be null)
   * @return the generated SQL string
   */
  public String toSql(RelNode plan) {
    try {
      RelToSqlConverter converter = new RelToSqlConverter(dialect);
      SqlNode sqlNode = converter.visitRoot(plan).asStatement();
      return sqlNode.toSqlString(dialect).getSql();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to transpile logical plan to SQL", e);
    }
  }

  /**
   * Creates a new builder for constructing a UnifiedQueryTranspiler.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for UnifiedQueryTranspiler. */
  public static class Builder {
    private SqlDialect dialect;

    private Builder() {}

    /**
     * Sets the target SQL dialect for transpilation.
     *
     * @param dialect the SQL dialect to use (must not be null)
     * @return this builder
     */
    public Builder dialect(SqlDialect dialect) {
      this.dialect = dialect;
      return this;
    }

    /**
     * Builds a new UnifiedQueryTranspiler instance.
     *
     * @return a new UnifiedQueryTranspiler
     * @throws IllegalStateException if dialect has not been set
     */
    public UnifiedQueryTranspiler build() {
      if (dialect == null) {
        throw new IllegalStateException("SQL dialect must be specified");
      }
      return new UnifiedQueryTranspiler(this);
    }
  }
}
