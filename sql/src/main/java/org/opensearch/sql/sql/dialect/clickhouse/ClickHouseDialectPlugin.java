/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.dialect.clickhouse;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.opensearch.sql.api.dialect.DialectNames;
import org.opensearch.sql.api.dialect.DialectPlugin;
import org.opensearch.sql.api.dialect.QueryPreprocessor;

/**
 * ClickHouse dialect plugin providing all components for ClickHouse SQL query processing. Wires
 * together the preprocessor, operator table, parser config, and SQL dialect into a single plugin.
 */
public class ClickHouseDialectPlugin implements DialectPlugin {

  public static final ClickHouseDialectPlugin INSTANCE = new ClickHouseDialectPlugin();

  @Override
  public String dialectName() {
    return DialectNames.CLICKHOUSE;
  }

  @Override
  public QueryPreprocessor preprocessor() {
    return new ClickHouseQueryPreprocessor();
  }

  @Override
  public SqlParser.Config parserConfig() {
    return SqlParser.config()
        .withQuoting(Quoting.BACK_TICK)
        .withCaseSensitive(false)
        .withUnquotedCasing(Casing.TO_LOWER);
  }

  @Override
  public SqlOperatorTable operatorTable() {
    return ClickHouseOperatorTable.INSTANCE;
  }

  @Override
  public SqlDialect sqlDialect() {
    return OpenSearchClickHouseSqlDialect.DEFAULT;
  }
}
