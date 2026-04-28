/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.search;

import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Wraps unnamed SELECT-list items with {@code AS <text>} so Calcite uses the expression text as the
 * column name instead of synthetic {@code EXPR$N}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SelectItemAliasRewriter extends SqlShuttle {

  public static final SelectItemAliasRewriter INSTANCE = new SelectItemAliasRewriter();

  /** Unparse config: bare identifiers (e.g., {@code SUM(a)} not {@code SUM(`a`)}). */
  private static final UnaryOperator<SqlWriterConfig> UNPARSE_CONFIG =
      c ->
          c.withDialect(AnsiSqlDialect.DEFAULT)
              .withQuoteAllIdentifiers(false)
              .withAlwaysUseParentheses(false)
              .withSelectListItemsOnSeparateLines(false)
              .withUpdateSetListNewline(false)
              .withIndentation(0);

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited instanceof SqlSelect select) {
      SqlNodeList list = select.getSelectList();
      for (int i = 0; i < list.size(); i++) {
        list.set(i, aliasIfNeeded(list.get(i)));
      }
    }
    return visited;
  }

  private static SqlNode aliasIfNeeded(SqlNode item) {
    if (item.getKind() == SqlKind.AS
        || item.getKind() == SqlKind.SELECT
        || item instanceof SqlIdentifier) {
      return item;
    }
    return SqlValidatorUtil.addAlias(item, item.toSqlString(UNPARSE_CONFIG).getSql());
  }
}
