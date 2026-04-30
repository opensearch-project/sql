/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Language specification defining the dialect the engine accepts. Provides parser configuration,
 * validator configuration, and composable {@link LanguageExtension}s that contribute operators and
 * post-parse rewrite rules.
 *
 * <p>Implementations define a complete language surface — for example, {@link UnifiedSqlSpec}
 * provides ANSI and extended SQL modes. A future PPL spec would implement this same interface once
 * PPL converges on the Calcite pipeline.
 */
public interface LanguageSpec {

  /**
   * A composable language extension that contributes operators and post-parse rewrite rules. All
   * methods have defaults so extensions only override what they need.
   */
  interface LanguageExtension {

    /**
     * Operators (functions, aggregates) this extension adds. Chained with the standard operator
     * table during validation.
     */
    default SqlOperatorTable operators() {
      return SqlOperatorTables.of();
    }

    /**
     * AST rewrite rules applied after parsing and before validation. Each visitor transforms the
     * parse tree (e.g., rewriting named arguments into MAP literals).
     */
    default List<SqlVisitor<SqlNode>> postParseRules() {
      return List.of();
    }
  }

  /**
   * Parser configuration controlling how SQL text is tokenized and parsed into a parse tree,
   * including parser factory, lexical rules, and conformance.
   */
  SqlParser.Config parserConfig();

  /**
   * Validator configuration controlling what SQL semantics the validator accepts, such as GROUP BY
   * behavior, LIMIT syntax, and type coercion.
   */
  SqlValidator.Config validatorConfig();

  /**
   * Language extensions registered with this spec. Each extension contributes operators and
   * post-parse rewrite rules that are composed by {@link #operatorTable()} and {@link
   * #postParseRules()}.
   */
  List<LanguageExtension> extensions();

  /**
   * Chained operator table combining the standard Calcite operators with all operators contributed
   * by registered extensions.
   */
  default SqlOperatorTable operatorTable() {
    List<SqlOperatorTable> tables = new ArrayList<>();
    tables.add(SqlStdOperatorTable.instance());
    extensions().forEach(ext -> tables.add(ext.operators()));
    return SqlOperatorTables.chain(tables);
  }

  /**
   * All post-parse rewrite rules from registered extensions, flattened in registration order.
   * Applied to the parse tree after parsing and before validation.
   */
  default List<SqlVisitor<SqlNode>> postParseRules() {
    return extensions().stream().flatMap(ext -> ext.postParseRules().stream()).toList();
  }
}
