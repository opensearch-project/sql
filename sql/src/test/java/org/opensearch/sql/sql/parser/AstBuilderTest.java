/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.describe;
import static org.opensearch.sql.ast.dsl.AstDSL.doubleLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.highlight;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.limit;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.relationSubquery;
import static org.opensearch.sql.ast.dsl.AstDSL.sort;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.values;
import static org.opensearch.sql.utils.SystemIndexUtils.TABLE_INFO;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.NestedAllTupleFields;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.TableFunction;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.SemanticCheckException;

class AstBuilderTest extends AstBuilderTestBase {

  @Test
  public void can_build_select_literals() {
    assertEquals(
        project(
            values(emptyList()),
            alias("123", intLiteral(123)),
            alias("'hello'", stringLiteral("hello")),
            alias("\"world\"", stringLiteral("world")),
            alias("false", booleanLiteral(false)),
            alias("-4.567", doubleLiteral(-4.567))),
        buildAST("SELECT 123, 'hello', \"world\", false, -4.567"));
  }

  @Test
  public void can_build_select_function_call_with_alias() {
    assertEquals(
        project(relation("test"), alias("ABS(age)", function("ABS", qualifiedName("age")), "a")),
        buildAST("SELECT ABS(age) AS a FROM test"));
  }

  @Test
  public void can_build_select_all_from_index() {
    assertEquals(project(relation("test"), AllFields.of()), buildAST("SELECT * FROM test"));

    assertThrows(SyntaxCheckException.class, () -> buildAST("SELECT *"));
  }

  @Test
  public void can_build_nested_select_all() {
    assertEquals(
        project(relation("test"), alias("nested(field.*)", new NestedAllTupleFields("field"))),
        buildAST("SELECT nested(field.*) FROM test"));
  }

  @Test
  public void can_build_select_all_and_fields_from_index() {
    assertEquals(
        project(
            relation("test"),
            AllFields.of(),
            alias("age", qualifiedName("age")),
            alias("age", qualifiedName("age"), "a")),
        buildAST("SELECT *, age, age as a FROM test"));
  }

  @Test
  public void can_build_select_fields_from_index() {
    assertEquals(
        project(relation("test"), alias("age", qualifiedName("age"))),
        buildAST("SELECT age FROM test"));
  }

  @Test
  public void can_build_select_fields_with_alias() {
    assertEquals(
        project(relation("test"), alias("age", qualifiedName("age"), "a")),
        buildAST("SELECT age AS a FROM test"));
  }

  @Test
  public void can_build_select_fields_with_alias_quoted() {
    assertEquals(
        project(
            relation("test"),
            alias("(age + 10)", function("+", qualifiedName("age"), intLiteral(10)), "Age_Expr")),
        buildAST("SELECT (age + 10) AS `Age_Expr` FROM test"));
  }

  @Test
  public void can_build_from_index_with_alias() {
    assertEquals(
        project(
            filter(
                relation("test", "tt"), function("=", qualifiedName("tt", "age"), intLiteral(30))),
            alias("tt.name", qualifiedName("tt", "name"))),
        buildAST("SELECT tt.name FROM test AS tt WHERE tt.age = 30"));
  }

  @Test
  public void can_build_from_index_with_alias_quoted() {
    assertEquals(
        project(
            filter(relation("test", "t"), function("=", qualifiedName("t", "age"), intLiteral(30))),
            alias("`t`.name", qualifiedName("t", "name"))),
        buildAST("SELECT `t`.name FROM test `t` WHERE `t`.age = 30"));
  }

  @Test
  public void can_build_from_table_function() {
    assertEquals(
        project(
            new SubqueryAlias(
                "v",
                new TableFunction(
                    qualifiedName("vectorSearch"),
                    ImmutableList.of(
                        new UnresolvedArgument("table", stringLiteral("products")),
                        new UnresolvedArgument("field", stringLiteral("embedding")),
                        new UnresolvedArgument("vector", stringLiteral("[0.1,0.2]")),
                        new UnresolvedArgument("option", stringLiteral("k=10"))))),
            AllFields.of()),
        buildAST(
            "SELECT * FROM vectorSearch("
                + "table='products', field='embedding', "
                + "vector='[0.1,0.2]', option='k=10') AS v"));
  }

  @Test
  public void can_build_from_table_function_with_where_order_limit() {
    assertEquals(
        project(
            limit(
                sort(
                    filter(
                        new SubqueryAlias(
                            "s",
                            new TableFunction(
                                qualifiedName("vectorSearch"),
                                ImmutableList.of(
                                    new UnresolvedArgument("table", stringLiteral("products")),
                                    new UnresolvedArgument("field", stringLiteral("embedding")),
                                    new UnresolvedArgument("vector", stringLiteral("[0.1,0.2]")),
                                    new UnresolvedArgument("option", stringLiteral("k=10"))))),
                        function("=", qualifiedName("s", "category"), stringLiteral("shoes"))),
                    field(qualifiedName("s", "_score"), argument("asc", booleanLiteral(false)))),
                5,
                0),
            alias("s.title", qualifiedName("s", "title")),
            alias("s._score", qualifiedName("s", "_score"))),
        buildAST(
            "SELECT s.title, s._score FROM vectorSearch("
                + "table='products', field='embedding', "
                + "vector='[0.1,0.2]', option='k=10') AS s "
                + "WHERE s.category = 'shoes' "
                + "ORDER BY s._score DESC "
                + "LIMIT 5"));
  }

  @Test
  public void table_function_args_are_resolved_by_name_not_position() {
    assertEquals(
        project(
            new SubqueryAlias(
                "v",
                new TableFunction(
                    qualifiedName("vectorSearch"),
                    ImmutableList.of(
                        new UnresolvedArgument("option", stringLiteral("k=10")),
                        new UnresolvedArgument("field", stringLiteral("embedding")),
                        new UnresolvedArgument("table", stringLiteral("products")),
                        new UnresolvedArgument("vector", stringLiteral("[0.1,0.2]"))))),
            AllFields.of()),
        buildAST(
            "SELECT * FROM vectorSearch("
                + "option='k=10', field='embedding', "
                + "table='products', vector='[0.1,0.2]') AS v"));
  }

  @Test
  public void table_function_arg_names_are_canonicalized() {
    assertEquals(
        project(
            new SubqueryAlias(
                "v",
                new TableFunction(
                    qualifiedName("vectorSearch"),
                    ImmutableList.of(
                        new UnresolvedArgument("table", stringLiteral("products")),
                        new UnresolvedArgument("field", stringLiteral("embedding")),
                        new UnresolvedArgument("vector", stringLiteral("[0.1,0.2]")),
                        new UnresolvedArgument("option", stringLiteral("k=10"))))),
            AllFields.of()),
        buildAST(
            "SELECT * FROM vectorSearch("
                + "TABLE='products', FIELD='embedding', "
                + "VECTOR='[0.1,0.2]', OPTION='k=10') AS v"));
  }

  @Test
  public void table_function_allows_alias_without_as_keyword() {
    assertEquals(
        project(
            new SubqueryAlias(
                "v",
                new TableFunction(
                    qualifiedName("vectorSearch"),
                    ImmutableList.of(
                        new UnresolvedArgument("table", stringLiteral("products")),
                        new UnresolvedArgument("vector", stringLiteral("[0.1]"))))),
            AllFields.of()),
        buildAST("SELECT * FROM vectorSearch(table='products', vector='[0.1]') v"));
  }

  @Test
  public void table_function_relation_requires_alias() {
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                buildAST(
                    "SELECT * FROM vectorSearch("
                        + "table='products', field='embedding', "
                        + "vector='[0.1,0.2]', option='k=10')"));
    assertThat(ex.getMessage(), containsString("requires a table alias"));
    assertThat(ex.getMessage(), containsString("vectorSearch"));
  }

  @Test
  public void table_function_relation_rejects_positional_argument() {
    // Grammar accepts both `ident=value` and bare `value` for each table function argument so
    // the real positional shape reaches the V2 AstBuilder. The AstBuilder must reject it with a
    // SemanticCheckException rather than let the request fall back to the legacy engine.
    SemanticCheckException ex =
        assertThrows(
            SemanticCheckException.class,
            () ->
                buildAST(
                    "SELECT * FROM vectorSearch('products', field='embedding', "
                        + "vector='[0.1,0.2]', option='k=10') AS v"));
    org.junit.jupiter.api.Assertions.assertTrue(
        ex.getMessage().contains("requires named arguments"));
  }

  @Test
  public void can_build_where_clause() {
    assertEquals(
        project(
            filter(relation("test"), function("=", qualifiedName("name"), stringLiteral("John"))),
            alias("name", qualifiedName("name"))),
        buildAST("SELECT name FROM test WHERE name = 'John'"));
  }

  @Test
  public void can_build_count_literal() {
    assertEquals(
        project(
            agg(
                relation("test"),
                ImmutableList.of(alias("COUNT(1)", aggregate("COUNT", intLiteral(1)))),
                emptyList(),
                emptyList(),
                emptyList()),
            alias("COUNT(1)", aggregate("COUNT", intLiteral(1)))),
        buildAST("SELECT COUNT(1) FROM test"));
  }

  @Test
  public void can_build_count_star() {
    assertEquals(
        project(
            agg(
                relation("test"),
                ImmutableList.of(alias("COUNT(*)", aggregate("COUNT", AllFields.of()))),
                emptyList(),
                emptyList(),
                emptyList()),
            alias("COUNT(*)", aggregate("COUNT", AllFields.of()))),
        buildAST("SELECT COUNT(*) FROM test"));
  }

  @Test
  public void can_build_group_by_field_name() {
    assertEquals(
        project(
            agg(
                relation("test"),
                ImmutableList.of(alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
                emptyList(),
                ImmutableList.of(alias("name", qualifiedName("name"))),
                emptyList()),
            alias("name", qualifiedName("name")),
            alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
        buildAST("SELECT name, AVG(age) FROM test GROUP BY name"));
  }

  @Test
  public void can_build_group_by_function() {
    assertEquals(
        project(
            agg(
                relation("test"),
                ImmutableList.of(alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
                emptyList(),
                ImmutableList.of(alias("abs(name)", function("abs", qualifiedName("name")))),
                emptyList()),
            alias("abs(name)", function("abs", qualifiedName("name"))),
            alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
        buildAST("SELECT abs(name), AVG(age) FROM test GROUP BY abs(name)"));
  }

  @Test
  public void can_build_group_by_uppercase_function() {
    assertEquals(
        project(
            agg(
                relation("test"),
                ImmutableList.of(alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
                emptyList(),
                ImmutableList.of(alias("ABS(name)", function("ABS", qualifiedName("name")))),
                emptyList()),
            alias("ABS(name)", function("ABS", qualifiedName("name"))),
            alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
        buildAST("SELECT ABS(name), AVG(age) FROM test GROUP BY 1"));
  }

  @Test
  public void can_build_group_by_alias() {
    assertEquals(
        project(
            agg(
                relation("test"),
                ImmutableList.of(alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
                emptyList(),
                ImmutableList.of(alias("abs(name)", function("abs", qualifiedName("name")))),
                emptyList()),
            alias("abs(name)", function("abs", qualifiedName("name")), "n"),
            alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
        buildAST("SELECT abs(name) as n, AVG(age) FROM test GROUP BY n"));
  }

  @Test
  public void can_build_group_by_ordinal() {
    assertEquals(
        project(
            agg(
                relation("test"),
                ImmutableList.of(alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
                emptyList(),
                ImmutableList.of(alias("abs(name)", function("abs", qualifiedName("name")))),
                emptyList()),
            alias("abs(name)", function("abs", qualifiedName("name")), "n"),
            alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
        buildAST("SELECT abs(name) as n, AVG(age) FROM test GROUP BY 1"));
  }

  @Test
  public void can_build_implicit_group_by_clause() {
    assertEquals(
        project(
            agg(
                relation("test"),
                ImmutableList.of(alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
                emptyList(),
                emptyList(),
                emptyList()),
            alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
        buildAST("SELECT AVG(age) FROM test"));
  }

  @Test
  public void can_build_having_clause() {
    assertEquals(
        project(
            filter(
                agg(
                    relation("test"),
                    ImmutableList.of(
                        alias("AVG(age)", aggregate("AVG", qualifiedName("age"))),
                        alias("MIN(balance)", aggregate("MIN", qualifiedName("balance")))),
                    emptyList(),
                    ImmutableList.of(alias("name", qualifiedName("name"))),
                    emptyList()),
                function(">", aggregate("MIN", qualifiedName("balance")), intLiteral(1000))),
            alias("name", qualifiedName("name")),
            alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
        buildAST("SELECT name, AVG(age) FROM test GROUP BY name HAVING MIN(balance) > 1000"));
  }

  @Test
  public void can_build_having_condition_using_alias() {
    assertEquals(
        project(
            filter(
                agg(
                    relation("test"),
                    ImmutableList.of(alias("AVG(age)", aggregate("AVG", qualifiedName("age")))),
                    emptyList(),
                    ImmutableList.of(alias("name", qualifiedName("name"))),
                    emptyList()),
                function(">", aggregate("AVG", qualifiedName("age")), intLiteral(1000))),
            alias("name", qualifiedName("name")),
            alias("AVG(age)", aggregate("AVG", qualifiedName("age")), "a")),
        buildAST("SELECT name, AVG(age) AS a FROM test GROUP BY name HAVING a > 1000"));
  }

  @Test
  public void can_build_order_by_field_name() {
    assertEquals(
        project(
            sort(relation("test"), field("name", argument("asc", booleanLiteral(true)))),
            alias("name", qualifiedName("name"))),
        buildAST("SELECT name FROM test ORDER BY name"));
  }

  @Test
  public void can_build_order_by_function() {
    assertEquals(
        project(
            sort(
                relation("test"),
                field(
                    function("ABS", qualifiedName("name")), argument("asc", booleanLiteral(true)))),
            alias("name", qualifiedName("name"))),
        buildAST("SELECT name FROM test ORDER BY ABS(name)"));
  }

  @Test
  public void can_build_order_by_alias() {
    assertEquals(
        project(
            sort(relation("test"), field("name", argument("asc", booleanLiteral(true)))),
            alias("name", qualifiedName("name"), "n")),
        buildAST("SELECT name AS n FROM test ORDER BY n ASC"));
  }

  @Test
  public void can_build_order_by_ordinal() {
    assertEquals(
        project(
            sort(relation("test"), field("name", argument("asc", booleanLiteral(false)))),
            alias("name", qualifiedName("name"))),
        buildAST("SELECT name FROM test ORDER BY 1 DESC"));
  }

  @Test
  public void can_build_order_by_multiple_field_names() {
    assertEquals(
        project(
            sort(
                relation("test"),
                field("name", argument("asc", booleanLiteral(true))),
                field("age", argument("asc", booleanLiteral(false)))),
            alias("name", qualifiedName("name")),
            alias("age", qualifiedName("age"))),
        buildAST("SELECT name, age FROM test ORDER BY name, age DESC"));
  }

  @Test
  public void can_build_select_distinct_clause() {
    assertEquals(
        project(
            agg(
                relation("test"),
                emptyList(),
                emptyList(),
                ImmutableList.of(
                    alias("name", qualifiedName("name")), alias("age", qualifiedName("age"))),
                emptyList()),
            alias("name", qualifiedName("name")),
            alias("age", qualifiedName("age"))),
        buildAST("SELECT DISTINCT name, age FROM test"));
  }

  @Test
  public void can_build_select_distinct_clause_with_function() {
    assertEquals(
        project(
            agg(
                relation("test"),
                emptyList(),
                emptyList(),
                ImmutableList.of(
                    alias(
                        "SUBSTRING(name, 1, 2)",
                        function(
                            "SUBSTRING", qualifiedName("name"), intLiteral(1), intLiteral(2)))),
                emptyList()),
            alias(
                "SUBSTRING(name, 1, 2)",
                function("SUBSTRING", qualifiedName("name"), intLiteral(1), intLiteral(2)))),
        buildAST("SELECT DISTINCT SUBSTRING(name, 1, 2) FROM test"));
  }

  @Test
  public void can_build_select_all_clause() {
    assertEquals(
        buildAST("SELECT name, age FROM test"), buildAST("SELECT ALL name, age FROM test"));
  }

  @Test
  public void can_build_order_by_null_option() {
    assertEquals(
        project(
            sort(
                relation("test"),
                field(
                    "name",
                    argument("asc", booleanLiteral(true)),
                    argument("nullFirst", booleanLiteral(false)))),
            alias("name", qualifiedName("name"))),
        buildAST("SELECT name FROM test ORDER BY name NULLS LAST"));
  }

  /**
   *
   *
   * <pre>
   * Ensure Nested function falls back to legacy engine when used in an HAVING clause.
   * TODO Remove this test when support is added.
   * </pre>
   */
  @Test
  public void nested_in_having_clause_throws_exception() {
    SyntaxCheckException exception =
        assertThrows(
            SyntaxCheckException.class,
            () -> buildAST("SELECT count(*) FROM test HAVING nested(message.info)"));

    assertEquals(
        "Falling back to legacy engine. Nested function is not supported in the HAVING clause.",
        exception.getMessage());
  }

  @Test
  public void can_build_order_by_sort_order_keyword_insensitive() {
    assertEquals(
        project(
            sort(relation("test"), field("age", argument("asc", booleanLiteral(true)))),
            alias("age", qualifiedName("age"))),
        buildAST("SELECT age FROM test ORDER BY age ASC"));

    assertEquals(
        project(
            sort(relation("test"), field("age", argument("asc", booleanLiteral(true)))),
            alias("age", qualifiedName("age"))),
        buildAST("SELECT age FROM test ORDER BY age asc"));
  }

  @Test
  public void can_build_from_subquery() {
    assertEquals(
        project(
            filter(
                relationSubquery(
                    project(
                        relation("test"),
                        alias("firstname", qualifiedName("firstname"), "firstName"),
                        alias("lastname", qualifiedName("lastname"), "lastName")),
                    "a"),
                function(">", qualifiedName("age"), intLiteral(20))),
            alias("a.firstName", qualifiedName("a", "firstName")),
            alias("lastName", qualifiedName("lastName"))),
        buildAST(
            "SELECT a.firstName, lastName FROM ("
                + "SELECT firstname AS firstName, lastname AS lastName FROM test"
                + ") AS a where age > 20"));
  }

  @Test
  public void can_build_from_subquery_with_backquoted_alias() {
    assertEquals(
        project(
            relationSubquery(
                project(
                    relation("test"), alias("firstname", qualifiedName("firstname"), "firstName")),
                "a"),
            alias("a.firstName", qualifiedName("a", "firstName"))),
        buildAST(
            "SELECT a.firstName "
                + "FROM ( "
                + " SELECT `firstname` AS `firstName` "
                + " FROM `test` "
                + ") AS `a`"));
  }

  @Test
  public void can_build_show_all_tables() {
    assertEquals(
        project(
            filter(
                describe(TABLE_INFO),
                function("like", qualifiedName("TABLE_NAME"), stringLiteral("%"))),
            AllFields.of()),
        buildAST("SHOW TABLES LIKE '%'"));
  }

  @Test
  public void can_build_show_selected_tables() {
    assertEquals(
        project(
            filter(
                describe(TABLE_INFO),
                function("like", qualifiedName("TABLE_NAME"), stringLiteral("a_c%"))),
            AllFields.of()),
        buildAST("SHOW TABLES LIKE 'a_c%'"));
  }

  @Test
  public void show_compatible_with_old_engine_syntax() {
    assertEquals(
        project(
            filter(
                describe(TABLE_INFO),
                function("like", qualifiedName("TABLE_NAME"), stringLiteral("%"))),
            AllFields.of()),
        buildAST("SHOW TABLES LIKE '%'"));
  }

  @Test
  public void can_build_describe_selected_tables() {
    assertEquals(
        project(describe(mappingTable("a_c%")), AllFields.of()),
        buildAST("DESCRIBE TABLES LIKE 'a_c%'"));
  }

  @Test
  public void can_build_describe_selected_tables_field_filter() {
    assertEquals(
        project(
            filter(
                describe(mappingTable("a_c%")),
                function("like", qualifiedName("COLUMN_NAME"), stringLiteral("name%"))),
            AllFields.of()),
        buildAST("DESCRIBE TABLES LIKE 'a_c%' COLUMNS LIKE 'name%'"));
  }

  @Test
  public void can_build_alias_by_keywords() {
    assertEquals(
        project(relation("test"), alias("avg_age", qualifiedName("avg_age"), "avg")),
        buildAST("SELECT avg_age AS avg FROM test"));
  }

  @Test
  public void can_build_limit_clause() {
    assertEquals(
        project(
            limit(
                sort(relation("test"), field("age", argument("asc", booleanLiteral(true)))), 10, 0),
            alias("name", qualifiedName("name")),
            alias("age", qualifiedName("age"))),
        buildAST("SELECT name, age FROM test ORDER BY age LIMIT 10"));
  }

  @Test
  public void can_build_limit_clause_with_offset() {
    assertEquals(
        project(limit(relation("test"), 10, 5), alias("name", qualifiedName("name"))),
        buildAST("SELECT name FROM test LIMIT 10 OFFSET 5"));

    assertEquals(
        project(limit(relation("test"), 10, 5), alias("name", qualifiedName("name"))),
        buildAST("SELECT name FROM test LIMIT 5, 10"));
  }

  @Test
  public void can_build_qualified_name_highlight() {
    Map<String, Literal> args = new HashMap<>();
    assertEquals(
        project(
            relation("test"),
            alias("highlight(fieldA)", highlight(AstDSL.qualifiedName("fieldA"), args))),
        buildAST("SELECT highlight(fieldA) FROM test"));
  }

  @Test
  public void can_build_qualified_highlight_with_arguments() {
    Map<String, Literal> args = new HashMap<>();
    args.put("pre_tags", new Literal("<mark>", DataType.STRING));
    args.put("post_tags", new Literal("</mark>", DataType.STRING));
    assertEquals(
        project(
            relation("test"),
            alias(
                "highlight(fieldA, pre_tags='<mark>', post_tags='</mark>')",
                highlight(AstDSL.qualifiedName("fieldA"), args))),
        buildAST(
            "SELECT highlight(fieldA, pre_tags='<mark>', post_tags='</mark>') " + "FROM test"));
  }

  @Test
  public void can_build_string_literal_highlight() {
    Map<String, Literal> args = new HashMap<>();
    assertEquals(
        project(
            relation("test"),
            alias("highlight(\"fieldA\")", highlight(AstDSL.stringLiteral("fieldA"), args))),
        buildAST("SELECT highlight(\"fieldA\") FROM test"));
  }
}
