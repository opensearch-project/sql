/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor.pagination;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.opensearch.sql.ast.dsl.AstDSL.agg;
import static org.opensearch.sql.ast.dsl.AstDSL.aggregate;
import static org.opensearch.sql.ast.dsl.AstDSL.alias;
import static org.opensearch.sql.ast.dsl.AstDSL.allFields;
import static org.opensearch.sql.ast.dsl.AstDSL.and;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.between;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.caseWhen;
import static org.opensearch.sql.ast.dsl.AstDSL.cast;
import static org.opensearch.sql.ast.dsl.AstDSL.compare;
import static org.opensearch.sql.ast.dsl.AstDSL.equalTo;
import static org.opensearch.sql.ast.dsl.AstDSL.eval;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.function;
import static org.opensearch.sql.ast.dsl.AstDSL.highlight;
import static org.opensearch.sql.ast.dsl.AstDSL.in;
import static org.opensearch.sql.ast.dsl.AstDSL.intLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.intervalLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.limit;
import static org.opensearch.sql.ast.dsl.AstDSL.map;
import static org.opensearch.sql.ast.dsl.AstDSL.not;
import static org.opensearch.sql.ast.dsl.AstDSL.or;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.sort;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.tableFunction;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedArg;
import static org.opensearch.sql.ast.dsl.AstDSL.unresolvedAttr;
import static org.opensearch.sql.ast.dsl.AstDSL.values;
import static org.opensearch.sql.ast.dsl.AstDSL.when;
import static org.opensearch.sql.ast.dsl.AstDSL.window;
import static org.opensearch.sql.ast.dsl.AstDSL.xor;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.RelevanceFieldList;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class CanPaginateVisitorTest {

  static final CanPaginateVisitor visitor = new CanPaginateVisitor();

  @Test
  // select * from y
  public void accept_query_with_select_star_and_from() {
    var plan = project(relation("dummy"), allFields());
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select x from y
  public void allow_query_with_select_field_and_from() {
    var plan = project(relation("dummy"), field("pewpew"));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select x from y
  public void visitUnresolvedAttribute() {
    var plan = project(relation("dummy"), unresolvedAttr("pewpew"));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select x as z from y
  public void allow_query_with_select_alias_and_from() {
    var plan = project(relation("dummy"), alias("pew", field("pewpew"), "pew"));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select N from y
  public void allow_query_with_select_literal_and_from() {
    var plan = project(relation("dummy"), intLiteral(42));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select x.z from y
  public void allow_query_with_select_qn_and_from() {
    var plan = project(relation("dummy"), qualifiedName("field.subfield"));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select x,z from y
  public void allow_query_with_select_fields_and_from() {
    var plan = project(relation("dummy"), field("pewpew"), field("pewpew"));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select x
  public void allow_query_without_from() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", intLiteral(1)));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  public void visitField() {
    // test combinations of acceptable and not acceptable args for coverage
    assertAll(
        () -> assertFalse(project(relation("dummy"),
                field(map("1", "2"), argument("name", intLiteral(0))))
            .accept(visitor, null)),
        () -> assertFalse(project(relation("dummy"),
                field("field", new Argument("", new Literal(1, DataType.INTEGER) {
                    @Override
                    public List<UnresolvedExpression> getChild() {
                      return List.of(map("1", "2"));
                    }
                })))
            .accept(visitor, null))
    );
  }

  @Test
  public void visitAlias() {
    // test combinations of acceptable and not acceptable args for coverage
    assertAll(
        () -> assertFalse(project(relation("dummy"),
                alias("pew", map("1", "2"), "pew"))
            .accept(visitor, null)),
        () -> assertFalse(project(relation("dummy"), new Alias("pew", field("pew")) {
              @Override
              public List<? extends Node> getChild() {
                return List.of(map("1", "2"));
              }
            })
            .accept(visitor, null))
    );
  }

  @Test
  // select a = b
  public void visitEqualTo() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", equalTo(intLiteral(1), intLiteral(1))));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select interval
  public void visitInterval() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", intervalLiteral(intLiteral(1), DataType.INTEGER, "days")));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select a != b
  public void visitCompare() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", compare("!=", intLiteral(1), intLiteral(1))));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select NOT a
  public void visitNot() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", not(booleanLiteral(true))));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select a OR b
  public void visitOr() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", or(booleanLiteral(true), booleanLiteral(false))));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select a AND b
  public void visitAnd() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", and(booleanLiteral(true), booleanLiteral(false))));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select a XOR b
  public void visitXor() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", xor(booleanLiteral(true), booleanLiteral(false))));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select f()
  public void visitFunction() {
    var plan = project(values(List.of(intLiteral(1))),
        function("func"));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select nested() ...
  public void visitNested() {
    var plan = project(values(List.of(intLiteral(1))),
        function("nested"));
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select a IN ()
  public void visitIn() {
    // test combinations of acceptable and not acceptable args for coverage
    assertAll(
        () -> assertTrue(project(values(List.of(intLiteral(1))), alias("1", in(field("a"))))
            .accept(visitor, null)),
        () -> assertFalse(project(values(List.of(intLiteral(1))),
                alias("1", in(field("a"), map("1", "2"))))
            .accept(visitor, null)),
        () -> assertFalse(project(values(List.of(intLiteral(1))),
                alias("1", in(map("1", "2"), field("a"))))
            .accept(visitor, null))
    );
  }

  @Test
  // select a BETWEEN 1 AND 2
  public void visitBetween() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", between(field("a"), intLiteral(1), intLiteral(2))));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select a CASE 1 WHEN 2
  public void visitCase() {
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", caseWhen(intLiteral(1), when(intLiteral(3), intLiteral(4)))));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select CAST(a as TYPE)
  public void visitCast() {
    // test combinations of acceptable and not acceptable args for coverage
    assertAll(
        () -> assertTrue(project(values(List.of(intLiteral(1))),
                alias("1", cast(intLiteral(2), stringLiteral("int"))))
            .accept(visitor, null)),
        () -> assertFalse(project(values(List.of(intLiteral(1))),
                alias("1", cast(intLiteral(2), new Literal(1, DataType.INTEGER) {
                  @Override
                  public List<UnresolvedExpression> getChild() {
                    return List.of(map("1", "2"));
                  }
                })))
            .accept(visitor, null)),
        () -> assertFalse(project(values(List.of(intLiteral(1))),
                alias("1", cast(map("1", "2"), stringLiteral("int"))))
            .accept(visitor, null))
    );
  }

  @Test
  public void visitArgument() {
    var plan = project(relation("dummy"), field("pewpew", argument("name", intLiteral(0))));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // source=x | eval a = b
  public void reject_query_with_eval() {
    var plan = project(eval(relation("dummy")));
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select highlight("Body") from beer.stackexchange where
  // simple_query_string(["Tags" ^ 1.5, "Title", "Body" 4.2], "taste")
  // and Tags like "% % %" and Title like "%";
  public void accept_query_with_highlight_and_relevance_func() {
    var plan = project(
        filter(
            relation("beer.stackexchange"),
            and(
                and(
                    function("like", qualifiedName("Tags"), stringLiteral("% % %")),
                    function("like", qualifiedName("Title"), stringLiteral("%"))),
                function("simple_query_string",
                    unresolvedArg("fields",
                        new RelevanceFieldList(Map.of("Title", 1.0F, "Body", 4.2F, "Tags", 1.5F))),
                    unresolvedArg("query",
                        stringLiteral("taste"))))),
        alias("highlight(\"Body\")",
            highlight(stringLiteral("Body"), Map.of())));
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select * from y limit z
  public void reject_query_with_limit_but_no_offset() {
    var plan = project(limit(relation("dummy"), 1, 0), allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select * from y limit z, n
  public void reject_query_with_offset() {
    var plan = project(limit(relation("dummy"), 0, 1), allFields());
    assertFalse(plan.accept(visitor, null));
  }

  // test added for coverage only
  @Test
  public void visitLimit() {
    var visitor = new CanPaginateVisitor() {
      @Override
      public Boolean visitRelation(Relation node, Object context) {
        return Boolean.FALSE;
      }
    };
    var plan = project(limit(relation("dummy"), 0, 0), allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select * from y where z
  public void allow_query_with_where() {
    var plan = project(filter(relation("dummy"),
        booleanLiteral(true)), allFields());
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select * from y order by z
  public void allow_query_with_order_by_with_column_references_only() {
    var plan = project(sort(relation("dummy"), field("1")), allFields());
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select * from y order by func(z)
  public void reject_query_with_order_by_with_an_expression() {
    var plan = project(sort(relation("dummy"), field(function("func"))),
        allFields());
    assertFalse(plan.accept(visitor, null));
  }

  // test added for coverage only
  @Test
  public void visitSort() {
    CanPaginateVisitor visitor = new CanPaginateVisitor() {
      @Override
      public Boolean visitRelation(Relation node, Object context) {
        return Boolean.FALSE;
      }
    };
    var plan = project(sort(relation("dummy"), field("1")), allFields());
    assertFalse(plan.accept(visitor, null));
    visitor = new CanPaginateVisitor() {
      @Override
      public Boolean visitField(Field node, Object context) {
        return Boolean.FALSE;
      }
    };
    plan = project(sort(relation("dummy"), field("1")), allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select * from y group by z
  public void reject_query_with_group_by() {
    var plan = project(agg(
        relation("dummy"), List.of(), List.of(), List.of(field("1")), List.of()),
        allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select agg(x) from y
  public void reject_query_with_aggregation_function() {
    var plan = project(agg(
        relation("dummy"),
        List.of(alias("agg", aggregate("func", field("pewpew")))),
        List.of(), List.of(), List.of()),
        allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select window(x) from y
  public void reject_query_with_window_function() {
    var plan = project(relation("dummy"),
        alias("pewpew",
            window(
                aggregate("func", field("pewpew")),
                    List.of(qualifiedName("1")), List.of())));
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select * from y, z
  public void reject_query_with_select_from_multiple_indices() {
    var plan = mock(Project.class);
    when(plan.getChild()).thenReturn(List.of(relation("dummy"), relation("pummy")));
    when(plan.getProjectList()).thenReturn(List.of(allFields()));
    assertFalse(visitor.visitProject(plan, null));
  }

  @Test
  // unreal case, added for coverage only
  public void reject_project_when_relation_has_child() {
    var relation = mock(Relation.class, withSettings().useConstructor(qualifiedName("42")));
    when(relation.getChild()).thenReturn(List.of(relation("pewpew")));
    when(relation.accept(visitor, null)).thenCallRealMethod();
    var plan = mock(Project.class);
    when(plan.getChild()).thenReturn(List.of(relation));
    when(plan.getProjectList()).thenReturn(List.of(allFields()));
    assertFalse(visitor.visitProject((Project) plan, null));
  }

  @Test
  // test combinations of acceptable and not acceptable args for coverage
  public void visitFilter() {
    assertAll(
        () -> assertTrue(project(filter(relation("dummy"), booleanLiteral(true)))
            .accept(visitor, null)),
        () -> assertFalse(project(filter(relation("dummy"), map("1", "2")))
            .accept(visitor, null)),
        () -> assertFalse(project(filter(tableFunction(List.of("1", "2")), booleanLiteral(true)))
            .accept(visitor, null))
    );
  }
}
