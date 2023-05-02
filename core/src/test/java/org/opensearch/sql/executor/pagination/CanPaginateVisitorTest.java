/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Literal;
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
  public void reject_query_with_select_field_and_from() {
    var plan = project(relation("dummy"), field("pewpew"));
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select x,z from y
  public void reject_query_with_select_fields_and_from() {
    var plan = project(relation("dummy"),
        field("pewpew"), field("pewpew"));
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select x
  public void allow_query_without_from() {
    // bypass check for `AllFields` in `visitProject`
    // TODO remove after #1500 merge https://github.com/opensearch-project/sql/pull/1500
    var visitor = new CanPaginateVisitor() {
      @Override
      public Boolean visitProject(Project node, Object context) {
        return node.getChild().get(0).accept(this, context);
      }
    };
    var plan = project(values(List.of(intLiteral(1))),
        alias("1", intLiteral(1)));
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
  public void reject_query_with_where() {
    var plan = project(filter(relation("dummy"),
        booleanLiteral(true)), allFields());
    assertFalse(plan.accept(visitor, null));
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
    var visitor = new CanPaginateVisitor() {
      @Override
      public Boolean visitRelation(Relation node, Object context) {
        return Boolean.FALSE;
      }
    };
    var plan = project(sort(relation("dummy"), field("1")), allFields());
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
  // test added just for coverage
  public void canPaginate() {
    assertTrue(visitor.canPaginate(new Node() {
      @Override
      public List<? extends Node> getChild() {
        return null;
      }
    }, null));
  }
}
