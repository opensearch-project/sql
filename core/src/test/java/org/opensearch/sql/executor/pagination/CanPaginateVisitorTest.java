/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.pagination;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.executor.pagination.CanPaginateVisitor;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class CanPaginateVisitorTest {

  static final CanPaginateVisitor visitor = new CanPaginateVisitor();

  @Test
  // select * from y
  public void accept_query_with_select_star_and_from() {
    var plan = AstDSL.project(AstDSL.relation("dummy"), AstDSL.allFields());
    assertTrue(plan.accept(visitor, null));
  }

  @Test
  // select x from y
  public void reject_query_with_select_field_and_from() {
    var plan = AstDSL.project(AstDSL.relation("dummy"), AstDSL.field("pewpew"));
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select x,z from y
  public void reject_query_with_select_fields_and_from() {
    var plan = AstDSL.project(AstDSL.relation("dummy"),
        AstDSL.field("pewpew"), AstDSL.field("pewpew"));
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select x
  public void reject_query_without_from() {
    var plan = AstDSL.project(AstDSL.values(List.of(AstDSL.intLiteral(1))),
        AstDSL.alias("1",AstDSL.intLiteral(1)));
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select * from y limit z
  public void reject_query_with_limit() {
    var plan = AstDSL.project(AstDSL.limit(AstDSL.relation("dummy"), 1, 2), AstDSL.allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select * from y where z
  public void reject_query_with_where() {
    var plan = AstDSL.project(AstDSL.filter(AstDSL.relation("dummy"),
        AstDSL.booleanLiteral(true)), AstDSL.allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select * from y order by z
  public void reject_query_with_order_by() {
    var plan = AstDSL.project(AstDSL.sort(AstDSL.relation("dummy"), AstDSL.field("1")),
        AstDSL.allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select * from y group by z
  public void reject_query_with_group_by() {
    var plan = AstDSL.project(AstDSL.agg(
        AstDSL.relation("dummy"), List.of(), List.of(), List.of(AstDSL.field("1")), List.of()),
        AstDSL.allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select agg(x) from y
  public void reject_query_with_aggregation_function() {
    var plan = AstDSL.project(AstDSL.agg(
        AstDSL.relation("dummy"),
        List.of(AstDSL.alias("agg", AstDSL.aggregate("func", AstDSL.field("pewpew")))),
        List.of(), List.of(), List.of()),
        AstDSL.allFields());
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select window(x) from y
  public void reject_query_with_window_function() {
    var plan = AstDSL.project(AstDSL.relation("dummy"),
        AstDSL.alias("pewpew",
            AstDSL.window(
                AstDSL.aggregate("func", AstDSL.field("pewpew")),
                    List.of(AstDSL.qualifiedName("1")), List.of())));
    assertFalse(plan.accept(visitor, null));
  }

  @Test
  // select * from y, z
  public void reject_query_with_select_from_multiple_indices() {
    var plan = mock(Project.class);
    when(plan.getChild()).thenReturn(List.of(AstDSL.relation("dummy"), AstDSL.relation("pummy")));
    when(plan.getProjectList()).thenReturn(List.of(AstDSL.allFields()));
    assertFalse(visitor.visitProject(plan, null));
  }

  @Test
  // unreal case, added for coverage only
  public void reject_project_when_relation_has_child() {
    var relation = mock(Relation.class, withSettings().useConstructor(AstDSL.qualifiedName("42")));
    when(relation.getChild()).thenReturn(List.of(AstDSL.relation("pewpew")));
    when(relation.accept(visitor, null)).thenCallRealMethod();
    var plan = mock(Project.class);
    when(plan.getChild()).thenReturn(List.of(relation));
    when(plan.getProjectList()).thenReturn(List.of(AstDSL.allFields()));
    assertFalse(visitor.visitProject((Project) plan, null));
  }
}
