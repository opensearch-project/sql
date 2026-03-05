/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.ast.dsl.AstDSL.argument;
import static org.opensearch.sql.ast.dsl.AstDSL.booleanLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.field;
import static org.opensearch.sql.ast.dsl.AstDSL.fillNull;
import static org.opensearch.sql.ast.dsl.AstDSL.filter;
import static org.opensearch.sql.ast.dsl.AstDSL.map;
import static org.opensearch.sql.ast.dsl.AstDSL.mvcombine;
import static org.opensearch.sql.ast.dsl.AstDSL.project;
import static org.opensearch.sql.ast.dsl.AstDSL.projectWithArg;
import static org.opensearch.sql.ast.dsl.AstDSL.relation;
import static org.opensearch.sql.ast.dsl.AstDSL.rename;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.ReplacePair;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelBuilder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.executor.QueryType;

@ExtendWith(MockitoExtension.class)
public class MapPathPreMaterializerTest {

  @Mock private CalciteRexNodeVisitor rexVisitor;
  @Mock private OpenSearchRelBuilder relBuilder;

  /** Intercepts static factory methods in {@link CalciteToolsHelper} during context creation. */
  @Mock private MockedStatic<CalciteToolsHelper> mockedToolsHelper;

  /** Placeholder child plan node required by commands like rename and fillnull. */
  private static final UnresolvedPlan DUMMY_CHILD = relation("t");

  private CalcitePlanContext context;
  private MapPathPreMaterializer materializer;

  @BeforeEach
  void setUp() {
    materializer = new MapPathPreMaterializer(rexVisitor);
    mockedToolsHelper
        .when(() -> CalciteToolsHelper.connect(any(), any()))
        .thenReturn(mock(Connection.class));
    mockedToolsHelper
        .when(() -> CalciteToolsHelper.create(any(), any(), any()))
        .thenReturn(relBuilder);
    when(relBuilder.getRexBuilder()).thenReturn(new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY));
    context =
        CalcitePlanContext.create(mock(FrameworkConfig.class), SysLimit.DEFAULT, QueryType.PPL);
    lenient().when(relBuilder.size()).thenReturn(1);
    lenient().when(relBuilder.peek()).thenReturn(mock(RelNode.class, RETURNS_DEEP_STUBS));
  }

  // ---- Symbol-based commands ----

  @Test
  void testRename() {
    givenMapPaths("doc.user.name")
        .whenCommand(rename(DUMMY_CHILD, map("doc.user.name", "username")))
        .shouldProject("doc.user.name");
  }

  @Test
  void testFillnull() {
    givenMapPaths("doc.user.name")
        .whenCommand(
            fillNull(DUMMY_CHILD, List.of(Pair.of(field("doc.user.name"), stringLiteral("N/A")))))
        .shouldProject("doc.user.name");
  }

  @Test
  void testReplace() {
    givenMapPaths("doc.user.name")
        .whenCommand(
            new Replace(
                List.of(new ReplacePair(stringLiteral("a"), stringLiteral("b"))),
                Set.of(field("doc.user.name"))))
        .shouldProject("doc.user.name");
  }

  @Test
  void testFieldsExclusion() {
    givenMapPaths("doc.user.name")
        .whenCommand(
            projectWithArg(
                DUMMY_CHILD,
                List.of(argument("exclude", booleanLiteral(true))),
                field("doc.user.name")))
        .shouldProject("doc.user.name");
  }

  @Test
  void testAddtotals() {
    givenMapPaths("doc.user.name")
        .whenCommand(new AddTotals(List.of(field("doc.user.name")), Map.of()))
        .shouldProject("doc.user.name");
  }

  @Test
  void testMvcombine() {
    givenMapPaths("doc.user.name")
        .whenCommand(mvcombine(field("doc.user.name")))
        .shouldProject("doc.user.name");
  }

  // ---- Multiple fields cases ----

  @Test
  void testMultipleMapPaths() {
    givenMapPaths("doc.user.name", "doc.user.age")
        .whenCommand(
            fillNull(
                DUMMY_CHILD,
                List.of(
                    Pair.of(field("doc.user.name"), stringLiteral("N/A")),
                    Pair.of(field("doc.user.age"), stringLiteral("0")))))
        .shouldProject("doc.user.name", "doc.user.age");
  }

  @Test
  void testMixedMapAndNonMapFields() {
    givenMapPaths("doc.user.name")
        .givenNonMapPaths("regular_field")
        .whenCommand(
            fillNull(
                DUMMY_CHILD,
                List.of(
                    Pair.of(field("doc.user.name"), stringLiteral("N/A")),
                    Pair.of(field("regular_field"), stringLiteral("0")))))
        .shouldProject("doc.user.name");
  }

  // ---- No-op cases ----

  @Test
  void testNoOpForFilter() {
    givenMapPaths("doc.user.name")
        .whenCommand(filter(DUMMY_CHILD, field("doc.user.name")))
        .shouldNotProject();
  }

  @Test
  void testNoOpForNonExcludedProject() {
    givenMapPaths("doc.user.name")
        .whenCommand(project(DUMMY_CHILD, field("doc.user.name")))
        .shouldNotProject();
  }

  @Test
  void testNoOpWhenRelBuilderStackEmpty() {
    lenient().when(relBuilder.size()).thenReturn(0);
    givenMapPaths("doc.user.name")
        .whenCommand(rename(DUMMY_CHILD, map("doc.user.name", "u")))
        .shouldNotProject();
  }

  @Test
  void testNoOpWhenFieldResolvesToNonItemAccess() {
    givenMapPaths()
        .givenNonMapPaths("regular_field")
        .whenCommand(rename(DUMMY_CHILD, map("regular_field", "alias")))
        .shouldNotProject();
  }

  // ---- Fluent test helper ----

  private MapPathAssertion givenMapPaths(String... fieldNames) {
    return new MapPathAssertion(fieldNames);
  }

  private class MapPathAssertion {
    private final Map<String, RexNode> aliasedNodes = new LinkedHashMap<>();

    MapPathAssertion(String... mapFieldNames) {
      for (String name : mapFieldNames) {
        RexNode item = mock(RexNode.class, "item:" + name);
        RexNode aliased = mock(RexNode.class, "aliased:" + name);
        lenient().when(item.getKind()).thenReturn(SqlKind.ITEM);
        lenient().when(rexVisitor.analyze(fieldMatching(name), eq(context))).thenReturn(item);
        lenient().when(relBuilder.alias(item, name)).thenReturn(aliased);
        aliasedNodes.put(name, aliased);
      }
    }

    MapPathAssertion givenNonMapPaths(String... fieldNames) {
      for (String name : fieldNames) {
        RexNode ref = mock(RexNode.class, "ref:" + name);
        when(ref.getKind()).thenReturn(SqlKind.INPUT_REF);
        lenient().when(rexVisitor.analyze(fieldMatching(name), eq(context))).thenReturn(ref);
      }
      return this;
    }

    MapPathAssertion whenCommand(UnresolvedPlan command) {
      materializer.materializePaths(command, context);
      return this;
    }

    void shouldProject(String... expectedFields) {
      List<RexNode> expected = Stream.of(expectedFields).map(aliasedNodes::get).toList();
      verify(relBuilder).projectPlus(expected);
    }

    void shouldNotProject() {
      verify(relBuilder, never()).projectPlus(any(List.class));
    }

    private static UnresolvedExpression fieldMatching(String name) {
      return argThat(expr -> expr instanceof Field f && f.getField().toString().equals(name));
    }
  }
}
