/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.opensearch.sql.ast.dsl.AstDSL.*;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.*;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelBuilder;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.QueryType;

/**
 * Unit tests for {@link MapPathPreMaterializer}.
 *
 * <p>Tests the three-step pipeline: extractOperands → collectMapPaths → materialize.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class MapPathPreMaterializerTest {

  @Mock DataSourceService dataSourceService;
  @Mock RelOptCluster cluster;
  @Mock RelOptPlanner planner;
  @Mock RelMetadataQuery mq;
  @Mock RelNode input;
  @Mock Connection connection;
  @Mock FrameworkConfig frameworkConfig;

  RexBuilder rexBuilder = new RexBuilder(TYPE_FACTORY);
  OpenSearchRelBuilder relBuilder;
  CalcitePlanContext context;
  MapPathPreMaterializer materializer;
  MockedStatic<CalciteToolsHelper> mockedStatic;
  RelDataType mapType;
  CalciteRexNodeVisitor spyRexVisitor;

  @BeforeEach
  public void setUp() {
    when(cluster.getTypeFactory()).thenReturn(TYPE_FACTORY);
    when(cluster.getRexBuilder()).thenReturn(rexBuilder);
    when(mq.isVisibleInExplain(any(), any())).thenReturn(true);
    when(cluster.getMetadataQuery()).thenReturn(mq);
    when(cluster.traitSet()).thenReturn(RelTraitSet.createEmpty());
    when(cluster.traitSetOf(Convention.NONE))
        .thenReturn(RelTraitSet.createEmpty().replace(Convention.NONE));
    when(cluster.getPlanner()).thenReturn(planner);
    when(planner.getExecutor()).thenReturn(null);

    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    mapType =
        TYPE_FACTORY.createMapType(
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR),
            TYPE_FACTORY.createTypeWithNullability(
                TYPE_FACTORY.createSqlType(SqlTypeName.ANY), true));
    RelDataType inputRowType =
        TYPE_FACTORY.createStructType(List.of(intType, mapType), List.of("id", "doc"));
    when(input.getCluster()).thenReturn(cluster);
    when(input.getRowType()).thenReturn(inputRowType);

    relBuilder = new OpenSearchRelBuilder(null, cluster, null);
    mockedStatic = Mockito.mockStatic(CalciteToolsHelper.class);
    mockedStatic.when(() -> CalciteToolsHelper.connect(any(), any())).thenReturn(connection);
    mockedStatic.when(() -> CalciteToolsHelper.create(any(), any(), any())).thenReturn(relBuilder);
    context = CalcitePlanContext.create(frameworkConfig, SysLimit.DEFAULT, QueryType.PPL);

    spyRexVisitor = spy(new CalciteRexNodeVisitor(new CalciteRelNodeVisitor(dataSourceService)));
    doAnswer(
            inv -> {
              UnresolvedExpression expr = inv.getArgument(0);
              if (expr instanceof Field f) {
                String path = f.getField().toString();
                String nested = path.contains(".") ? path.substring(path.indexOf('.') + 1) : path;
                return rexBuilder.makeCall(
                    org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM,
                    rexBuilder.makeInputRef(mapType, 1),
                    rexBuilder.makeLiteral(nested));
              }
              throw new IllegalArgumentException("Cannot resolve: " + expr);
            })
        .when(spyRexVisitor)
        .analyze(any(UnresolvedExpression.class), any(CalcitePlanContext.class));
    materializer = new MapPathPreMaterializer(spyRexVisitor);
  }

  @AfterEach
  public void tearDown() {
    mockedStatic.close();
  }

  private static Field field(String name) {
    return new Field(QualifiedName.of(name));
  }

  private static final UnresolvedPlan DUMMY_CHILD = new Relation(QualifiedName.of("dummy"));

  // ---- End-to-end: materializePaths ----

  static Stream<Arguments> materializationCases() {
    return Stream.of(
        Arguments.of("rename", rename(DUMMY_CHILD, map("doc.user.name", "username"))),
        Arguments.of(
            "fillnull",
            fillNull(DUMMY_CHILD, List.of(Pair.of(field("doc.user.name"), stringLiteral("N/A"))))),
        Arguments.of(
            "replace",
            new Replace(
                List.of(new ReplacePair(stringLiteral("a"), stringLiteral("b"))),
                Set.of(field("doc.user.name")))),
        Arguments.of(
            "fields -",
            projectWithArg(
                DUMMY_CHILD,
                List.of(argument("exclude", booleanLiteral(true))),
                field("doc.user.name"))),
        Arguments.of(
            "addtotals", new AddTotals(List.of(field("doc.user.name")), java.util.Map.of())),
        Arguments.of("mvcombine", mvcombine(field("doc.user.name"))));
  }

  @ParameterizedTest(name = "materialize: {0}")
  @MethodSource("materializationCases")
  public void testMaterializeProjectsMapPath(String desc, UnresolvedPlan command) {
    relBuilder.push(input);
    materializer.materializePaths(command, context);
    RelNode top = relBuilder.peek();
    assertInstanceOf(LogicalProject.class, top);
    assertEquals(
        "LogicalProject(id=[$0], doc=[$1], doc.user.name=[ITEM($1, 'user.name')])\n",
        top.explain().replaceAll("\\r\\n", "\n"));
  }

  @Test
  public void testMaterializeNoOpForNonCategoryA() {
    relBuilder.push(input);
    materializer.materializePaths(new Filter(field("x")), context);
    assertSame(input, relBuilder.peek());
  }
}
