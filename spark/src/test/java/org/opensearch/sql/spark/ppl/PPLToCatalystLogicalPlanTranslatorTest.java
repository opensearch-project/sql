/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.ppl;

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar$;
import org.apache.spark.sql.catalyst.analysis.UnresolvedTable;
import org.apache.spark.sql.catalyst.expressions.EqualTo;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstExpressionBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;
import org.opensearch.sql.spark.client.SparkClient;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.internal.Trees;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.List.of;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static scala.collection.JavaConverters.asScalaBuffer;


public class PPLToCatalystLogicalPlanTranslatorTest {
    private PPLSyntaxParser parser = new PPLSyntaxParser();
    @Mock
    private SparkClient sparkClient;

    @Mock
    private LogicalProject logicalProject;
    private CatalystPlanContext context = new CatalystPlanContext();

    private Statement plan(String query, boolean isExplain) {
        final AstStatementBuilder builder =
                new AstStatementBuilder(
                        new AstBuilder(new AstExpressionBuilder(), query),
                        AstStatementBuilder.StatementBuilderContext.builder().isExplain(isExplain).build());
        return builder.visit(parser.parse(query));
    }

    @Test
    void testSearchWithTableAllFieldsPlan() {
        Statement plan = plan("search source = table ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        Seq<?> projectList = JavaConverters.asScalaBuffer(Collections.singletonList((Object) UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()))).toSeq();
        Project expectedPlan = new Project((Seq<NamedExpression>) projectList, new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty()));
        Assertions.assertEquals(context.getPlan().toString(), expectedPlan.toString());
    }

    @Test
    void testSourceWithTableAllFieldsPlan() {
        Statement plan = plan("source = table ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        Seq<?> projectList = JavaConverters.asScalaBuffer(Collections.singletonList((Object) UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()))).toSeq();
        Project expectedPlan = new Project((Seq<NamedExpression>) projectList, new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty()));
        Assertions.assertEquals(context.getPlan().toString(), expectedPlan.toString());
    }

    @Test
    void testSourceWithTableOneFieldPlan() {
        Statement plan = plan("source=table | fields A", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        // Create a Project node for fields A and B
        List<NamedExpression> projectList = Arrays.asList(
                UnresolvedAttribute$.MODULE$.apply(JavaConverters.asScalaBuffer(Collections.singletonList("A")))
        );
        Project expectedPlan = new Project(JavaConverters.asScalaBuffer(projectList).toSeq(), new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty()));
        Assertions.assertEquals(context.getPlan().toString(), expectedPlan.toString());
    }

    @Test
    void testSourceWithTableAndConditionPlan() {
        Statement plan = plan("source=t a = 1 ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        // Create a Project node for fields A and B
        List<NamedExpression> projectList = Arrays.asList(
                UnresolvedAttribute$.MODULE$.apply(JavaConverters.asScalaBuffer(Collections.singletonList("a")))
        );
        UnresolvedTable table = new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty());
        // Create a Filter node for the condition 'a = 1'
        EqualTo filterCondition = new EqualTo((Expression)projectList.get(0), Literal.create(1,IntegerType));
        LogicalPlan filterPlan = new Filter(filterCondition, table);
        Assertions.assertEquals(context.getPlan().toString(), filterPlan.toString());
    }

    @Test
    void testSourceWithTableTwoFieldPlan() {
        Statement plan = plan("source=table | fields A, B", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        // Create a Project node for fields A and B
        List<NamedExpression> projectList = Arrays.asList(
                UnresolvedAttribute$.MODULE$.apply(JavaConverters.asScalaBuffer(Collections.singletonList("A"))),
                UnresolvedAttribute$.MODULE$.apply(JavaConverters.asScalaBuffer(Collections.singletonList("B")))
        );
        Project expectedPlan = new Project(JavaConverters.asScalaBuffer(projectList).toSeq(), new UnresolvedTable(asScalaBuffer(of("table")).toSeq(), "source=table ", Option.<String>empty()));
        Assertions.assertEquals(context.getPlan().toString(), expectedPlan.toString());
    }

    @Test
    void testSearchWithMultiTablesPlan() {
        Statement plan = plan("search source = table1, table2 ", false);
        CatalystQueryPlanVisitor planVisitor = new CatalystQueryPlanVisitor();
        planVisitor.visit(plan, context);
        Seq<?> projectList = JavaConverters.asScalaBuffer(Collections.singletonList((Object) UnresolvedStar$.MODULE$.apply(Option.<Seq<String>>empty()))).toSeq();
        Project expectedPlanTable1 = new Project((Seq<NamedExpression>) projectList, new UnresolvedTable(asScalaBuffer(of("table1")).toSeq(), "source=table ", Option.<String>empty()));
        Project expectedPlanTable2 = new Project((Seq<NamedExpression>) projectList, new UnresolvedTable(asScalaBuffer(of("table2")).toSeq(), "source=table ", Option.<String>empty()));
        // Create a Union logical plan
        Seq<LogicalPlan> unionChildren = JavaConverters.asScalaBuffer(Arrays.asList((LogicalPlan) expectedPlanTable1, (LogicalPlan) expectedPlanTable2)).toSeq();
        // todo : parameterize the union options byName & allowMissingCol
        LogicalPlan unionPlan = new Union(unionChildren, true, false);
        Assertions.assertEquals(context.getPlan().toString(), unionPlan.toString());
    }

}


