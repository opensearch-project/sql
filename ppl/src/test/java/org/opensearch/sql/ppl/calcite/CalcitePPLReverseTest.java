/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** Unit tests for {@code reverse} command in PPL parser. */
public class CalcitePPLReverseTest extends CalcitePPLAbstractTest {
    public CalcitePPLReverseTest() {
        super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
    }

    @Override
    protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

        // Add a test table for reverse operations
        ImmutableList<Object[]> rows = ImmutableList.of(
                new Object[] {1, "Alice", "Engineering", "Senior"},
                new Object[] {2, "Bob", "Engineering", "Junior"},
                new Object[] {3, "Charlie", "Marketing", "Senior"},
                new Object[] {4, "Diana", "Marketing", "Junior"},
                new Object[] {5, "Eve", "Engineering", "Senior"},
                new Object[] {6, "Frank", "Sales", "Junior"});
        schema.add("EMPLOYEES", new EmployeeTable(rows));

        return Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(schema)
                .traitDefs((List<RelTraitDef>) null)
                .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
    }

    @Test
    public void testReverseParserSuccess() {
        String ppl = "source=EMPLOYEES | reverse";
        // Test that parser can successfully parse the reverse command
        getRelNode(ppl);
    }

    @Test
    public void testReverseWithSortParserSuccess() {
        String ppl = "source=EMPLOYEES | sort NAME | reverse";
        // Test that parser can successfully parse reverse with preceding sort
        getRelNode(ppl);
    }

    @Test
    public void testDoubleReverseParserSuccess() {
        String ppl = "source=EMPLOYEES | reverse | reverse";
        // Test that parser can successfully parse double reverse
        getRelNode(ppl);
    }

    @Test
    public void testReverseWithHeadParserSuccess() {
        String ppl = "source=EMPLOYEES | reverse | head 2";
        // Test that parser can successfully parse reverse with head
        getRelNode(ppl);
    }

    @Test(expected = Exception.class)
    public void testReverseWithNumberShouldFail() {
        String ppl = "source=EMPLOYEES | reverse 2";
        getRelNode(ppl);
    }

    @Test(expected = Exception.class)
    public void testReverseWithFieldShouldFail() {
        String ppl = "source=EMPLOYEES | reverse ID";
        getRelNode(ppl);
    }

    @Test(expected = Exception.class)
    public void testReverseWithStringShouldFail() {
        String ppl = "source=EMPLOYEES | reverse \"desc\"";
        getRelNode(ppl);
    }

    @Test(expected = Exception.class)
    public void testReverseWithExpressionShouldFail() {
        String ppl = "source=EMPLOYEES | reverse ID + 1";
        getRelNode(ppl);
    }

    @RequiredArgsConstructor
    public static class EmployeeTable implements ScannableTable {
        private final ImmutableList<Object[]> rows;

        protected final RelProtoDataType protoRowType = factory ->
                factory.builder()
                        .add("ID", SqlTypeName.INTEGER)
                        .add("NAME", SqlTypeName.VARCHAR)
                        .add("DEPT", SqlTypeName.VARCHAR)
                        .add("LEVEL", SqlTypeName.VARCHAR)
                        .build();

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(rows);
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return protoRowType.apply(typeFactory);
        }

        @Override
        public Statistic getStatistic() {
            return Statistics.of(0d, ImmutableList.of(), RelCollations.createSingleton(0));
        }

        @Override
        public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        @Override
        public boolean isRolledUp(String column) {
            return false;
        }

        @Override
        public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
                                                    @Nullable SqlNode parent,
                                                    @Nullable CalciteConnectionConfig config) {
            return false;
        }
    }
}