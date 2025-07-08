/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableList;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.junit.Test;

/** Unit tests for {@code reverse} command in PPL. */
public class CalcitePPLReverseTest extends CalcitePPLAbstractTest {
    public CalcitePPLReverseTest() {
        super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
    }

    @Override
    protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);

        // Main test table with multi-field data
        ImmutableList<Object[]> rows = ImmutableList.of(
                new Object[] {1, "Engineering", "Senior", "Alice"},
                new Object[] {2, "Engineering", "Junior", "Bob"},
                new Object[] {3, "Marketing", "Senior", "Charlie"},
                new Object[] {4, "Marketing", "Junior", "Diana"},
                new Object[] {5, "Engineering", "Senior", "Eve"},
                new Object[] {6, "Sales", "Junior", "Frank"});
        schema.add("EMPLOYEES", new EmployeeTable(rows));
        
        // Empty table for edge case testing
        schema.add("EMPTY", new EmployeeTable(ImmutableList.of()));
        
        // Single row table
        schema.add("SINGLE", new EmployeeTable(ImmutableList.of(new Object[] {99, "IT", "Manager", "Solo"})));

        return Frameworks.newConfigBuilder()
                .defaultSchema(schema);
    }

    @Test
    public void testReverse() {
        String ppl = "source=EMPLOYEES | fields ID, NAME | reverse";
        RelNode root = getRelNode(ppl);

        String expectedResult = """
                ID=6; NAME=Frank
                ID=5; NAME=Eve
                ID=4; NAME=Diana
                ID=3; NAME=Charlie
                ID=2; NAME=Bob
                ID=1; NAME=Alice
                """;
        verifyResult(root, expectedResult);
    }

    @Test
    public void testReverseWithSort() {
        String ppl = "source=EMPLOYEES | sort NAME | fields ID, NAME | reverse";
        RelNode root = getRelNode(ppl);

        String expectedResult = """
                ID=6; NAME=Frank
                ID=5; NAME=Eve
                ID=4; NAME=Diana
                ID=3; NAME=Charlie
                ID=2; NAME=Bob
                ID=1; NAME=Alice
                """;
        verifyResult(root, expectedResult);
    }

    @Test
    public void testDoubleReverse() {
        String ppl = "source=EMPLOYEES | fields ID, NAME | reverse | reverse";
        RelNode root = getRelNode(ppl);

        String expectedResult = """
                ID=1; NAME=Alice
                ID=2; NAME=Bob
                ID=3; NAME=Charlie
                ID=4; NAME=Diana
                ID=5; NAME=Eve
                ID=6; NAME=Frank
                """;
        verifyResult(root, expectedResult);
    }

    @Test
    public void testReverseWithHead() {
        String ppl = "source=EMPLOYEES | fields ID, NAME | reverse | head 2";
        RelNode root = getRelNode(ppl);

        String expectedResult = """
                ID=6; NAME=Frank
                ID=5; NAME=Eve
                """;
        verifyResult(root, expectedResult);
    }

    @Test
    public void testReverseWithFields() {
        String ppl = "source=EMPLOYEES | fields ID | reverse";
        RelNode root = getRelNode(ppl);

        String expectedResult = """
                ID=6
                ID=5
                ID=4
                ID=3
                ID=2
                ID=1
                """;
        verifyResult(root, expectedResult);
    }

    @Test
    public void testReverseEmptyTable() {
        String ppl = "source=EMPTY | reverse";
        RelNode root = getRelNode(ppl);
        
        String expectedResult = "";
        verifyResult(root, expectedResult);
    }

    @Test
    public void testReverseSingleRow() {
        String ppl = "source=SINGLE | fields ID, NAME | reverse";
        RelNode root = getRelNode(ppl);

        String expectedResult = "ID=99; NAME=Solo\n";
        verifyResult(root, expectedResult);
    }

    @Test
    public void testReverseWithComplexPipeline() {
        String ppl = "source=EMPLOYEES | where ID > 3 | fields ID, NAME | reverse | head 1";
        RelNode root = getRelNode(ppl);

        String expectedResult = "ID=6; NAME=Frank\n";
        verifyResult(root, expectedResult);
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

    @Test
    public void testReverseWithMultipleSorts() {
        // Expected: Complete reversal of the final sorted order (NAME->LEVEL->DEPT precedence)
        // NOT: Flipped sort directions which would give (NAME DESC, LEVEL DESC, DEPT DESC)
        String ppl = "source=EMPLOYEES | sort DEPT | sort LEVEL | sort NAME | reverse";
        RelNode root = getRelNode(ppl);


        String expectedResult = """
                ID=6; DEPT=Sales; LEVEL=Junior; NAME=Frank
                ID=5; DEPT=Engineering; LEVEL=Senior; NAME=Eve
                ID=4; DEPT=Marketing; LEVEL=Junior; NAME=Diana
                ID=3; DEPT=Marketing; LEVEL=Senior; NAME=Charlie
                ID=2; DEPT=Engineering; LEVEL=Junior; NAME=Bob
                ID=1; DEPT=Engineering; LEVEL=Senior; NAME=Alice
                """;
        verifyResult(root, expectedResult);
    }

    @RequiredArgsConstructor
    public static class EmployeeTable implements ScannableTable {
        private final ImmutableList<Object[]> rows;

        protected final RelProtoDataType protoRowType = factory ->
                factory.builder()
                        .add("ID", SqlTypeName.INTEGER)
                        .add("DEPT", SqlTypeName.VARCHAR)
                        .add("LEVEL", SqlTypeName.VARCHAR)
                        .add("NAME", SqlTypeName.VARCHAR)
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
        public boolean rolledUpColumnValidInsideAgg(String column, org.apache.calcite.sql.SqlCall call,
                                                    org.apache.calcite.sql.SqlNode parent,
                                                    org.apache.calcite.config.CalciteConnectionConfig config) {
            return false;
        }
    }
}
