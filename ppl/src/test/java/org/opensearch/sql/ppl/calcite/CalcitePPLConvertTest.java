/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** Unit tests for PPL convert command. */
public class CalcitePPLConvertTest extends CalcitePPLAbstractTest {

  private static final int WIDE_SUBFIELDS = 100_000;

  public CalcitePPLConvertTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add("WIDE", new CalcitePPLWideNestedSchemaTest.WideNestedTable(WIDE_SUBFIELDS));
    schema.add("REPEATED", new RepeatedNamesTable());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /**
   * Resolving every column through {@code RelBuilder.field(String)}, which searches the field names
   * linearly on each call, made convert O(columns^2); this query took about 40s. The timeout
   * catches a regression.
   */
  @Test(timeout = 10_000)
  public void testConvertOnWideSchemaIsLinear() {
    RelNode root = getRelNode("source=WIDE | convert num(NAME), auto(ID) AS ID_NUM");
    String expectedLogical =
        "LogicalProject(ID=[$0], ATTRS=[$1], LONE.X=[$3], NAME=[NUM($"
            + (WIDE_SUBFIELDS + 5)
            + ")], ID_NUM=[AUTO($0)])\n"
            + "  LogicalTableScan(table=[[scott, WIDE]])\n";
    verifyLogical(root, expectedLogical);
  }

  /**
   * Unconverted columns resolve the way {@code RelBuilder.field(String)} does: names match
   * case-sensitively, and a repeated name resolves to its first column.
   */
  @Test
  public void testConvertResolvesColumnsLikeFieldByName() {
    RelNode root = getRelNode("source=REPEATED | convert num(B)");
    String expectedLogical =
        "LogicalProject(A=[$0], a=[$1], B=[NUM($2)], A0=[$0])\n"
            + "  LogicalTableScan(table=[[scott, REPEATED]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testConvertBasic() {
    String ppl = "source=EMP | convert auto(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[AUTO($5)],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, AUTO(`SAL`) `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithAlias() {
    String ppl = "source=EMP | convert auto(SAL) AS salary_num";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], salary_num=[AUTO($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, AUTO(`SAL`)"
            + " `salary_num`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertMultipleFunctions() {
    String ppl = "source=EMP | convert auto(SAL), num(COMM)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[AUTO($5)],"
            + " COMM=[NUM($6)], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, AUTO(`SAL`) `SAL`, NUM(`COMM`) `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertMultipleWithAliases() {
    String ppl = "source=EMP | convert auto(SAL) AS salary, num(COMM) AS commission";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], salary=[AUTO($5)], commission=[NUM($6)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, AUTO(`SAL`)"
            + " `salary`, NUM(`COMM`) `commission`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithFields() {
    String ppl = "source=EMP | convert auto(SAL) AS salary_num | fields EMPNO, ENAME, salary_num";
    RelNode root = getRelNode(ppl);
    // Calcite optimizes the two projections into one - this is more efficient
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], salary_num=[AUTO($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, AUTO(`SAL`) `salary_num`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertNumFunction() {
    String ppl = "source=EMP | convert num(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[NUM($5)],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, NUM(`SAL`) `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertRmcommaFunction() {
    String ppl = "source=EMP | convert rmcomma(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[RMCOMMA($1)], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, RMCOMMA(`ENAME`) `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertRmunitFunction() {
    String ppl = "source=EMP | convert rmunit(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[RMUNIT($1)], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, RMUNIT(`ENAME`) `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertMemkFunction() {
    String ppl = "source=EMP | convert memk(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[MEMK($1)], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, MEMK(`ENAME`) `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertNoneFunction() {
    String ppl = "source=EMP | convert none(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical = "LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT *\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithWhere() {
    String ppl = "source=EMP | where DEPTNO = 10 | convert auto(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[AUTO($5)],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, AUTO(`SAL`) `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithSort() {
    String ppl = "source=EMP | convert auto(SAL) AS salary_num | sort - salary_num | head 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$8], dir0=[DESC-nulls-last], fetch=[3])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], salary_num=[AUTO($5)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, AUTO(`SAL`)"
            + " `salary_num`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY 9 DESC\n"
            + "LIMIT 3";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithStats() {
    String ppl = "source=EMP | convert auto(SAL) AS salary_num | stats avg(salary_num) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(avg(salary_num)=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], avg(salary_num)=[AVG($1)])\n"
            + "    LogicalProject(DEPTNO=[$7], salary_num=[AUTO($5)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT AVG(AUTO(`SAL`)) `avg(salary_num)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertAllFunctions() {
    String ppl =
        "source=EMP | convert auto(SAL) AS sal_auto, num(COMM) AS comm_num, rmcomma(ENAME) AS"
            + " name_clean, rmunit(JOB) AS job_clean, none(EMPNO) AS empno_same";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], sal_auto=[AUTO($5)], comm_num=[NUM($6)],"
            + " name_clean=[RMCOMMA($1)], job_clean=[RMUNIT($2)], empno_same=[$0])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, AUTO(`SAL`)"
            + " `sal_auto`, NUM(`COMM`) `comm_num`, RMCOMMA(`ENAME`) `name_clean`, RMUNIT(`JOB`)"
            + " `job_clean`, `EMPNO` `empno_same`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertAutoWithMemoryField() {
    String ppl = "source=EMP | convert auto(JOB) AS memory_size";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], memory_size=[AUTO($2)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, AUTO(`JOB`)"
            + " `memory_size`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertMktimeFunction() {
    String ppl = "source=EMP | convert mktime(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[MKTIME($1)], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, MKTIME(`ENAME`) `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertCtimeFunction() {
    String ppl = "source=EMP | convert ctime(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[CTIME($5)],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, CTIME(`SAL`) `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertDur2secFunction() {
    String ppl = "source=EMP | convert dur2sec(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[DUR2SEC($1)], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, DUR2SEC(`ENAME`) `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertMstimeFunction() {
    String ppl = "source=EMP | convert mstime(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[MSTIME($1)], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, MSTIME(`ENAME`) `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`,"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithTimeformatMktime() {
    String ppl = "source=EMP | convert timeformat=\"%Y-%m-%d\" mktime(ENAME)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[MKTIME($1, '%Y-%m-%d')], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, MKTIME(`ENAME`, '%Y-%m-%d') `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`,"
            + " `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertWithTimeformatCtime() {
    String ppl = "source=EMP | convert timeformat=\"%Y-%m-%d %H:%M:%S\" ctime(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[CTIME($5,"
            + " '%Y-%m-%d %H:%M:%S')], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, CTIME(`SAL`, '%Y-%m-%d %H:%M:%S')"
            + " `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertTimeformatWithMultipleFunctions() {
    String ppl = "source=EMP | convert timeformat=\"%Y-%m-%d\" mktime(ENAME), ctime(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[MKTIME($1, '%Y-%m-%d')], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[CTIME($5, '%Y-%m-%d')], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, MKTIME(`ENAME`, '%Y-%m-%d') `ENAME`, `JOB`, `MGR`, `HIREDATE`,"
            + " CTIME(`SAL`, '%Y-%m-%d') `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testConvertTimeformatMixedWithNonTimeFunctions() {
    String ppl = "source=EMP | convert timeformat=\"%Y-%m-%d\" mktime(ENAME), auto(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[MKTIME($1, '%Y-%m-%d')], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[AUTO($5)], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, MKTIME(`ENAME`, '%Y-%m-%d') `ENAME`, `JOB`, `MGR`, `HIREDATE`, AUTO(`SAL`)"
            + " `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Columns A, a, B, A: a repeated name and a name that differs only in case. */
  private static class RepeatedNamesTable extends AbstractTable implements ScannableTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      return factory
          .builder()
          .add("A", SqlTypeName.VARCHAR)
          .add("a", SqlTypeName.VARCHAR)
          .add("B", SqlTypeName.VARCHAR)
          .add("A", SqlTypeName.VARCHAR)
          .build();
    }

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.emptyEnumerable();
    }
  }
}
