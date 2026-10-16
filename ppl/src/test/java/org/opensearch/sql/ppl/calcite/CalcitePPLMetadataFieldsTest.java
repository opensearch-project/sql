/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * Metadata fields ({@code _id}, {@code _index}, {@code _score}, ...) are hidden from the output
 * unless the query selects them with an explicit {@code fields}. For commands that combine several
 * inputs, that decision belongs to each input: a {@code fields} on one side must not expose the
 * metadata of the other side.
 */
public class CalcitePPLMetadataFieldsTest extends CalcitePPLAbstractTest {

  public CalcitePPLMetadataFieldsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add(
        "LOGS",
        new TableWithMetadataFields(
            ImmutableList.of(
                new Object[] {1, "a", "id-1", "logs", 1.0f},
                new Object[] {2, "b", "id-2", "logs", 1.0f})));
    // A lookup table with other metadata values; only id 1 has a match.
    schema.add(
        "DEPTS",
        new TableWithMetadataFields(
            ImmutableList.<Object[]>of(new Object[] {1, "x", "dept-1", "depts", 2.0f})));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  private void verifyOutputFields(String ppl, String... expectedFields) {
    RelNode root = getRelNode(ppl);
    assertEquals(List.of(expectedFields), root.getRowType().getFieldNames());
  }

  /** Plans the query as a request with {@code include_metadata=true} would. */
  private RelNode getRelNodeIncludingMetadata(String ppl) {
    getConfig().context(Contexts.of(RelBuilder.Config.DEFAULT));
    CalcitePlanContext context =
        CalcitePlanContext.create(
            getConfig().build(), SysLimit.fromSettings(settings), QueryType.PPL, true);
    Query query =
        (Query)
            new AstStatementBuilder(
                    new AstBuilder(ppl, settings),
                    AstStatementBuilder.StatementBuilderContext.builder()
                        .includeMetadata(true)
                        .build())
                .visit(pplParser.parse(ppl));
    new CalciteRelNodeVisitor(mock(DataSourceService.class)).analyze(query.getPlan(), context);
    return context.relBuilder.build();
  }

  @Test
  public void testMetadataFieldsHiddenWithoutFields() {
    verifyOutputFields("source=LOGS", "id", "msg");
  }

  @Test
  public void testMetadataFieldsKeptWhenSelected() {
    verifyOutputFields("source=LOGS | fields id, _id", "id", "_id");
  }

  @Test
  public void testAppendWithFieldsInSubsearch() {
    verifyOutputFields("source=LOGS | append [ source=LOGS | fields id ]", "id", "msg");
  }

  @Test
  public void testAppendWithFieldsInMainSearch() {
    verifyOutputFields("source=LOGS | fields id | append [ source=LOGS ]", "id", "msg");
  }

  @Test
  public void testAppendKeepsMetadataSelectedInMainSearch() {
    RelNode root = getRelNode("source=LOGS | fields id, _id | append [ source=LOGS ]");
    assertEquals(List.of("id", "_id", "msg"), root.getRowType().getFieldNames());
    // The subsearch did not select _id, so its rows carry null there instead of their own _id.
    verifyResult(
        root,
        "id=1; _id=id-1; msg=null\n"
            + "id=2; _id=id-2; msg=null\n"
            + "id=1; _id=null; msg=a\n"
            + "id=2; _id=null; msg=b\n");
  }

  @Test
  public void testAppendKeepsMetadataSelectedInSubsearch() {
    verifyOutputFields(
        "source=LOGS | append [ source=LOGS | fields id, _score ]", "id", "msg", "_score");
  }

  @Test
  public void testAppendWithoutFieldsKeepsMetadataSelectableAfterwards() {
    verifyOutputFields("source=LOGS | append [ source=LOGS ] | fields id, _id", "id", "_id");
  }

  @Test
  public void testNestedAppendWithFieldsInInnermostSubsearch() {
    verifyOutputFields(
        "source=LOGS | append [ source=LOGS | append [ source=LOGS | fields id ] ]", "id", "msg");
  }

  @Test
  public void testAppendPipeWithFieldsInSubPipeline() {
    verifyOutputFields("source=LOGS | appendpipe [ fields id ]", "id", "msg");
  }

  @Test
  public void testAppendColWithFieldsInSubsearch() {
    RelNode root = getRelNode("source=LOGS | appendcol [ eval x = 1 | fields x ]");
    assertEquals(List.of("id", "msg", "x"), root.getRowType().getFieldNames());
    verifyResult(root, "id=1; msg=a; x=1\nid=2; msg=b; x=1\n");
  }

  @Test
  public void testAppendColWithFieldsInMainSearch() {
    verifyOutputFields("source=LOGS | fields id | appendcol [ eval x = 1 ]", "id", "msg", "x");
  }

  @Test
  public void testUnionWithFieldsInDataset() {
    verifyOutputFields("source=LOGS | union [ search source=LOGS | fields id ]", "id", "msg");
  }

  @Test
  public void testMultisearchWithFieldsInOneSubsearch() {
    verifyOutputFields(
        "| multisearch [ search source=LOGS | fields id ] [ search source=LOGS ]", "id", "msg");
  }

  @Test
  public void testLookupWithoutOutputListAfterFields() {
    verifyOutputFields("source=LOGS | fields id | lookup LOGS id", "id", "msg");
  }

  @Test
  public void testLookupWithoutOutputListWithoutFields() {
    verifyOutputFields("source=LOGS | lookup LOGS id", "id", "msg");
  }

  @Test
  public void testLookupByMetadataField() {
    verifyOutputFields("source=LOGS | fields msg, _id | lookup LOGS _id", "_id", "id", "msg");
  }

  @Test
  public void testLookupKeepsSourceMetadataForLaterFields() {
    // The lookup table's _id must not replace the source rows' own, which a later fields selects.
    RelNode root = getRelNode("source=LOGS | lookup DEPTS id | fields id, _id");
    verifyResult(root, "id=1; _id=id-1\nid=2; _id=id-2\n");
  }

  @Test
  public void testIncludeMetadataWithLookupKeepsSourceMetadata() {
    // Both tables name their metadata fields alike, so one set is kept: the source rows' own,
    // also for the row without a match.
    RelNode root = getRelNodeIncludingMetadata("source=LOGS | lookup DEPTS id");
    assertEquals(
        List.of("id", "_id", "_index", "_score", "msg"), root.getRowType().getFieldNames());
    verifyResult(
        root,
        "id=1; _id=id-1; _index=logs; _score=1.0; msg=x\n"
            + "id=2; _id=id-2; _index=logs; _score=1.0; msg=null\n");
  }

  @Test
  public void testIncludeMetadataWithLookupOutputsLookupMetadataByName() {
    RelNode root =
        getRelNodeIncludingMetadata(
            "source=LOGS | lookup DEPTS id OUTPUT _id AS dept_doc_id, _index AS dept_index");
    verifyResult(
        root,
        "id=1; msg=a; _id=id-1; _index=logs; _score=1.0; dept_index=depts; dept_doc_id=dept-1\n"
            + "id=2; msg=b; _id=id-2; _index=logs; _score=1.0; dept_index=null;"
            + " dept_doc_id=null\n");
  }

  @Test
  public void testIncludeMetadataKeepsMetadataFieldsOfAppendInputs() {
    RelNode root = getRelNodeIncludingMetadata("source=LOGS | append [ source=LOGS | fields id ]");
    assertEquals(
        List.of("id", "msg", "_id", "_index", "_score"), root.getRowType().getFieldNames());
  }

  /** A table whose schema carries OpenSearch metadata fields next to two ordinary fields. */
  @RequiredArgsConstructor
  static class TableWithMetadataFields implements ScannableTable {
    private final ImmutableList<Object[]> rows;

    @Override
    public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory
          .builder()
          .add("id", SqlTypeName.INTEGER)
          .nullable(true)
          .add("msg", SqlTypeName.VARCHAR)
          .nullable(true)
          .add("_id", SqlTypeName.VARCHAR)
          .nullable(true)
          .add("_index", SqlTypeName.VARCHAR)
          .nullable(true)
          .add("_score", SqlTypeName.REAL)
          .nullable(true)
          .build();
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
    public boolean rolledUpColumnValidInsideAgg(
        String column,
        SqlCall call,
        @Nullable SqlNode parent,
        @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
}
