/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.opensearch.sql.calcite.FlatObjectScopeValidator;
import org.opensearch.sql.common.error.ErrorReport;

/**
 * That PPL syntax reaches the rules a flat_object field is held to; the rules themselves are
 * covered over plans in {@code FlatObjectScopeValidatorTest}. The field is recognized by its
 * Calcite type, MAP&lt;VARCHAR, VARIANT&gt;, which the table below registers.
 */
public class CalcitePPLFlatObjectScopeTest extends CalcitePPLAbstractTest {

  public CalcitePPLFlatObjectScopeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /** A table with a flat_object column, as OpenSearch registers one. */
  public static class TableWithFlatObject implements Table {
    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("SERVICE", SqlTypeName.VARCHAR)
                .add(
                    "ATTRS",
                    factory.createMapType(
                        factory.createSqlType(SqlTypeName.VARCHAR),
                        factory.createTypeWithNullability(
                            factory.createSqlType(SqlTypeName.VARIANT), true)))
                .build();

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
    public boolean rolledUpColumnValidInsideAgg(
        String column,
        SqlCall call,
        @Nullable SqlNode parent,
        @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add("OTEL", new TableWithFlatObject());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((java.util.List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  private void allowed(String ppl) {
    RelNode root = getRelNode(ppl);
    FlatObjectScopeValidator.validate(root);
  }

  /**
   * Rejected either while the plan is built (a function that cannot take a leaf, refused by the
   * function resolver) or by the validator afterwards; the message carries the reason either way.
   */
  private void rejected(String ppl, String what) {
    Exception ex =
        assertThrows(
            Exception.class,
            () -> {
              RelNode root = getRelNode(ppl);
              FlatObjectScopeValidator.validate(root);
            });
    String msg = String.valueOf(ex.getMessage());
    assertTrue(msg, msg.contains(what));
    // the rule and the doc link ride on the report, not on the message
    assertTrue(msg, ex instanceof ErrorReport);
    String suggestion = String.valueOf(((ErrorReport) ex).getSuggestion());
    assertTrue(
        suggestion,
        suggestion.contains(
            "A flat_object field can only be projected, or looked up in the index -- by value, by"
                + " existence or by a pattern"));
    assertTrue(
        suggestion,
        suggestion.contains(
            "https://docs.opensearch.org/latest/mappings/supported-field-types/flat-object/"));
  }

  // ---- allowed: projection

  @Test
  public void projectTheFieldAndALeaf() {
    allowed("source=OTEL | fields ATTRS");
    allowed("source=OTEL | fields SERVICE, ATTRS.duration_ms");
    allowed("source=OTEL | eval d = ATTRS.duration_ms | fields d");
    allowed("source=OTEL | rename ATTRS.duration_ms as d | fields d");
  }

  // ---- allowed: every lookup the index answers on its own

  @Test
  public void filterByLookup() {
    allowed("source=OTEL | where ATTRS.namespace = 'prod' | stats count()");
    allowed("source=OTEL | where 'prod' = ATTRS.namespace | fields SERVICE");
    allowed("source=OTEL | where ATTRS.namespace != 'prod' | fields SERVICE");
    allowed("source=OTEL | where isnotnull(ATTRS.error.type) | fields SERVICE");
    allowed("source=OTEL | where isnull(ATTRS.error.type) | fields SERVICE");
    allowed("source=OTEL | where ATTRS.namespace in ('prod', 'staging') | fields SERVICE");
    allowed(
        "source=OTEL | where ATTRS.namespace = 'prod' and SERVICE = 'checkout' or"
            + " isnotnull(ATTRS.k) | fields SERVICE");
  }

  // The index files a number and the text that spells it under one term, so a term query answers
  // the lookup whatever the literal looks like -- the answer the same DSL query gives.
  @Test
  public void filterByALiteralThatIsNotPlainText() {
    allowed("source=OTEL | where ATTRS.duration_ms = 4 | fields SERVICE");
    allowed("source=OTEL | where ATTRS.status_code = '503' | fields SERVICE");
    allowed("source=OTEL | where ATTRS.ok = 'true' | fields SERVICE");
    allowed("source=OTEL | where ATTRS.status_code in (500, 503) | fields SERVICE");
  }

  // Any pattern, not only a leading prefix: a prefix is a prefix of the term and anything else a
  // wildcard over it, both answered by the index alone.
  @Test
  public void filterByPattern() {
    allowed("source=OTEL | where like(ATTRS.namespace, 'ns-0%') | fields SERVICE");
    allowed("source=OTEL | where like(ATTRS.namespace, '%-prod') | fields SERVICE");
    allowed("source=OTEL | where like(ATTRS.namespace, '%prod%') | fields SERVICE");
    allowed("source=OTEL | where not like(ATTRS.namespace, 'ns-0%') | fields SERVICE");
  }

  // ---- rejected: everything that would open every record

  // typeof would answer "undefined" for every record: the leaf has no type until it is read
  @Test
  public void typeofALeaf() {
    rejected(
        "source=OTEL | eval t = typeof(ATTRS.duration_ms) | fields t",
        "Cannot apply typeof to a flat_object leaf");
  }

  // The one lookup the index would answer by comparing text: "200" > "90" is false, so a range
  // query over the folded terms answers a numeric comparison wrongly rather than not at all.
  @Test
  public void aComparison() {
    rejected(
        "source=OTEL | where ATTRS.duration_ms > 50 | fields SERVICE",
        "Cannot compare ATTRS.duration_ms with >");
    rejected(
        "source=OTEL | where ATTRS.duration_ms <= 50 | fields SERVICE",
        "Cannot compare ATTRS.duration_ms with <=");
  }

  @Test
  public void aggregateOverALeaf() {
    rejected(
        "source=OTEL | stats avg(ATTRS.duration_ms)", "Cannot apply avg to a flat_object leaf");
    rejected(
        "source=OTEL | stats count() by ATTRS.status_code", "Cannot group by ATTRS.status_code");
  }

  @Test
  public void sortByALeaf() {
    rejected(
        "source=OTEL | sort ATTRS.duration_ms | fields SERVICE",
        "Cannot sort by ATTRS.duration_ms");
  }

  @Test
  public void aLeafRenamedThenAggregatedIsStillALeaf() {
    rejected(
        "source=OTEL | eval s = ATTRS.status_code | stats count() by s",
        "Cannot group by ATTRS.status_code");
  }

  // expand takes a column of the index; a leaf is a value read out of one, and used to fail with
  // a ClassCastException from the cast to RexInputRef
  @Test
  public void expandALeaf() {
    Exception ex =
        assertThrows(
            Exception.class, () -> getRelNode("source=OTEL | expand ATTRS.tags | fields SERVICE"));
    assertTrue(
        String.valueOf(ex.getMessage()),
        String.valueOf(ex.getMessage()).contains("Cannot expand [ATTRS.tags]"));
  }

  @Test
  public void otherFieldsAreUnaffected() {
    allowed("source=OTEL | where SERVICE = 'x' | stats count() by SERVICE | sort SERVICE");
  }
}
