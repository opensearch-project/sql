/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.mockito.Mockito.doReturn;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
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
import org.junit.Before;
import org.opensearch.sql.common.setting.Settings;

/**
 * Abstract base class for PPL tests that require permissive mode with dynamic fields support.
 * Provides a custom table with _MAP field to simulate dynamic/unmapped fields.
 */
public abstract class CalcitePPLPermissiveAbstractTest extends CalcitePPLAbstractTest {

  /**
   * Custom table with _MAP field to simulate dynamic fields in permissive mode. Schema: id
   * (INTEGER), name (VARCHAR), _MAP (MAP<VARCHAR, ANY>)
   */
  public static class TableWithDynamicFields implements Table {
    protected final RelProtoDataType protoRowType =
        factory ->
            factory
                .builder()
                .add("id", SqlTypeName.INTEGER)
                .add("name", SqlTypeName.VARCHAR)
                .add(
                    "_MAP",
                    factory.createMapType(
                        factory.createSqlType(SqlTypeName.VARCHAR),
                        factory.createSqlType(SqlTypeName.ANY)))
                .build();

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.of(100d, ImmutableList.of(), RelCollations.createSingleton(0));
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

  public CalcitePPLPermissiveAbstractTest(CalciteAssert.SchemaSpec... schemaSpecs) {
    super(schemaSpecs);
  }

  @Before
  public void init() {
    super.init();
    doReturn(true).when(settings).getSettingValue(Settings.Key.PPL_QUERY_PERMISSIVE);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add("test_dynamic", new TableWithDynamicFields());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }
}
