/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This file contains code from the Apache Calcite project (original license below).
 * It contains modifications, which are licensed as above:
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.sql.calcite.utils;

import static java.util.Objects.requireNonNull;
import static org.opensearch.sql.monitor.profile.MetricName.OPTIMIZE;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.CalciteJdbc41Factory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.Scannable;
import org.opensearch.sql.calcite.plan.rule.OpenSearchRules;
import org.opensearch.sql.calcite.plan.rule.PPLSimplifyDedupRule;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.monitor.profile.ProfileMetric;
import org.opensearch.sql.monitor.profile.QueryProfiling;

/**
 * Calcite Tools Helper. This class is used to create customized: 1. Connection 2. JavaTypeFactory
 * 3. RelBuilder 4. RelRunner 5. CalcitePreparingStmt. TODO delete it in future if possible.
 */
public class CalciteToolsHelper {
  /** Create a RelBuilder with testing */
  public static RelBuilder create(FrameworkConfig config) {
    return RelBuilder.create(config);
  }

  /** Create a RelBuilder with typeFactory */
  public static OpenSearchRelBuilder create(
      FrameworkConfig config, JavaTypeFactory typeFactory, Connection connection) {
    return withPrepare(
        config,
        typeFactory,
        connection,
        (cluster, relOptSchema, rootSchema, statement) ->
            new OpenSearchRelBuilder(config.getContext(), cluster, relOptSchema));
  }

  public static Connection connect(FrameworkConfig config, JavaTypeFactory typeFactory) {
    final Properties info = new Properties();
    if (config.getTypeSystem() != RelDataTypeSystem.DEFAULT) {
      info.setProperty(
          CalciteConnectionProperty.TYPE_SYSTEM.camelName(),
          config.getTypeSystem().getClass().getName());
    }
    try {
      return new OpenSearchDriver().connect("jdbc:calcite:", info, null, typeFactory);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static RelBuilderFactory proto(final Context context) {
    return (cluster, schema) -> new OpenSearchRelBuilder(context, cluster, schema);
  }

  /**
   * This method copied from {@link Frameworks#withPrepare(FrameworkConfig,
   * Frameworks.BasePrepareAction)}. The purpose is the method {@link
   * CalciteFactory#newConnection(UnregisteredDriver, AvaticaFactory, String, Properties)} create
   * connection with null instance of JavaTypeFactory. So we add a parameter JavaTypeFactory.
   */
  private static <R> R withPrepare(
      FrameworkConfig config,
      JavaTypeFactory typeFactory,
      Connection connection,
      Frameworks.BasePrepareAction<R> action) {
    try {
      final Properties info = new Properties();
      if (config.getTypeSystem() != RelDataTypeSystem.DEFAULT) {
        info.setProperty(
            CalciteConnectionProperty.TYPE_SYSTEM.camelName(),
            config.getTypeSystem().getClass().getName());
      }
      final CalciteServerStatement statement =
          connection.createStatement().unwrap(CalciteServerStatement.class);
      return new OpenSearchPrepareImpl().perform(statement, config, typeFactory, action);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static class OpenSearchDriver extends Driver {

    public Connection connect(
        String url, Properties info, CalciteSchema rootSchema, JavaTypeFactory typeFactory)
        throws SQLException {
      // Add current timestamp in nanos as hook
      Instant now = Instant.now();
      long nanosSinceEpoch = now.getEpochSecond() * 1_000_000_000L + now.getNano();
      Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(nanosSinceEpoch));
      CalciteJdbc41Factory factory = new CalciteJdbc41Factory();
      AvaticaConnection connection =
          factory.newConnection((Driver) this, factory, url, info, rootSchema, typeFactory);
      this.handler.onConnectionInit(connection);
      return connection;
    }

    @Override
    public CalcitePrepare createPrepare() {
      if (prepareFactory != null) {
        return prepareFactory.get();
      }
      return new OpenSearchPrepareImpl();
    }
  }

  /** do nothing, just extend for a public construct for new */
  public static class OpenSearchRelBuilder extends RelBuilder {
    public OpenSearchRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
      super(context, cluster, relOptSchema);
    }

    @Override
    public AggCall avg(boolean distinct, String alias, RexNode operand) {
      return aggregateCall(
          SqlParserPos.ZERO,
          PPLBuiltinOperators.AVG_NULLABLE,
          distinct,
          false,
          false,
          null,
          null,
          ImmutableList.of(),
          alias,
          ImmutableList.of(),
          ImmutableList.of(operand));
    }
  }

  public static class OpenSearchPrepareImpl extends CalcitePrepareImpl {
    /**
     * Similar to {@link CalcitePrepareImpl#perform(CalciteServerStatement, FrameworkConfig,
     * Frameworks.BasePrepareAction)}, but with a custom typeFactory.
     */
    public <R> R perform(
        CalciteServerStatement statement,
        FrameworkConfig config,
        JavaTypeFactory typeFactory,
        Frameworks.BasePrepareAction<R> action) {
      final CalcitePrepare.Context prepareContext = statement.createPrepareContext();
      SchemaPlus defaultSchema = config.getDefaultSchema();
      final CalciteSchema schema =
          defaultSchema != null
              ? CalciteSchema.from(defaultSchema)
              : prepareContext.getRootSchema();
      CalciteCatalogReader catalogReader =
          new CalciteCatalogReader(
              schema.root(), schema.path(null), typeFactory, prepareContext.config());
      final RexBuilder rexBuilder = new RexBuilder(typeFactory);
      final RelOptPlanner planner =
          createPlanner(
              prepareContext, Contexts.of(prepareContext.config()), config.getCostFactory());
      registerCustomizedRules(planner);
      final RelOptCluster cluster = createCluster(planner, rexBuilder);
      return action.apply(cluster, catalogReader, prepareContext.getRootSchema().plus(), statement);
    }

    private void registerCustomizedRules(RelOptPlanner planner) {
      OpenSearchRules.OPEN_SEARCH_OPT_RULES.forEach(planner::addRule);
    }

    /**
     * Customize CalcitePreparingStmt. Override {@link CalcitePrepareImpl#getPreparingStmt} and
     * return {@link OpenSearchCalcitePreparingStmt}
     */
    @Override
    protected CalcitePrepareImpl.CalcitePreparingStmt getPreparingStmt(
        CalcitePrepare.Context context,
        Type elementType,
        CalciteCatalogReader catalogReader,
        RelOptPlanner planner) {
      final JavaTypeFactory typeFactory = context.getTypeFactory();
      final EnumerableRel.Prefer prefer;
      if (elementType == Object[].class) {
        prefer = EnumerableRel.Prefer.ARRAY;
      } else {
        prefer = EnumerableRel.Prefer.CUSTOM;
      }
      final Convention resultConvention =
          enableBindable ? BindableConvention.INSTANCE : EnumerableConvention.INSTANCE;
      return new OpenSearchCalcitePreparingStmt(
          this,
          context,
          catalogReader,
          typeFactory,
          context.getRootSchema(),
          prefer,
          createCluster(planner, new RexBuilder(typeFactory)),
          resultConvention,
          createConvertletTable());
    }
  }

  /**
   * Similar to {@link CalcitePrepareImpl.CalcitePreparingStmt}. Customize the logic to convert an
   * EnumerableTableScan to BindableTableScan.
   */
  public static class OpenSearchCalcitePreparingStmt
      extends CalcitePrepareImpl.CalcitePreparingStmt {

    protected final RelOptCluster cluster;

    public OpenSearchCalcitePreparingStmt(
        CalcitePrepareImpl prepare,
        CalcitePrepare.Context context,
        CatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        CalciteSchema schema,
        EnumerableRel.Prefer prefer,
        RelOptCluster cluster,
        Convention resultConvention,
        SqlRexConvertletTable convertletTable) {
      super(
          prepare,
          context,
          catalogReader,
          typeFactory,
          schema,
          prefer,
          cluster,
          resultConvention,
          convertletTable);
      this.cluster = cluster;
    }

    @Override
    protected PreparedResult implement(RelRoot root) {
      if (root.rel instanceof Scannable scannable) {
        Hook.PLAN_BEFORE_IMPLEMENTATION.run(root);
        RelDataType resultType = root.rel.getRowType();
        boolean isDml = root.kind.belongsTo(SqlKind.DML);
        final Bindable bindable = dataContext -> scannable.scan();

        return new PreparedResultImpl(
            resultType,
            requireNonNull(parameterRowType, "parameterRowType"),
            requireNonNull(fieldOrigins, "fieldOrigins"),
            root.collation.getFieldCollations().isEmpty()
                ? ImmutableList.of()
                : ImmutableList.of(root.collation),
            root.rel,
            mapTableModOp(isDml, root.kind),
            isDml) {
          @Override
          public String getCode() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Bindable getBindable(Meta.CursorFactory cursorFactory) {
            return bindable;
          }

          @Override
          public Type getElementType() {
            return resultType.getFieldList().size() == 1 ? Object.class : Object[].class;
          }
        };
      }
      return super.implement(root);
    }

    @Override
    protected SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator, CatalogReader catalogReader, SqlToRelConverter.Config config) {
      return new OpenSearchSqlToRelConverter(
          this, validator, catalogReader, this.cluster, convertletTable, config);
    }
  }

  public static class OpenSearchSqlToRelConverter extends SqlToRelConverter {
    protected final RelBuilder relBuilder;

    public OpenSearchSqlToRelConverter(
        ViewExpander viewExpander,
        @Nullable SqlValidator validator,
        CatalogReader catalogReader,
        RelOptCluster cluster,
        SqlRexConvertletTable convertletTable,
        Config config) {
      super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
      this.relBuilder =
          config
              .getRelBuilderFactory()
              .create(
                  cluster,
                  validator != null
                      ? validator.getCatalogReader().unwrap(RelOptSchema.class)
                      : null)
              .transform(config.getRelBuilderConfigTransform());
    }

    @Override
    protected RelFieldTrimmer newFieldTrimmer() {
      return new OpenSearchRelFieldTrimmer(validator, this.relBuilder);
    }
  }

  public static class OpenSearchRelRunners {
    /**
     * Runs a relational expression by existing connection. This class copied from {@link
     * org.apache.calcite.tools.RelRunners#run(RelNode)}
     */
    public static PreparedStatement run(CalcitePlanContext context, RelNode rel) {
      ProfileMetric optimizeTime = QueryProfiling.current().getOrCreateMetric(OPTIMIZE);
      long startTime = System.nanoTime();
      // Optimize the plan by Calcite's HepPlanner before using VolcanoPlanner in prepareStatement.
      rel = CalciteToolsHelper.optimize(rel, context);
      final RelShuttle shuttle =
          new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(TableScan scan) {
              final RelOptTable table = scan.getTable();
              if (scan instanceof LogicalTableScan
                  && Bindables.BindableTableScan.canHandle(table)) {
                // Always replace the LogicalTableScan with BindableTableScan
                // because it's implementation does not require a "schema" as context.
                return Bindables.BindableTableScan.create(scan.getCluster(), table);
              }
              return super.visit(scan);
            }
          };
      rel = rel.accept(shuttle);
      // the line we changed here
      try (Connection connection = context.connection) {
        final RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement preparedStatement = runner.prepareStatement(rel);
        optimizeTime.set(System.nanoTime() - startTime);
        return preparedStatement;
      } catch (SQLException e) {
        // Detect if error is due to window functions in unsupported context (bins on time fields)
        String errorMsg = e.getMessage();
        if (errorMsg != null
            && errorMsg.contains("Error while preparing plan")
            && errorMsg.contains("WIDTH_BUCKET")) {
          throw new UnsupportedOperationException(
              "The 'bins' parameter on timestamp fields requires: (1) pushdown to be enabled"
                  + " (controlled by plugins.calcite.pushdown.enabled, enabled by default), and"
                  + " (2) the timestamp field to be used as an aggregation bucket (e.g., 'stats"
                  + " count() by @timestamp').");
        }
        throw Util.throwAsRuntime(e);
      }
    }
  }

  /** Try to optimize the plan by using HepPlanner */
  private static final List<RelOptRule> hepRuleList =
      List.of(FilterMergeRule.Config.DEFAULT.toRule(), PPLSimplifyDedupRule.DEDUP_SIMPLIFY_RULE);

  private static final HepProgram HEP_PROGRAM =
      new HepProgramBuilder().addRuleCollection(hepRuleList).build();

  public static RelNode optimize(RelNode plan, CalcitePlanContext context) {
    Util.discard(context);
    HepPlanner planner = new HepPlanner(HEP_PROGRAM);
    planner.setRoot(plan);
    return planner.findBestExp();
  }
}
