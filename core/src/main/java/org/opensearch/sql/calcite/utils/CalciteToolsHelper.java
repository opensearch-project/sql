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

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteFactory;
import org.apache.calcite.jdbc.CalciteJdbc41Factory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.util.Util;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.udf.udaf.NullableSqlAvgAggFunction;
import org.opensearch.sql.calcite.udf.udaf.NullableSqlSumAggFunction;

/**
 * Calcite Tools Helper. This class is used to create customized: 1. Connection 2. JavaTypeFactory
 * 3. RelBuilder 4. RelRunner TODO delete it in future if possible.
 */
public class CalciteToolsHelper {

  /** Create a RelBuilder with testing */
  public static RelBuilder create(FrameworkConfig config) {
    return RelBuilder.create(config);
  }

  /** Create a RelBuilder with typeFactory */
  public static RelBuilder create(
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
      CalciteJdbc41Factory factory = new CalciteJdbc41Factory();
      AvaticaConnection connection =
          factory.newConnection((Driver) this, factory, url, info, rootSchema, typeFactory);
      this.handler.onConnectionInit(connection);
      return connection;
    }
  }

  /** do nothing, just extend for a public construct for new */
  public static class OpenSearchRelBuilder extends RelBuilder {
    public OpenSearchRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
      super(context, cluster, relOptSchema);
    }

    @Override
    public AggCall sum(boolean distinct, String alias, RexNode operand) {
      return aggregateCall(
          SUM_NULLABLE,
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

    @Override
    public AggCall avg(boolean distinct, String alias, RexNode operand) {
      return aggregateCall(
          SqlParserPos.ZERO,
          AVG_NULLABLE,
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

  public static final SqlAggFunction SUM_NULLABLE =
      new NullableSqlSumAggFunction(castNonNull(null));
  public static final SqlAggFunction AVG_NULLABLE = new NullableSqlAvgAggFunction(SqlKind.AVG);
  public static final SqlAggFunction STDDEV_POP_NULLABLE =
      new NullableSqlAvgAggFunction(SqlKind.STDDEV_POP);
  public static final SqlAggFunction STDDEV_SAMP_NULLABLE =
      new NullableSqlAvgAggFunction(SqlKind.STDDEV_SAMP);
  public static final SqlAggFunction VAR_POP_NULLABLE =
      new NullableSqlAvgAggFunction(SqlKind.VAR_POP);
  public static final SqlAggFunction VAR_SAMP_NULLABLE =
      new NullableSqlAvgAggFunction(SqlKind.VAR_SAMP);

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
          createPlanner(prepareContext, Contexts.of(prepareContext.config()), config.getCostFactory());
      final RelOptCluster cluster = createCluster(planner, rexBuilder);
      return action.apply(cluster, catalogReader, prepareContext.getRootSchema().plus(), statement);
    }
  }

  public static class OpenSearchRelRunners {
    /**
     * Runs a relational expression by existing connection. This class copied from {@link
     * org.apache.calcite.tools.RelRunners#run(RelNode)}
     */
    public static PreparedStatement run(CalcitePlanContext context, RelNode rel) {
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
        return runner.prepareStatement(rel);
      } catch (SQLException e) {
        throw Util.throwAsRuntime(e);
      }
    }
  }
}
