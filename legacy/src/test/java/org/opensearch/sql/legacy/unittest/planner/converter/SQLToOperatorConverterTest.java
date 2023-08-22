/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.planner.converter;

import static org.junit.Assert.assertTrue;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.util.JdbcConstants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.opensearch.client.Client;
import org.opensearch.sql.legacy.domain.ColumnTypeProvider;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.query.planner.converter.SQLToOperatorConverter;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.node.project.PhysicalProject;

@RunWith(MockitoJUnitRunner.class)
public class SQLToOperatorConverterTest {
  @Mock private Client client;

  private SQLToOperatorConverter converter;

  @Before
  public void setup() {
    converter = new SQLToOperatorConverter(client, new ColumnTypeProvider());
  }

  @Test
  public void convertAggShouldPass() {
    String sql =
        "SELECT dayOfWeek, max(FlightDelayMin), MIN(FlightDelayMin) as min "
            + "FROM opensearch_dashboards_sample_data_flights "
            + "GROUP BY dayOfWeek";
    toExpr(sql).accept(converter);
    PhysicalOperator<BindingTuple> physicalOperator = converter.getPhysicalOperator();

    assertTrue(physicalOperator instanceof PhysicalProject);
  }

  @Test
  public void convertMaxMinusMinShouldPass() {
    String sql =
        "SELECT dayOfWeek, max(FlightDelayMin) - MIN(FlightDelayMin) as diff "
            + "FROM opensearch_dashboards_sample_data_flights "
            + "GROUP BY dayOfWeek";
    toExpr(sql).accept(converter);
    PhysicalOperator<BindingTuple> physicalOperator = converter.getPhysicalOperator();

    assertTrue(physicalOperator instanceof PhysicalProject);
  }

  @Test
  public void convertDistinctPass() {
    String sql =
        "SELECT dayOfWeek, max(FlightDelayMin) - MIN(FlightDelayMin) as diff "
            + "FROM opensearch_dashboards_sample_data_flights "
            + "GROUP BY dayOfWeek";
    toExpr(sql).accept(converter);
    PhysicalOperator<BindingTuple> physicalOperator = converter.getPhysicalOperator();

    assertTrue(physicalOperator instanceof PhysicalProject);
  }

  private SQLQueryExpr toExpr(String sql) {
    String dbType = JdbcConstants.MYSQL;
    return (SQLQueryExpr) SQLUtils.toSQLExpr(sql, dbType);
  }
}
