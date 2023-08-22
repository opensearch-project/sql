/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.converter;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.util.List;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.sql.legacy.domain.ColumnTypeProvider;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.parser.SqlParser;
import org.opensearch.sql.legacy.query.AggregationQueryAction;
import org.opensearch.sql.legacy.query.planner.core.ColumnNode;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.node.project.PhysicalProject;
import org.opensearch.sql.legacy.query.planner.physical.node.scroll.PhysicalScroll;

/** Definition of SQL to PhysicalOperator converter. */
public class SQLToOperatorConverter extends MySqlASTVisitorAdapter {
  private static final Logger LOG = LogManager.getLogger(SQLToOperatorConverter.class);

  private final Client client;
  private final SQLAggregationParser aggregationParser;

  @Getter private PhysicalOperator<BindingTuple> physicalOperator;

  public SQLToOperatorConverter(Client client, ColumnTypeProvider columnTypeProvider) {
    this.client = client;
    this.aggregationParser = new SQLAggregationParser(columnTypeProvider);
  }

  @Override
  public boolean visit(MySqlSelectQueryBlock query) {

    // 1. parse the aggregation
    aggregationParser.parse(query);

    // 2. construct the PhysicalOperator
    physicalOperator = project(scroll(query));
    return false;
  }

  /**
   * Get list of {@link ColumnNode}.
   *
   * @return list of {@link ColumnNode}.
   */
  public List<ColumnNode> getColumnNodes() {
    return aggregationParser.getColumnNodes();
  }

  private PhysicalOperator<BindingTuple> project(PhysicalOperator<BindingTuple> input) {
    return new PhysicalProject(input, aggregationParser.getColumnNodes());
  }

  @SneakyThrows
  private PhysicalOperator<BindingTuple> scroll(MySqlSelectQueryBlock query) {
    query.getSelectList().clear();
    query.getSelectList().addAll(aggregationParser.selectItemList());
    Select select = new SqlParser().parseSelect(query);
    return new PhysicalScroll(new AggregationQueryAction(client, select));
  }
}
