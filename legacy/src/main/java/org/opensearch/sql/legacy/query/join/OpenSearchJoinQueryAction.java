/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.join;

import static org.opensearch.sql.common.setting.Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER;

import java.util.List;
import org.opensearch.client.Client;
import org.opensearch.common.util.ArrayUtils;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.domain.JoinSelect;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.domain.TableOnJoinSelect;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.pit.PointInTimeHandler;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.opensearch.sql.legacy.query.planner.HashJoinQueryPlanRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.Config;

/** Created by Eliran on 15/9/2015. */
public abstract class OpenSearchJoinQueryAction extends QueryAction {

  protected JoinSelect joinSelect;
  private PointInTimeHandler pointInTimeHandler;

  public OpenSearchJoinQueryAction(Client client, JoinSelect joinSelect) {
    super(client, joinSelect);
    this.joinSelect = joinSelect;
  }

  @Override
  public SqlElasticRequestBuilder explain() throws SqlParseException {
    JoinRequestBuilder requestBuilder = createSpecificBuilder();
    fillBasicJoinRequestBuilder(requestBuilder);
    fillSpecificRequestBuilder(requestBuilder);
    if (LocalClusterState.state().getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER)) {
      updateRequestWithPitId(requestBuilder);
    }
    return requestBuilder;
  }

  public PointInTimeHandler getPointInTimeHandler() {
    return pointInTimeHandler;
  }

  protected abstract void fillSpecificRequestBuilder(JoinRequestBuilder requestBuilder)
      throws SqlParseException;

  protected abstract JoinRequestBuilder createSpecificBuilder();

  private void fillBasicJoinRequestBuilder(JoinRequestBuilder requestBuilder)
      throws SqlParseException {

    fillTableInJoinRequestBuilder(requestBuilder.getFirstTable(), joinSelect.getFirstTable());
    fillTableInJoinRequestBuilder(requestBuilder.getSecondTable(), joinSelect.getSecondTable());

    requestBuilder.setJoinType(joinSelect.getJoinType());

    requestBuilder.setTotalLimit(joinSelect.getTotalLimit());

    updateRequestWithHints(requestBuilder);
  }

  protected void updateRequestWithHints(JoinRequestBuilder requestBuilder) {
    for (Hint hint : joinSelect.getHints()) {
      Object[] params = hint.getParams();
      switch (hint.getType()) {
        case JOIN_LIMIT:
          requestBuilder.getFirstTable().setHintLimit((Integer) params[0]);
          requestBuilder.getSecondTable().setHintLimit((Integer) params[1]);
          break;
        case JOIN_ALGORITHM_BLOCK_SIZE:
          if (requestBuilder instanceof HashJoinQueryPlanRequestBuilder) {
            queryPlannerConfig(requestBuilder).configureBlockSize(hint.getParams());
          }
          break;
        case JOIN_SCROLL_PAGE_SIZE:
          if (requestBuilder instanceof HashJoinQueryPlanRequestBuilder) {
            queryPlannerConfig(requestBuilder).configureScrollPageSize(hint.getParams());
          }
          break;
        case JOIN_CIRCUIT_BREAK_LIMIT:
          if (requestBuilder instanceof HashJoinQueryPlanRequestBuilder) {
            queryPlannerConfig(requestBuilder).configureCircuitBreakLimit(hint.getParams());
          }
          break;
        case JOIN_BACK_OFF_RETRY_INTERVALS:
          if (requestBuilder instanceof HashJoinQueryPlanRequestBuilder) {
            queryPlannerConfig(requestBuilder).configureBackOffRetryIntervals(hint.getParams());
          }
          break;
        case JOIN_TIME_OUT:
          if (requestBuilder instanceof HashJoinQueryPlanRequestBuilder) {
            queryPlannerConfig(requestBuilder).configureTimeOut(hint.getParams());
          }
          break;
      }
    }
  }

  private void updateRequestWithPitId(JoinRequestBuilder requestBuilder) {
    String[] indices =
        ArrayUtils.concat(
            requestBuilder.getFirstTable().getOriginalSelect().getIndexArr(),
            requestBuilder.getSecondTable().getOriginalSelect().getIndexArr());
    this.pointInTimeHandler = new PointInTimeHandler(client, indices);
    String pitId = pointInTimeHandler.getPitId();
    requestBuilder.setPitId(pitId);
    requestBuilder.getFirstTable().setPitId(pitId);
    requestBuilder.getSecondTable().setPitId(pitId);
  }

  private Config queryPlannerConfig(JoinRequestBuilder requestBuilder) {
    return ((HashJoinQueryPlanRequestBuilder) requestBuilder).getConfig();
  }

  private void fillTableInJoinRequestBuilder(
      TableInJoinRequestBuilder requestBuilder, TableOnJoinSelect tableOnJoinSelect)
      throws SqlParseException {
    List<Field> connectedFields = tableOnJoinSelect.getConnectedFields();
    addFieldsToSelectIfMissing(tableOnJoinSelect, connectedFields);
    requestBuilder.setOriginalSelect(tableOnJoinSelect);
    DefaultQueryAction queryAction = new DefaultQueryAction(client, tableOnJoinSelect);
    queryAction.explain();
    requestBuilder.setRequestBuilder(queryAction.getRequestBuilder());
    requestBuilder.setReturnedFields(tableOnJoinSelect.getSelectedFields());
    requestBuilder.setAlias(tableOnJoinSelect.getAlias());
  }

  private void addFieldsToSelectIfMissing(Select select, List<Field> fields) {
    // this means all fields
    if (select.getFields() == null || select.getFields().size() == 0) {
      return;
    }

    List<Field> selectedFields = select.getFields();
    for (Field field : fields) {
      if (!selectedFields.contains(field)) {
        selectedFields.add(field);
      }
    }
  }
}
