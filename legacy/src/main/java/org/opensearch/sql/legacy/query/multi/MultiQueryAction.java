/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.multi;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.client.Client;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.pit.PointInTimeHandler;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.opensearch.common.util.ArrayUtils;

/** Created by Eliran on 19/8/2016. */
public class MultiQueryAction extends QueryAction {
  private MultiQuerySelect multiQuerySelect;
  private PointInTimeHandler pointInTimeHandler;

  public MultiQueryAction(Client client, MultiQuerySelect multiSelect) {
    super(client, null);
    this.multiQuerySelect = multiSelect;
  }

  @Override
  public SqlElasticRequestBuilder explain() throws SqlParseException {
    if (!isValidMultiSelectReturnFields()) {
      throw new SqlParseException(
          "on multi query fields/aliases of one table should be subset of other");
    }
    MultiQueryRequestBuilder requestBuilder = new MultiQueryRequestBuilder(this.multiQuerySelect);
    requestBuilder.setFirstSearchRequest(
        createRequestBuilder(this.multiQuerySelect.getFirstSelect()));
    requestBuilder.setSecondSearchRequest(
        createRequestBuilder(this.multiQuerySelect.getSecondSelect()));
    requestBuilder.fillTableAliases(
        this.multiQuerySelect.getFirstSelect().getFields(),
        this.multiQuerySelect.getSecondSelect().getFields());
    updateRequestWithPitId(requestBuilder);
    return requestBuilder;
  }

  public PointInTimeHandler getPointInTimeHandler() {
    return pointInTimeHandler;
  }

  private void updateRequestWithPitId(MultiQueryRequestBuilder requestBuilder) {
    String[] indices =
        ArrayUtils.concat(
            requestBuilder.getOriginalSelect(true).getIndexArr(),
            requestBuilder.getOriginalSelect(false).getIndexArr());
    this.pointInTimeHandler = new PointInTimeHandler(client, indices);
    String pitId = pointInTimeHandler.getPointInTimeId();
    requestBuilder.setPitId(pitId);
  }

  private boolean isValidMultiSelectReturnFields() {
    List<Field> firstQueryFields = multiQuerySelect.getFirstSelect().getFields();
    List<Field> secondQueryFields = multiQuerySelect.getSecondSelect().getFields();
    if (firstQueryFields.size() > secondQueryFields.size()) {
      return isSubsetFields(firstQueryFields, secondQueryFields);
    }
    return isSubsetFields(secondQueryFields, firstQueryFields);
  }

  private boolean isSubsetFields(List<Field> bigGroup, List<Field> smallerGroup) {
    Set<String> biggerGroup = new HashSet<>();
    for (Field field : bigGroup) {
      String fieldName = getNameOrAlias(field);
      biggerGroup.add(fieldName);
    }
    for (Field field : smallerGroup) {
      String fieldName = getNameOrAlias(field);
      if (!biggerGroup.contains(fieldName)) {
        return false;
      }
    }
    return true;
  }

  private String getNameOrAlias(Field field) {
    String fieldName = field.getName();
    if (field.getAlias() != null && !field.getAlias().isEmpty()) {
      fieldName = field.getAlias();
    }
    return fieldName;
  }

  protected SearchRequestBuilder createRequestBuilder(Select select) throws SqlParseException {
    DefaultQueryAction queryAction = new DefaultQueryAction(client, select);
    queryAction.explain();
    return queryAction.getRequestBuilder();
  }
}
