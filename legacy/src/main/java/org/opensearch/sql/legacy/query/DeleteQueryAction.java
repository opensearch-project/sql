/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query;

import org.opensearch.client.Client;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.DeleteByQueryRequestBuilder;
import org.opensearch.sql.legacy.domain.Delete;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.maker.QueryMaker;

public class DeleteQueryAction extends QueryAction {

  private final Delete delete;
  private DeleteByQueryRequestBuilder request;

  public DeleteQueryAction(Client client, Delete delete) {
    super(client, delete);
    this.delete = delete;
  }

  @Override
  public SqlElasticDeleteByQueryRequestBuilder explain() throws SqlParseException {
    this.request = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE);

    setIndicesAndTypes();
    setWhere(delete.getWhere());
    SqlElasticDeleteByQueryRequestBuilder deleteByQueryRequestBuilder =
        new SqlElasticDeleteByQueryRequestBuilder(request);
    return deleteByQueryRequestBuilder;
  }

  /** Set indices and types to the delete by query request. */
  private void setIndicesAndTypes() {

    DeleteByQueryRequest innerRequest = request.request();
    innerRequest.indices(query.getIndexArr());
  }

  /**
   * Create filters based on the Where clause.
   *
   * @param where the 'WHERE' part of the SQL query.
   * @throws SqlParseException
   */
  private void setWhere(Where where) throws SqlParseException {
    if (where != null) {
      QueryBuilder whereQuery = QueryMaker.explain(where);
      request.filter(whereQuery);
    } else {
      request.filter(QueryBuilders.matchAllQuery());
    }
  }
}
