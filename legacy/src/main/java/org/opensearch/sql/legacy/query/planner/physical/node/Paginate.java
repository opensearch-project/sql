package org.opensearch.sql.legacy.query.planner.physical.node;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.maker.QueryMaker;
import org.opensearch.sql.legacy.query.planner.core.ExecuteParams;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.physical.Row;
import org.opensearch.sql.legacy.query.planner.physical.estimation.Cost;
import org.opensearch.sql.legacy.query.planner.resource.ResourceManager;

public abstract class Paginate extends BatchPhysicalOperator<SearchHit> {

  /** Request to submit to OpenSearch to scroll over */
  protected final TableInJoinRequestBuilder request;

  /** Page size to scroll over index */
  protected final int pageSize;

  /** Client connection to ElasticSearch */
  protected Client client;

  /** Currently undergoing scan */
  protected SearchResponse searchResponse;

  /** Time out */
  protected Integer timeout;

  /** Resource monitor manager */
  protected ResourceManager resourceMgr;

  public Paginate(TableInJoinRequestBuilder request, int pageSize) {
    this.request = request;
    this.pageSize = pageSize;
  }

  @Override
  public PlanNode[] children() {
    return new PlanNode[0];
  }

  @Override
  public Cost estimate() {
    return new Cost();
  }

  @Override
  public void open(ExecuteParams params) throws Exception {
    super.open(params);
    client = params.get(ExecuteParams.ExecuteParamType.CLIENT);
    timeout = params.get(ExecuteParams.ExecuteParamType.TIMEOUT);
    resourceMgr = params.get(ExecuteParams.ExecuteParamType.RESOURCE_MANAGER);

    Object filter = params.get(ExecuteParams.ExecuteParamType.EXTRA_QUERY_FILTER);
    if (filter instanceof BoolQueryBuilder) {
      request
          .getRequestBuilder()
          .setQuery(generateNewQueryWithExtraFilter((BoolQueryBuilder) filter));

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Received extra query filter, re-build query: {}",
            Strings.toString(
                XContentType.JSON, request.getRequestBuilder().request().source(), true, true));
      }
    }
  }

  @Override
  protected Collection<Row<SearchHit>> prefetch() {
    Objects.requireNonNull(client, "Client connection is not ready");
    Objects.requireNonNull(resourceMgr, "ResourceManager is not set");
    Objects.requireNonNull(timeout, "Time out is not set");

    if (searchResponse == null) {
      loadFirstBatch();
      updateMetaResult();
    } else {
      loadNextBatch();
    }
    return wrapRowForCurrentBatch();
  }

  protected abstract void loadFirstBatch();

  protected abstract void loadNextBatch();

  /**
   * Extra filter pushed down from upstream. Re-parse WHERE clause with extra filter because
   * OpenSearch RequestBuilder doesn't allow QueryBuilder inside be changed after added.
   */
  protected QueryBuilder generateNewQueryWithExtraFilter(BoolQueryBuilder filter)
      throws SqlParseException {
    Where where = request.getOriginalSelect().getWhere();
    BoolQueryBuilder newQuery;
    if (where != null) {
      newQuery = QueryMaker.explain(where, false);
      newQuery.must(filter);
    } else {
      newQuery = filter;
    }
    return newQuery;
  }

  protected void updateMetaResult() {
    resourceMgr.getMetaResult().addTotalNumOfShards(searchResponse.getTotalShards());
    resourceMgr.getMetaResult().addSuccessfulShards(searchResponse.getSuccessfulShards());
    resourceMgr.getMetaResult().addFailedShards(searchResponse.getFailedShards());
    resourceMgr.getMetaResult().updateTimeOut(searchResponse.isTimedOut());
  }

  @SuppressWarnings("unchecked")
  protected Collection<Row<SearchHit>> wrapRowForCurrentBatch() {
    SearchHit[] hits = searchResponse.getHits().getHits();
    Row[] rows = new Row[hits.length];
    for (int i = 0; i < hits.length; i++) {
      rows[i] = new SearchHitRow(hits[i], request.getAlias());
    }
    return Arrays.asList(rows);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [ " + describeTable() + ", pageSize=" + pageSize + " ]";
  }

  protected String describeTable() {
    return request.getOriginalSelect().getFrom().get(0).getIndex() + " as " + request.getAlias();
  }

  /*********************************************
   *          Getters for Explain
   *********************************************/

  public String getRequest() {
    return Strings.toString(XContentType.JSON, request.getRequestBuilder().request().source());
  }
}
