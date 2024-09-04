package org.opensearch.sql.legacy.query.planner.physical.node.pointInTime;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.pit.PointInTimeHandlerImpl;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.maker.QueryMaker;
import org.opensearch.sql.legacy.query.planner.core.ExecuteParams;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.physical.Row;
import org.opensearch.sql.legacy.query.planner.physical.estimation.Cost;
import org.opensearch.sql.legacy.query.planner.physical.node.BatchPhysicalOperator;
import org.opensearch.sql.legacy.query.planner.resource.ResourceManager;

/** OpenSearch Search API with Point in time as physical implementation of TableScan */
public class PointInTime extends BatchPhysicalOperator<SearchHit> {

    /** Request to submit to OpenSearch to scroll over */
    private final TableInJoinRequestBuilder request;

    /** Page size to scroll over index */
    private final int pageSize;

    /** Client connection to ElasticSearch */
    private Client client;

    /** Currently undergoing search request */
    private SearchResponse searchResponse;

    /** Time out */
    private Integer timeout;

    private String pitId;

    private PointInTimeHandlerImpl pit;

    /** Resource monitor manager */
    private ResourceManager resourceMgr;

    public PointInTime(TableInJoinRequestBuilder request, int pageSize) {
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
    public void close() {
        if (searchResponse != null) {
            LOG.debug("Closing Point In Time (PIT) context");

            // Delete the Point In Time context
            pit.delete();

            searchResponse = null;
        } else {
            LOG.debug("PIT context is already closed or was never opened");
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
            loadNextBatchByPitId();
        }
        return wrapRowForCurrentBatch();
    }

    /**
     * Extra filter pushed down from upstream. Re-parse WHERE clause with extra filter because
     * OpenSearch RequestBuilder doesn't allow QueryBuilder inside be changed after added.
     */
    private QueryBuilder generateNewQueryWithExtraFilter(BoolQueryBuilder filter)
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

    private void loadFirstBatch() {
        // Create PIT and set to request object
        pit = new PointInTimeHandlerImpl(client, request.getOriginalSelect().getIndexArr());
        pit.create();
        pitId = pit.getPitId();
        searchResponse =
                request
                        .getRequestBuilder()
                        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                        .setSize(pageSize)
                        .setTimeout(TimeValue.timeValueSeconds(timeout))
                        .setPointInTime(new PointInTimeBuilder(pitId))
                        .get();
        LOG.info("Loading first batch of response using Point In Time");
    }

    private void updateMetaResult() {
        resourceMgr.getMetaResult().addTotalNumOfShards(searchResponse.getTotalShards());
        resourceMgr.getMetaResult().addSuccessfulShards(searchResponse.getSuccessfulShards());
        resourceMgr.getMetaResult().addFailedShards(searchResponse.getFailedShards());
        resourceMgr.getMetaResult().updateTimeOut(searchResponse.isTimedOut());
    }

    private void loadNextBatchByPitId() {
        // Add PIT with search after to fetch next batch of data
        if (searchResponse.getHits().getHits() !=null && searchResponse.getHits().getHits().length > 0) {
            Object[] sortValues = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1].getSortValues();
            LOG.info("Loading next batch of response using Point In Time. - " + pitId);
            searchResponse =
                    request
                            .getRequestBuilder()
                            .setSize(pageSize)
                            .setTimeout(TimeValue.timeValueSeconds(timeout))
                            .setPointInTime(new PointInTimeBuilder(pitId))
                            .searchAfter(sortValues)
                            .get();
        }

    }

    @SuppressWarnings("unchecked")
    private Collection<Row<SearchHit>> wrapRowForCurrentBatch() {
        SearchHit[] hits = searchResponse.getHits().getHits();
        Row[] rows = new Row[hits.length];
        for (int i = 0; i < hits.length; i++) {
            rows[i] = new SearchHitRow(hits[i], request.getAlias());
        }
        return Arrays.asList(rows);
    }

    @Override
    public String toString() {
        return "PointInTime [ " + describeTable() + ", pageSize=" + pageSize + " ]";
    }

    private String describeTable() {
        return request.getOriginalSelect().getFrom().get(0).getIndex() + " as " + request.getAlias();
    }

    /*********************************************
     *          Getters for Explain
     *********************************************/

    public String getRequest() {
        return Strings.toString(XContentType.JSON, request.getRequestBuilder().request().source());
    }
}
