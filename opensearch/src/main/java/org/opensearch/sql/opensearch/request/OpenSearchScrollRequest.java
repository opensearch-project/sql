/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

/**
 * OpenSearch scroll search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation 2)
 * Maintain scroll ID between calls to client search method
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchScrollRequest implements OpenSearchRequest {

  /**
   * Search request used to initiate paged (scrolled) search. Not needed to get subsequent pages.
   */
  @EqualsAndHashCode.Exclude
  private final transient SearchRequest initialSearchRequest;
  /** Scroll context timeout. */
  private final TimeValue scrollTimeout;

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private final IndexName indexName;

  /**
   * Routing Ids used for the request
   * {@link OpenSearchRequest.IndexName}.
   */
  private final IndexName routingId;

  /** Index name. */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Scroll id which is set after first request issued. Because OpenSearchClient is shared by
   * multiple threads so this state has to be maintained here.
   */
  @Setter
  @Getter
  private String scrollId = NO_SCROLL_ID;

  public static final String NO_SCROLL_ID = "";

  @EqualsAndHashCode.Exclude
  private boolean needClean = true;

  @Getter
  private final List<String> includes;

  /** Constructor. */
  public OpenSearchScrollRequest(IndexName indexName,
                                 IndexName routingId,
                                 TimeValue scrollTimeout,
                                 SearchSourceBuilder sourceBuilder,
                                 OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.routingId = routingId;
    this.scrollTimeout = scrollTimeout;
    this.exprValueFactory = exprValueFactory;
    this.initialSearchRequest = new SearchRequest()
        .indices(indexName.getIndexNames())
        .scroll(scrollTimeout)
        .source(sourceBuilder);
    if (routingId != null) {
      this.initialSearchRequest.routing(routingId.getIndexNames());
    }

    includes = sourceBuilder.fetchSource() == null
        ? List.of()
        : Arrays.asList(sourceBuilder.fetchSource().includes());
  }


  /** Executes request using either {@param searchAction} or {@param scrollAction} as appropriate.
   */
  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse;
    if (isScroll()) {
      openSearchResponse = scrollAction.apply(scrollRequest());
    } else {
      if (initialSearchRequest == null) {
        // Probably a first page search (since there is no scroll set) called on a deserialized
        // `OpenSearchScrollRequest`, which has no `initialSearchRequest`.
        throw new UnsupportedOperationException("Misuse of OpenSearchScrollRequest");
      }
      openSearchResponse = searchAction.apply(initialSearchRequest);
    }

    var response = new OpenSearchResponse(openSearchResponse, exprValueFactory, includes);
    needClean = response.isEmpty();
    if (!needClean) {
      setScrollId(openSearchResponse.getScrollId());
    }
    return response;
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    try {
      // clean on the last page only, to prevent closing the scroll/cursor in the middle of paging.
      if (needClean && isScroll()) {
        cleanAction.accept(getScrollId());
        setScrollId(NO_SCROLL_ID);
      }
    } finally {
      reset();
    }
  }

  /**
   * Is scroll started which means pages after first is being requested.
   *
   * @return true if scroll started
   */
  public boolean isScroll() {
    return !scrollId.equals(NO_SCROLL_ID);
  }

  /**
   * Generate OpenSearch scroll request by scroll id maintained.
   *
   * @return scroll request
   */
  public SearchScrollRequest scrollRequest() {
    Objects.requireNonNull(scrollId, "Scroll id cannot be null");
    return new SearchScrollRequest().scroll(scrollTimeout).scrollId(scrollId);
  }

  /**
   * Reset internal state in case any stale data. However, ideally the same instance is not supposed
   * to be reused across different physical plan.
   */
  public void reset() {
    scrollId = NO_SCROLL_ID;
  }

  @Override
  public boolean hasAnotherBatch() {
    return !needClean && !scrollId.equals(NO_SCROLL_ID);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeTimeValue(scrollTimeout);
    out.writeString(scrollId);
    out.writeStringCollection(includes);
    indexName.writeTo(out);
    routingId.writeTo(out);
  }

  /**
   * Constructs OpenSearchScrollRequest from serialized representation.
   * @param in stream to read data from.
   * @param engine OpenSearchSqlEngine to get node-specific context.
   * @throws IOException thrown if reading from input {@code in} fails.
   */
  public OpenSearchScrollRequest(StreamInput in, OpenSearchStorageEngine engine)
      throws IOException {
    initialSearchRequest = null;
    scrollTimeout = in.readTimeValue();
    scrollId = in.readString();
    includes = in.readStringList();
    indexName = new IndexName(in);
    routingId = new IndexName(in);
    OpenSearchIndex index = (OpenSearchIndex) engine.getTable(
        null,
        indexName.toString(),
        routingId.toString());
    exprValueFactory = new OpenSearchExprValueFactory(index.getFieldOpenSearchTypes());
  }
}
