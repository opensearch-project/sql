/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

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
  private SearchRequest initialSearchRequest;
  /** Scroll context timeout. */
  private TimeValue scrollTimeout;

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private IndexName indexName;

  /** Index name. */
  @EqualsAndHashCode.Exclude
  @ToString.Exclude
  private OpenSearchExprValueFactory exprValueFactory;
  /**
   * Scroll id which is set after first request issued. Because ElasticsearchClient is shared by
   * multi-thread so this state has to be maintained here.
   */
  @Setter
  @Getter
  private String scrollId;

  private boolean needClean = false;

  private List<String> includes;

  /** Default constructor for Externalizable only.
   */
  public OpenSearchScrollRequest() {
  }

  /** Constructor. */
  public OpenSearchScrollRequest(IndexName indexName,
                                 TimeValue scrollTimeout,
                                 SearchSourceBuilder sourceBuilder,
                                 OpenSearchExprValueFactory exprValueFactory) {
    this.indexName = indexName;
    this.scrollTimeout = scrollTimeout;
    this.exprValueFactory = exprValueFactory;
    this.initialSearchRequest = new SearchRequest()
        .indices(indexName.getIndexNames())
        .scroll(scrollTimeout)
        .source(sourceBuilder);

    includes = sourceBuilder.fetchSource() != null && sourceBuilder.fetchSource().includes() != null
      ? Arrays.asList(sourceBuilder.fetchSource().includes())
      : List.of();
    }


  /** Constructor. */
  @Override
  public OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction,
                                   Function<SearchScrollRequest, SearchResponse> scrollAction) {
    SearchResponse openSearchResponse;
    if (isScroll()) {
      openSearchResponse = scrollAction.apply(scrollRequest());
    } else {
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
        setScrollId(null);
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
    return scrollId != null;
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
    scrollId = null;
  }

  /**
   * Convert a scroll request to string that can be included in a cursor.
   * @return a string representing the scroll request.
   */
  @Override
  public boolean hasAnotherBatch() {
    return scrollId != null && !scrollId.equals("");
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(initialSearchRequest);
    out.writeLong(scrollTimeout.getMillis());
    out.writeObject(indexName);
    out.writeObject(exprValueFactory);
    out.writeUTF(scrollId);
    out.writeBoolean(needClean);
    out.writeObject(includes);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    initialSearchRequest = (SearchRequest) in.readObject();
    scrollTimeout = TimeValue.timeValueMillis(in.readLong());
    indexName = (IndexName) in.readObject();
    exprValueFactory = (OpenSearchExprValueFactory) in.readObject();
    scrollId = in.readUTF();
    needClean = in.readBoolean();
    includes = (List<String>) in.readObject();
  }
}
