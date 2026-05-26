/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.TestOnly;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ShardDocSortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

/**
 * OpenSearch search request. This has to be stateful because it needs to:
 *
 * <p>1) Accumulate search source builder when visiting logical plan to push down operation. 2)
 * Indicate the search already done.
 */
@EqualsAndHashCode
@Getter
@ToString
public class OpenSearchQueryRequest implements OpenSearchRequest {
  private static final Logger LOG = LogManager.getLogger();

  /** {@link OpenSearchRequest.IndexName}. */
  private final IndexName indexName;

  /** Search request source builder. */
  private final SearchSourceBuilder sourceBuilder;

  /** OpenSearchExprValueFactory. */
  @EqualsAndHashCode.Exclude @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /** List of includes expected in the response. */
  @EqualsAndHashCode.Exclude @ToString.Exclude private final List<String> includes;

  @EqualsAndHashCode.Exclude @ToString.Exclude private boolean needClean = true;

  /** Indicate the search already done. */
  @EqualsAndHashCode.Exclude @ToString.Exclude private boolean searchDone = false;

  private String pitId;

  private final TimeValue cursorKeepAlive;

  private Object[] searchAfter;

  private SearchResponse searchResponse = null;

  @ToString.Exclude private Map<String, Object> afterKey;

  @TestOnly
  static OpenSearchQueryRequest of(
      String indexName, int size, OpenSearchExprValueFactory factory, List<String> includes) {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.from(0);
    sourceBuilder.size(size);
    sourceBuilder.timeout(DEFAULT_QUERY_TIMEOUT);
    return of(indexName, sourceBuilder, factory, includes);
  }

  @TestOnly
  static OpenSearchQueryRequest of(
      String indexName,
      SearchSourceBuilder sourceBuilder,
      OpenSearchExprValueFactory factory,
      List<String> includes) {
    return OpenSearchQueryRequest.of(new IndexName(indexName), sourceBuilder, factory, includes);
  }

  /** Build an OpenSearchQueryRequest without PIT support. */
  public static OpenSearchQueryRequest of(
      IndexName indexName,
      SearchSourceBuilder sourceBuilder,
      OpenSearchExprValueFactory factory,
      List<String> includes) {
    return new OpenSearchQueryRequest(indexName, sourceBuilder, factory, includes, null, null);
  }

  /** Build an OpenSearchQueryRequest with PIT support. */
  public static OpenSearchQueryRequest pitOf(
      IndexName indexName,
      SearchSourceBuilder sourceBuilder,
      OpenSearchExprValueFactory factory,
      List<String> includes,
      TimeValue cursorKeepAlive,
      String pitId) {
    return new OpenSearchQueryRequest(
        indexName, sourceBuilder, factory, includes, cursorKeepAlive, pitId);
  }

  /** Do not new it directly, use of() and pitOf() instead. */
  OpenSearchQueryRequest(
      IndexName indexName,
      SearchSourceBuilder sourceBuilder,
      OpenSearchExprValueFactory factory,
      List<String> includes,
      TimeValue cursorKeepAlive,
      String pitId) {
    this.indexName = indexName;
    this.sourceBuilder = sourceBuilder;
    this.exprValueFactory = factory;
    this.includes = includes;
    this.cursorKeepAlive = cursorKeepAlive;
    this.pitId = pitId;
  }

  /** true if the request is a count aggregation request. */
  public boolean isCountAggRequest() {
    return !searchDone
        && sourceBuilder.size() == 0
        && sourceBuilder.trackTotalHitsUpTo() != null // only set in v3
        && sourceBuilder.trackTotalHitsUpTo() == Integer.MAX_VALUE;
  }

  /**
   * Constructs OpenSearchQueryRequest from serialized representation.
   *
   * @param in stream to read data from.
   * @param engine OpenSearchSqlEngine to get node-specific context.
   * @throws IOException thrown if reading from input {@code in} fails.
   */
  public OpenSearchQueryRequest(StreamInput in, OpenSearchStorageEngine engine) throws IOException {
    // Deserialize the SearchSourceBuilder from the string representation
    String sourceBuilderString = in.readString();

    NamedXContentRegistry xContentRegistry =
        new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
    XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(xContentRegistry, IGNORE_DEPRECATIONS, sourceBuilderString);
    this.sourceBuilder = SearchSourceBuilder.fromXContent(parser);

    cursorKeepAlive = in.readTimeValue();
    pitId = in.readString();
    includes = in.readStringList();
    indexName = new IndexName(in);

    int length = in.readVInt();
    this.searchAfter = new Object[length];
    for (int i = 0; i < length; i++) {
      this.searchAfter[i] = in.readGenericValue();
    }

    OpenSearchIndex index = (OpenSearchIndex) engine.getTable(null, indexName.toString());
    exprValueFactory =
        new OpenSearchExprValueFactory(
            index.getFieldOpenSearchTypes(), index.isFieldTypeTolerance());
  }

  @Override
  public OpenSearchResponse search(
      Function<SearchRequest, SearchResponse> searchAction,
      Function<SearchScrollRequest, SearchResponse> scrollAction) {
    if (this.pitId == null) {
      return search(searchAction);
    } else {
      // Search with PIT instead of scroll API
      return searchWithPIT(searchAction);
    }
  }

  private OpenSearchResponse search(Function<SearchRequest, SearchResponse> searchAction) {
    OpenSearchResponse openSearchResponse;
    if (searchDone) {
      openSearchResponse =
          new OpenSearchResponse(
              SearchHits.empty(), exprValueFactory, includes, isCountAggRequest());
    } else {
      // Set afterKey to request, null for first round (afterKey is null in the beginning).
      if (this.sourceBuilder.aggregations() != null) {
        this.sourceBuilder.aggregations().getAggregatorFactories().stream()
            .filter(a -> a instanceof CompositeAggregationBuilder)
            .forEach(c -> ((CompositeAggregationBuilder) c).aggregateAfter(afterKey));
        if (LOG.isDebugEnabled()) {
          LOG.debug(sourceBuilder);
        }
      }

      SearchRequest searchRequest =
          new SearchRequest().indices(indexName.getIndexNames()).source(this.sourceBuilder);
      this.searchResponse = searchAction.apply(searchRequest);

      openSearchResponse =
          new OpenSearchResponse(
              this.searchResponse, exprValueFactory, includes, isCountAggRequest());

      // Update afterKey from response
      if (openSearchResponse.isAggregationResponse()) {
        openSearchResponse.getAggregations().asList().stream()
            .filter(a -> a instanceof InternalComposite)
            .forEach(c -> afterKey = ((InternalComposite) c).afterKey());
      }
      if (afterKey != null) {
        // For composite aggregation, searchDone is determined by response result.
        searchDone = openSearchResponse.isEmpty();
      } else {
        // Direct set searchDone to true for non-composite aggregation.
        searchDone = true;
      }
      needClean = searchDone;
    }
    return openSearchResponse;
  }

  public OpenSearchResponse searchWithPIT(Function<SearchRequest, SearchResponse> searchAction) {
    OpenSearchResponse openSearchResponse;
    if (searchDone) {
      openSearchResponse =
          new OpenSearchResponse(
              SearchHits.empty(), exprValueFactory, includes, isCountAggRequest());
    } else {
      this.sourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(this.pitId));
      this.sourceBuilder.timeout(cursorKeepAlive);
      // check for search after
      if (searchAfter != null) {
        this.sourceBuilder.searchAfter(searchAfter);
      }
      // Add sort tiebreaker for PIT search.
      // We cannot remove it since `_shard_doc` is not added implicitly in PIT now.
      // Ref https://github.com/opensearch-project/OpenSearch/pull/18924#issuecomment-3342365950
      if (this.sourceBuilder.sorts() == null || this.sourceBuilder.sorts().isEmpty()) {
        // If no sort field specified, sort by `_doc` + `_shard_doc`to get better performance
        this.sourceBuilder.sort(DOC_FIELD_NAME, ASC);
        this.sourceBuilder.sort(SortBuilders.shardDocSort());
      } else {
        // If sort fields specified, sort by `fields` + `_doc` + `_shard_doc`.
        if (this.sourceBuilder.sorts().stream()
            .noneMatch(
                b -> b instanceof FieldSortBuilder f && f.fieldName().equals(DOC_FIELD_NAME))) {
          this.sourceBuilder.sort(DOC_FIELD_NAME, ASC);
        }
        if (this.sourceBuilder.sorts().stream().noneMatch(ShardDocSortBuilder.class::isInstance)) {
          this.sourceBuilder.sort(SortBuilders.shardDocSort());
        }
      }
      SearchRequest searchRequest =
          new SearchRequest().indices(indexName.getIndexNames()).source(this.sourceBuilder);
      this.searchResponse = searchAction.apply(searchRequest);

      openSearchResponse =
          new OpenSearchResponse(
              this.searchResponse, exprValueFactory, includes, isCountAggRequest());

      needClean = openSearchResponse.isEmpty();
      searchDone = openSearchResponse.isEmpty();
      SearchHit[] searchHits = this.searchResponse.getHits().getHits();
      if (searchHits != null && searchHits.length > 0) {
        searchAfter = searchHits[searchHits.length - 1].getSortValues();
        this.sourceBuilder.searchAfter(searchAfter);
        if (LOG.isDebugEnabled()) {
          LOG.debug(sourceBuilder);
        }
      }
    }
    return openSearchResponse;
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    try {
      // clean on the last page only, to prevent deleting the PitId in the middle of paging.
      if (this.pitId != null && needClean) {
        cleanAction.accept(this.pitId);
        searchDone = true;
      }
    } finally {
      this.pitId = null;
      this.searchAfter = null;
      this.afterKey = null;
    }
  }

  @Override
  public void forceClean(Consumer<String> cleanAction) {
    try {
      if (this.pitId != null) {
        cleanAction.accept(this.pitId);
        searchDone = true;
      }
    } finally {
      this.pitId = null;
      this.searchAfter = null;
      this.afterKey = null;
    }
  }

  @Override
  public boolean hasAnotherBatch() {
    if (this.pitId != null) {
      return !needClean;
    }
    return false;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    if (this.pitId != null) {
      // Convert SearchSourceBuilder to XContent and write it as a string
      out.writeString(sourceBuilder.toString());

      out.writeTimeValue(sourceBuilder.timeout());
      out.writeString(sourceBuilder.pointInTimeBuilder().getId());
      out.writeStringCollection(includes);
      indexName.writeTo(out);

      // Serialize the searchAfter array
      if (searchAfter != null) {
        out.writeVInt(searchAfter.length);
        for (Object obj : searchAfter) {
          out.writeGenericValue(obj);
        }
      }
    } else {
      // OpenSearch Query request without PIT for single page requests
      throw new UnsupportedOperationException(
          "OpenSearchQueryRequest serialization is not implemented.");
    }
  }
}
