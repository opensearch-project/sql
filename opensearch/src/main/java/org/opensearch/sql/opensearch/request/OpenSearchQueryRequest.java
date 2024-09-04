/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.action.search.*;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

import static org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;

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

  /** {@link OpenSearchRequest.IndexName}. */
  private final IndexName indexName;

  /** Search request source builder. */
  private SearchSourceBuilder sourceBuilder;

  /** OpenSearchExprValueFactory. */
  @EqualsAndHashCode.Exclude @ToString.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /** List of includes expected in the response. */
  @EqualsAndHashCode.Exclude @ToString.Exclude private final List<String> includes;

  @EqualsAndHashCode.Exclude private boolean needClean = true;

  /** Indicate the search already done. */
  private boolean searchDone = false;

  private String pitId;

  private OpenSearchClient client;

  private TimeValue cursorKeepAlive;

  private Object[] searchAfter;

  private SearchResponse searchResponse = null;

  /** Constructor of OpenSearchQueryRequest. */
  public OpenSearchQueryRequest(
      String indexName, int size, OpenSearchExprValueFactory factory, List<String> includes) {
    this(new IndexName(indexName), size, factory, includes);
  }

  /** Constructor of OpenSearchQueryRequest. */
  public OpenSearchQueryRequest(
      IndexName indexName, int size, OpenSearchExprValueFactory factory, List<String> includes) {
    this.indexName = indexName;
    this.sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.from(0);
    sourceBuilder.size(size);
    sourceBuilder.timeout(DEFAULT_QUERY_TIMEOUT);
    this.exprValueFactory = factory;
    this.includes = includes;
  }

  /** Constructor of OpenSearchQueryRequest. */
  public OpenSearchQueryRequest(
      IndexName indexName,
      SearchSourceBuilder sourceBuilder,
      OpenSearchExprValueFactory factory,
      List<String> includes) {
    this.indexName = indexName;
    this.sourceBuilder = sourceBuilder;
    this.exprValueFactory = factory;
    this.includes = includes;
  }

  /** Constructor of OpenSearchQueryRequest with PIT support. */
  public OpenSearchQueryRequest(
          IndexName indexName,
          SearchSourceBuilder sourceBuilder,
          OpenSearchExprValueFactory factory,
          List<String> includes,
          TimeValue cursorKeepAlive,
          OpenSearchClient client) {
    this.indexName = indexName;
    this.sourceBuilder = sourceBuilder;
    this.exprValueFactory = factory;
    this.includes = includes;
    this.cursorKeepAlive = cursorKeepAlive;
    this.client = client;
  }

  @Override
  public OpenSearchResponse search(
      Function<SearchRequest, SearchResponse> searchAction,
      Function<SearchScrollRequest, SearchResponse> scrollAction) {
    OpenSearchResponse openSearchResponse;
    if (searchDone) {
      openSearchResponse = new OpenSearchResponse(SearchHits.empty(), exprValueFactory, includes);
    } else {
      searchDone = true;

      // Create PIT and Set PIT
      if (this.pitId ==  null) {
        this.pitId = createPIT();
      }
      this.sourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(this.pitId));
      this.sourceBuilder.timeout(cursorKeepAlive);

      // check for search after
      if (searchAfter != null) {
        this.sourceBuilder.searchAfter(searchAfter); // Set search_after in the query
      }

      // Set sort field for search_after
      if (this.sourceBuilder.sorts() == null) {
        this.sourceBuilder.sort(DOC_FIELD_NAME, ASC);
      }
      SearchRequest searchRequest = new SearchRequest().indices(indexName.getIndexNames()).source(this.sourceBuilder);
      this.searchResponse = searchAction.apply(searchRequest);

      openSearchResponse = new OpenSearchResponse(
              this.searchResponse,
              exprValueFactory,
              includes);

      needClean = openSearchResponse.isEmpty();
      if (!needClean && this.searchResponse.getHits().getHits() != null) {
        searchAfter = this.searchResponse.getHits().getHits()[this.searchResponse.getHits().getHits().length - 1].getSortValues();
        this.sourceBuilder.searchAfter(searchAfter);

        SearchResponse nextBatchResponse = searchAction.apply(new SearchRequest().indices(indexName.getIndexNames()).source(this.sourceBuilder));
        needClean = nextBatchResponse.getHits().getHits().length == 0;

      }
    }
    return openSearchResponse;
  }

  public String createPIT() {

    CreatePitRequest createPitRequest =
            new CreatePitRequest(this.cursorKeepAlive, false, indexName.getIndexNames());
    ActionFuture<CreatePitResponse> execute =
            this.client.getNodeClient().execute(CreatePitAction.INSTANCE, createPitRequest);
    try {
      CreatePitResponse pitResponse = execute.get();
      return pitResponse.getId();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error occurred while creating PIT for new engine SQL query", e);
    }
  }

  public void deletePit() {
    DeletePitRequest deletePitRequest = new DeletePitRequest(this.pitId);
    ActionFuture<DeletePitResponse> execute =
            this.client.getNodeClient().execute(DeletePitAction.INSTANCE, deletePitRequest);
    try {
      DeletePitResponse deletePitResponse = execute.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Error occurred while deleting PIT.", e);
    }
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    if (needClean) {
      deletePit();
    }
  }

  @Override
  public boolean hasAnotherBatch() {
    return !needClean;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    // Convert SearchSourceBuilder to XContent and write it as a string
    out.writeString(sourceBuilder.toString());

    out.writeTimeValue(sourceBuilder.timeout());
    out.writeString(sourceBuilder.pointInTimeBuilder().getId());
    out.writeStringCollection(includes);
    indexName.writeTo(out);

    // Serialize the searchAfter array
    out.writeVInt(searchAfter.length);
    for (Object obj : searchAfter) {
      out.writeGenericValue(obj);
    }
  }

  /**
   * Constructs OpenSearchQueryRequest from serialized representation.
   *
   * @param in stream to read data from.
   * @param engine OpenSearchSqlEngine to get node-specific context.
   * @throws IOException thrown if reading from input {@code in} fails.
   */
  public OpenSearchQueryRequest(StreamInput in, OpenSearchStorageEngine engine)
          throws IOException {
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

    this.client = engine.getClient();

    int length = in.readVInt();
    // Read each element of the searchAfter array
    this.searchAfter = new Object[length];
    for (int i = 0; i < length; i++) {
      this.searchAfter[i] = in.readGenericValue();
    }

    OpenSearchIndex index = (OpenSearchIndex) engine.getTable(null, indexName.toString());
    exprValueFactory = new OpenSearchExprValueFactory(index.getFieldOpenSearchTypes());
  }
}
