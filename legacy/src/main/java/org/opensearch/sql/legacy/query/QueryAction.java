/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query;

import com.fasterxml.jackson.core.JsonFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.json.JsonXContentParser;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.sql.legacy.domain.ColumnTypeProvider;
import org.opensearch.sql.legacy.domain.Query;
import org.opensearch.sql.legacy.domain.QueryStatement;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.domain.hints.HintType;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.Format;
import org.opensearch.sql.legacy.request.SqlRequest;

/**
 * Abstract class. used to transform Select object (Represents SQL query) to SearchRequestBuilder
 * (Represents OpenSearch query)
 */
public abstract class QueryAction {

  protected Query query;
  protected Client client;
  protected SqlRequest sqlRequest = SqlRequest.NULL;
  protected ColumnTypeProvider scriptColumnType;
  protected Format format;

  public QueryAction(Client client, Query query) {
    this.client = client;
    this.query = query;
  }

  public Client getClient() {
    return client;
  }

  public QueryStatement getQueryStatement() {
    return query;
  }

  public void setSqlRequest(SqlRequest sqlRequest) {
    this.sqlRequest = sqlRequest;
  }

  public void setColumnTypeProvider(ColumnTypeProvider scriptColumnType) {
    this.scriptColumnType = scriptColumnType;
  }

  public SqlRequest getSqlRequest() {
    return sqlRequest;
  }

  public void setFormat(Format format) {
    this.format = format;
  }

  public Format getFormat() {
    return this.format;
  }

  public ColumnTypeProvider getScriptColumnType() {
    return scriptColumnType;
  }

  /**
   * @return List of field names produced by the query
   */
  public Optional<List<String>> getFieldNames() {
    return Optional.empty();
  }

  protected void updateRequestWithCollapse(Select select, SearchRequestBuilder request)
      throws SqlParseException {
    JsonFactory jsonFactory = new JsonFactory();
    for (Hint hint : select.getHints()) {
      if (hint.getType() == HintType.COLLAPSE
          && hint.getParams() != null
          && 0 < hint.getParams().length) {
        try (JsonXContentParser parser =
            new JsonXContentParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                jsonFactory.createParser(hint.getParams()[0].toString()))) {
          request.setCollapse(CollapseBuilder.fromXContent(parser));
        } catch (IOException e) {
          throw new SqlParseException("could not parse collapse hint: " + e.getMessage());
        }
      }
    }
  }

  protected void updateRequestWithPostFilter(Select select, SearchRequestBuilder request) {
    for (Hint hint : select.getHints()) {
      if (hint.getType() == HintType.POST_FILTER
          && hint.getParams() != null
          && 0 < hint.getParams().length) {
        request.setPostFilter(QueryBuilders.wrapperQuery(hint.getParams()[0].toString()));
      }
    }
  }

  protected void updateRequestWithIndexAndRoutingOptions(
      Select select, SearchRequestBuilder request) {
    for (Hint hint : select.getHints()) {
      if (hint.getType() == HintType.IGNORE_UNAVAILABLE) {
        // saving the defaults from TransportClient search
        request.setIndicesOptions(
            IndicesOptions.fromOptions(
                true, false, true, false, IndicesOptions.strictExpandOpenAndForbidClosed()));
      }
      if (hint.getType() == HintType.ROUTINGS) {
        Object[] routings = hint.getParams();
        String[] routingsAsStringArray = new String[routings.length];
        for (int i = 0; i < routings.length; i++) {
          routingsAsStringArray[i] = routings[i].toString();
        }
        request.setRouting(routingsAsStringArray);
      }
    }
  }

  protected void updateRequestWithHighlight(Select select, SearchRequestBuilder request) {
    boolean foundAnyHighlights = false;
    HighlightBuilder highlightBuilder = new HighlightBuilder();
    for (Hint hint : select.getHints()) {
      if (hint.getType() == HintType.HIGHLIGHT) {
        HighlightBuilder.Field highlightField = parseHighlightField(hint.getParams());
        if (highlightField != null) {
          foundAnyHighlights = true;
          highlightBuilder.field(highlightField);
        }
      }
    }
    if (foundAnyHighlights) {
      request.highlighter(highlightBuilder);
    }
  }

  protected HighlightBuilder.Field parseHighlightField(Object[] params) {
    if (params == null || params.length == 0 || params.length > 2) {
      // todo: exception.
    }
    HighlightBuilder.Field field = new HighlightBuilder.Field(params[0].toString());
    if (params.length == 1) {
      return field;
    }
    Map<String, Object> highlightParams = (Map<String, Object>) params[1];

    for (Map.Entry<String, Object> param : highlightParams.entrySet()) {
      switch (param.getKey()) {
        case "type":
          field.highlighterType((String) param.getValue());
          break;
        case "boundary_chars":
          field.boundaryChars(fromArrayListToCharArray((ArrayList) param.getValue()));
          break;
        case "boundary_max_scan":
          field.boundaryMaxScan((Integer) param.getValue());
          break;
        case "force_source":
          field.forceSource((Boolean) param.getValue());
          break;
        case "fragmenter":
          field.fragmenter((String) param.getValue());
          break;
        case "fragment_offset":
          field.fragmentOffset((Integer) param.getValue());
          break;
        case "fragment_size":
          field.fragmentSize((Integer) param.getValue());
          break;
        case "highlight_filter":
          field.highlightFilter((Boolean) param.getValue());
          break;
        case "matched_fields":
          field.matchedFields((String[]) ((ArrayList) param.getValue()).toArray(new String[0]));
          break;
        case "no_match_size":
          field.noMatchSize((Integer) param.getValue());
          break;
        case "num_of_fragments":
          field.numOfFragments((Integer) param.getValue());
          break;
        case "order":
          field.order((String) param.getValue());
          break;
        case "phrase_limit":
          field.phraseLimit((Integer) param.getValue());
          break;
        case "post_tags":
          field.postTags((String[]) ((ArrayList) param.getValue()).toArray(new String[0]));
          break;
        case "pre_tags":
          field.preTags((String[]) ((ArrayList) param.getValue()).toArray(new String[0]));
          break;
        case "require_field_match":
          field.requireFieldMatch((Boolean) param.getValue());
          break;
      }
    }
    return field;
  }

  private char[] fromArrayListToCharArray(ArrayList arrayList) {
    char[] chars = new char[arrayList.size()];
    int i = 0;
    for (Object item : arrayList) {
      chars[i] = item.toString().charAt(0);
      i++;
    }
    return chars;
  }

  /**
   * Prepare the request, and return OpenSearch request.
   *
   * @return ActionRequestBuilder (OpenSearch request)
   * @throws SqlParseException
   */
  public abstract SqlElasticRequestBuilder explain() throws SqlParseException;
}
