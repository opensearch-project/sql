/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.join;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.domain.hints.HintType;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.ElasticHitsExecutor;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.pit.PointInTimeHandlerImpl;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.opensearch.sql.legacy.query.join.HashJoinElasticRequestBuilder;
import org.opensearch.sql.legacy.query.join.JoinRequestBuilder;
import org.opensearch.sql.legacy.query.join.NestedLoopsElasticRequestBuilder;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.HashJoinQueryPlanRequestBuilder;
import org.opensearch.transport.client.Client;

/** Created by Eliran on 15/9/2015. */
public abstract class ElasticJoinExecutor extends ElasticHitsExecutor {

  protected List<SearchHit> results; // Keep list to avoid copy to new array in SearchHits
  protected final MetaSearchResult metaResults;
  protected final int MAX_RESULTS_ON_ONE_FETCH = 10000;
  private final Set<String> aliasesOnReturn;
  private final boolean allFieldsReturn;
  protected final String[] indices;
  protected final JoinRequestBuilder requestBuilder; // Added to store request builder

  protected ElasticJoinExecutor(Client client, JoinRequestBuilder requestBuilder) {
    metaResults = new MetaSearchResult();
    aliasesOnReturn = new HashSet<>();
    List<Field> firstTableReturnedField = requestBuilder.getFirstTable().getReturnedFields();
    List<Field> secondTableReturnedField = requestBuilder.getSecondTable().getReturnedFields();
    allFieldsReturn =
        (firstTableReturnedField == null || firstTableReturnedField.size() == 0)
            && (secondTableReturnedField == null || secondTableReturnedField.size() == 0);
    indices = getIndices(requestBuilder);
    this.client = client;
    this.requestBuilder = requestBuilder; // Store request builder for hint access
  }

  public void sendResponse(RestChannel channel) throws IOException {
    XContentBuilder builder = null;
    long len;
    try {
      builder = ElasticUtils.hitsAsStringResultZeroCopy(results, metaResults, this);
      BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, builder);
      len = bytesRestResponse.content().length();
      channel.sendResponse(bytesRestResponse);
    } catch (IOException e) {
      try {
        if (builder != null) {
          builder.close();
        }
      } catch (Exception ex) {
        // Ignore. Already logged in channel
      }
      throw e;
    }
    LOG.debug(
        "[MCB] Successfully send response with size of {}. Thread id = {}",
        len,
        Thread.currentThread().getId());
  }

  public void run() throws IOException, SqlParseException {
    try {
      long timeBefore = System.currentTimeMillis();

      LOG.info("🔍 Starting join execution, checking for JOIN_TIME_OUT hints...");

      // ✅ Extract JOIN_TIME_OUT hint and create PIT with custom keepalive
      TimeValue customKeepAlive = extractJoinTimeoutFromHints();

      if (customKeepAlive != null) {
        LOG.info(
            "✅ Using custom PIT keepalive from JOIN_TIME_OUT hint: {} seconds ({}ms)",
            customKeepAlive.getSeconds(),
            customKeepAlive.getMillis());
        pit = new PointInTimeHandlerImpl(client, indices, customKeepAlive);
      } else {
        LOG.info("⚠️  No JOIN_TIME_OUT hint found, using default PIT keepalive");
        pit = new PointInTimeHandlerImpl(client, indices);
      }

      pit.create();
      results = innerRun();
      long joinTimeInMilli = System.currentTimeMillis() - timeBefore;
      this.metaResults.setTookImMilli(joinTimeInMilli);
    } catch (Exception e) {
      LOG.error("Failed during join query run.", e);
      throw new IllegalStateException("Error occurred during join query run", e);
    } finally {
      try {
        pit.delete();
      } catch (RuntimeException e) {
        Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
        LOG.info("Error deleting point in time {} ", pit);
      }
    }
  }

  /**
   * Extract JOIN_TIME_OUT hint value from the request builder
   *
   * @return TimeValue for custom keepalive, or null if no hint found
   */
  protected TimeValue extractJoinTimeoutFromHints() {
    try {
      LOG.info("🔍 DEBUG: Starting hint extraction");
      LOG.info(
          "🔍 DEBUG: requestBuilder = {}",
          requestBuilder != null ? requestBuilder.getClass().getSimpleName() : "null");

      // Debug first table
      TableInJoinRequestBuilder firstTable = requestBuilder.getFirstTable();
      LOG.info("🔍 DEBUG: firstTable = {}", firstTable != null ? "exists" : "null");

      if (firstTable != null) {
        Select firstSelect = firstTable.getOriginalSelect();
        LOG.info(
            "🔍 DEBUG: firstTable.getOriginalSelect() = {}",
            firstSelect != null ? "exists" : "null");

        if (firstSelect != null) {
          List<Hint> firstHints = firstSelect.getHints();
          LOG.info(
              "🔍 DEBUG: firstTable hints count = {}",
              firstHints != null ? firstHints.size() : "null");

          if (firstHints != null && !firstHints.isEmpty()) {
            LOG.info("🔍 DEBUG: First table hints:");
            for (int i = 0; i < firstHints.size(); i++) {
              Hint hint = firstHints.get(i);
              LOG.info(
                  "🔍 DEBUG:   Hint[{}]: type={}, params={}",
                  i,
                  hint.getType(),
                  hint.getParams() != null ? java.util.Arrays.toString(hint.getParams()) : "null");
            }
          }
        }
      }

      // Debug second table
      TableInJoinRequestBuilder secondTable = requestBuilder.getSecondTable();
      LOG.info("🔍 DEBUG: secondTable = {}", secondTable != null ? "exists" : "null");

      if (secondTable != null) {
        Select secondSelect = secondTable.getOriginalSelect();
        LOG.info(
            "🔍 DEBUG: secondTable.getOriginalSelect() = {}",
            secondSelect != null ? "exists" : "null");

        if (secondSelect != null) {
          List<Hint> secondHints = secondSelect.getHints();
          LOG.info(
              "🔍 DEBUG: secondTable hints count = {}",
              secondHints != null ? secondHints.size() : "null");

          if (secondHints != null && !secondHints.isEmpty()) {
            LOG.info("🔍 DEBUG: Second table hints:");
            for (int i = 0; i < secondHints.size(); i++) {
              Hint hint = secondHints.get(i);
              LOG.info(
                  "🔍 DEBUG:   Hint[{}]: type={}, params={}",
                  i,
                  hint.getType(),
                  hint.getParams() != null ? java.util.Arrays.toString(hint.getParams()) : "null");
            }
          }
        }
      }

      // Continue with original logic
      TimeValue timeout = getJoinTimeoutFromTable(requestBuilder.getFirstTable());
      if (timeout != null) {
        return timeout;
      }

      timeout = getJoinTimeoutFromTable(requestBuilder.getSecondTable());
      if (timeout != null) {
        return timeout;
      }

      LOG.info("🔍 DEBUG: No JOIN_TIME_OUT hint found after checking both tables");
      return null;

    } catch (Exception e) {
      LOG.error("🔍 DEBUG: Exception during hint extraction", e);
      return null;
    }
  }

  /** Extract JOIN_TIME_OUT hint from a specific table */
  private TimeValue getJoinTimeoutFromTable(TableInJoinRequestBuilder table) {
    if (table == null) {
      LOG.debug("Table is null, no hints to extract");
      return null;
    }

    Select originalSelect = table.getOriginalSelect();
    if (originalSelect == null) {
      LOG.debug("Original select is null, no hints to extract");
      return null;
    }

    // Get hints from the Select object
    List<Hint> hints = originalSelect.getHints();
    LOG.debug("Found {} hints in select statement", hints != null ? hints.size() : 0);

    if (hints != null && !hints.isEmpty()) {
      for (Hint hint : hints) {
        LOG.debug("Processing hint type: {}", hint.getType());
      }
    }

    return findJoinTimeoutInHints(hints);
  }

  /** Find JOIN_TIME_OUT hint in a list of hints */
  private TimeValue findJoinTimeoutInHints(List<Hint> hints) {
    if (hints == null) {
      LOG.debug("Hints list is null");
      return null;
    }

    LOG.debug("Searching for JOIN_TIME_OUT in {} hints", hints.size());

    for (Hint hint : hints) {
      LOG.debug(
          "Checking hint: type={}, params={}",
          hint.getType(),
          hint.getParams() != null ? java.util.Arrays.toString(hint.getParams()) : "null");

      if (hint.getType() == HintType.JOIN_TIME_OUT) {
        Object[] params = hint.getParams();
        if (params != null && params.length > 0) {
          Integer timeoutSeconds = (Integer) params[0];
          LOG.info(
              "✅ FOUND JOIN_TIME_OUT hint: {} seconds, converting to TimeValue", timeoutSeconds);
          TimeValue result = TimeValue.timeValueSeconds(timeoutSeconds);
          LOG.info("✅ Converted to TimeValue: {} ({}ms)", result, result.getMillis());
          return result;
        } else {
          LOG.warn("JOIN_TIME_OUT hint found but has no parameters");
        }
      }
    }

    LOG.debug("No JOIN_TIME_OUT hint found in hints list");
    return null;
  }

  protected abstract List<SearchHit> innerRun() throws IOException, SqlParseException;

  public SearchHits getHits() {
    return new SearchHits(
        results.toArray(new SearchHit[results.size()]),
        new TotalHits(results.size(), Relation.EQUAL_TO),
        1.0f);
  }

  public static ElasticJoinExecutor createJoinExecutor(
      Client client, SqlElasticRequestBuilder requestBuilder) {
    if (requestBuilder instanceof HashJoinQueryPlanRequestBuilder) {
      return new QueryPlanElasticExecutor(client, (HashJoinQueryPlanRequestBuilder) requestBuilder);
    } else if (requestBuilder instanceof HashJoinElasticRequestBuilder) {
      HashJoinElasticRequestBuilder hashJoin = (HashJoinElasticRequestBuilder) requestBuilder;
      return new HashJoinElasticExecutor(client, hashJoin);
    } else if (requestBuilder instanceof NestedLoopsElasticRequestBuilder) {
      NestedLoopsElasticRequestBuilder nestedLoops =
          (NestedLoopsElasticRequestBuilder) requestBuilder;
      return new NestedLoopsElasticExecutor(client, nestedLoops);
    } else {
      throw new RuntimeException("Unsuported requestBuilder of type: " + requestBuilder.getClass());
    }
  }

  protected void mergeSourceAndAddAliases(
      Map<String, Object> secondTableHitSource,
      SearchHit searchHit,
      String t1Alias,
      String t2Alias) {
    Map<String, Object> results = mapWithAliases(searchHit.getSourceAsMap(), t1Alias);
    results.putAll(mapWithAliases(secondTableHitSource, t2Alias));
    searchHit.getSourceAsMap().clear();
    searchHit.getSourceAsMap().putAll(results);
  }

  protected Map<String, Object> mapWithAliases(Map<String, Object> source, String alias) {
    Map<String, Object> mapWithAliases = new HashMap<>();
    for (Map.Entry<String, Object> fieldNameToValue : source.entrySet()) {
      if (!aliasesOnReturn.contains(fieldNameToValue.getKey())) {
        mapWithAliases.put(alias + "." + fieldNameToValue.getKey(), fieldNameToValue.getValue());
      } else {
        mapWithAliases.put(fieldNameToValue.getKey(), fieldNameToValue.getValue());
      }
    }
    return mapWithAliases;
  }

  protected void onlyReturnedFields(
      Map<String, Object> fieldsMap, List<Field> required, boolean allRequired) {
    HashMap<String, Object> filteredMap = new HashMap<>();
    if (allFieldsReturn || allRequired) {
      filteredMap.putAll(fieldsMap);
      return;
    }
    for (Field field : required) {
      String name = field.getName();
      String returnName = name;
      String alias = field.getAlias();
      if (alias != null && alias != "") {
        returnName = alias;
        aliasesOnReturn.add(alias);
      }
      filteredMap.put(returnName, deepSearchInMap(fieldsMap, name));
    }
    fieldsMap.clear();
    fieldsMap.putAll(filteredMap);
  }

  protected Object deepSearchInMap(Map<String, Object> fieldsMap, String name) {
    if (name.contains(".")) {
      String[] path = name.split("\\.");
      Map<String, Object> currentObject = fieldsMap;
      for (int i = 0; i < path.length - 1; i++) {
        Object valueFromCurrentMap = currentObject.get(path[i]);
        if (valueFromCurrentMap == null) {
          return null;
        }
        if (!Map.class.isAssignableFrom(valueFromCurrentMap.getClass())) {
          return null;
        }
        currentObject = (Map<String, Object>) valueFromCurrentMap;
      }
      return currentObject.get(path[path.length - 1]);
    }

    return fieldsMap.get(name);
  }

  protected void addUnmatchedResults(
      List<SearchHit> combinedResults,
      Collection<SearchHitsResult> firstTableSearchHits,
      List<Field> secondTableReturnedFields,
      int currentNumOfIds,
      int totalLimit,
      String t1Alias,
      String t2Alias) {
    boolean limitReached = false;
    for (SearchHitsResult hitsResult : firstTableSearchHits) {
      if (!hitsResult.isMatchedWithOtherTable()) {
        for (SearchHit hit : hitsResult.getSearchHits()) {

          // todo: decide which id to put or type. or maby its ok this way. just need to doc.
          SearchHit unmachedResult =
              createUnmachedResult(secondTableReturnedFields, hit.docId(), t1Alias, t2Alias, hit);
          combinedResults.add(unmachedResult);
          currentNumOfIds++;
          if (currentNumOfIds >= totalLimit) {
            limitReached = true;
            break;
          }
        }
      }
      if (limitReached) {
        break;
      }
    }
  }

  protected SearchHit createUnmachedResult(
      List<Field> secondTableReturnedFields,
      int docId,
      String t1Alias,
      String t2Alias,
      SearchHit hit) {
    String unmatchedId = hit.getId() + "|0";

    Map<String, DocumentField> documentFields = new HashMap<>();
    Map<String, DocumentField> metaFields = new HashMap<>();
    hit.getFields()
        .forEach(
            (fieldName, docField) ->
                (MapperService.META_FIELDS_BEFORE_7DOT8.contains(fieldName)
                        ? metaFields
                        : documentFields)
                    .put(fieldName, docField));
    SearchHit searchHit = new SearchHit(docId, unmatchedId, documentFields, metaFields);

    searchHit.sourceRef(hit.getSourceRef());
    searchHit.getSourceAsMap().clear();
    searchHit.getSourceAsMap().putAll(hit.getSourceAsMap());
    Map<String, Object> emptySecondTableHitSource = createNullsSource(secondTableReturnedFields);

    mergeSourceAndAddAliases(emptySecondTableHitSource, searchHit, t1Alias, t2Alias);

    return searchHit;
  }

  protected Map<String, Object> createNullsSource(List<Field> secondTableReturnedFields) {
    Map<String, Object> nulledSource = new HashMap<>();
    for (Field field : secondTableReturnedFields) {
      if (!field.getName().equals("*")) {
        nulledSource.put(field.getName(), null);
      }
    }
    return nulledSource;
  }

  protected void updateMetaSearchResults(SearchResponse searchResponse) {
    this.metaResults.addSuccessfulShards(searchResponse.getSuccessfulShards());
    this.metaResults.addFailedShards(searchResponse.getFailedShards());
    this.metaResults.addTotalNumOfShards(searchResponse.getTotalShards());
    this.metaResults.updateTimeOut(searchResponse.isTimedOut());
  }

  public SearchResponse getResponseWithHits(
      TableInJoinRequestBuilder tableRequest, int size, SearchResponse previousResponse) {

    return getResponseWithHits(
        tableRequest.getRequestBuilder(),
        tableRequest.getOriginalSelect(),
        size,
        previousResponse,
        pit);
  }

  public String[] getIndices(JoinRequestBuilder joinRequestBuilder) {
    return Stream.concat(
            Stream.of(joinRequestBuilder.getFirstTable().getOriginalSelect().getIndexArr()),
            Stream.of(joinRequestBuilder.getSecondTable().getOriginalSelect().getIndexArr()))
        .distinct()
        .toArray(String[]::new);
  }
}
