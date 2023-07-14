/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.executor.multi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.document.DocumentField;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.legacy.executor.ElasticHitsExecutor;
import org.opensearch.sql.legacy.query.multi.MultiQueryRequestBuilder;
import org.opensearch.sql.legacy.utils.Util;

/**
 * Created by Eliran on 21/8/2016.
 */
public class UnionExecutor implements ElasticHitsExecutor {

    private MultiQueryRequestBuilder multiQueryBuilder;
    private SearchHits results;
    private Client client;
    private int currentId;

    public UnionExecutor(Client client, MultiQueryRequestBuilder builder) {
        multiQueryBuilder = builder;
        this.client = client;
        currentId = 0;
    }

    @Override
    public void run() {
        SearchResponse firstResponse = this.multiQueryBuilder.getFirstSearchRequest().get();
        SearchHit[] hits = firstResponse.getHits().getHits();
        List<SearchHit> unionHits = new ArrayList<>(hits.length);
        fillInternalSearchHits(unionHits, hits, this.multiQueryBuilder.getFirstTableFieldToAlias());
        SearchResponse secondResponse = this.multiQueryBuilder.getSecondSearchRequest().get();
        fillInternalSearchHits(unionHits, secondResponse.getHits().getHits(),
                this.multiQueryBuilder.getSecondTableFieldToAlias());
        int totalSize = unionHits.size();
        SearchHit[] unionHitsArr = unionHits.toArray(new SearchHit[totalSize]);
        this.results = new SearchHits(unionHitsArr, new TotalHits(totalSize, Relation.EQUAL_TO), 1.0f);
    }

    private void fillInternalSearchHits(List<SearchHit> unionHits, SearchHit[] hits,
                                        Map<String, String> fieldNameToAlias) {
        for (SearchHit hit : hits) {
            Map<String, DocumentField> documentFields = new HashMap<>();
            Map<String, DocumentField> metaFields = new HashMap<>();
            hit.getFields().forEach((fieldName, docField) ->
                (MapperService.META_FIELDS_BEFORE_7DOT8.contains(fieldName) ? metaFields : documentFields).put(fieldName, docField));
            SearchHit searchHit = new SearchHit(currentId, hit.getId(), documentFields, metaFields);
            searchHit.sourceRef(hit.getSourceRef());
            searchHit.getSourceAsMap().clear();
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            if (!fieldNameToAlias.isEmpty()) {
                updateFieldNamesToAlias(sourceAsMap, fieldNameToAlias);
            }
            searchHit.getSourceAsMap().putAll(sourceAsMap);
            currentId++;
            unionHits.add(searchHit);
        }
    }


    private void updateFieldNamesToAlias(Map<String, Object> sourceAsMap, Map<String, String> fieldNameToAlias) {
        for (Map.Entry<String, String> fieldToAlias : fieldNameToAlias.entrySet()) {
            String fieldName = fieldToAlias.getKey();
            Object value = null;
            Map<String, Object> deleteFrom = null;
            if (fieldName.contains(".")) {
                String[] split = fieldName.split("\\.");
                String[] path = Arrays.copyOf(split, split.length - 1);
                Object placeInMap = Util.searchPathInMap(sourceAsMap, path);
                if (placeInMap != null) {
                    if (!Map.class.isAssignableFrom(placeInMap.getClass())) {
                        continue;
                    }
                }
                deleteFrom = (Map<String, Object>) placeInMap;
                value = deleteFrom.get(split[split.length - 1]);
            } else if (sourceAsMap.containsKey(fieldName)) {
                value = sourceAsMap.get(fieldName);
                deleteFrom = sourceAsMap;
            }
            if (value != null) {
                sourceAsMap.put(fieldToAlias.getValue(), value);
                deleteFrom.remove(fieldName);
            }
        }
        Util.clearEmptyPaths(sourceAsMap);
    }

    @Override
    public SearchHits getHits() {
        return results;
    }
}
