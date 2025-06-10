/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.operator.predicate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.openjdk.jmh.annotations.Benchmark;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.request.system.OpenSearchDescribeIndexRequest;

public class MergeArrayAndObjectMapBenchmark {
  private static final List<Map<String, OpenSearchDataType>> candidateMaps = prepareListOfMaps(120);

  @Benchmark
  public void testMerge() {
    Map<String, OpenSearchDataType> finalResult = new HashMap<>();
    for (Map<String, OpenSearchDataType> map : candidateMaps) {
      OpenSearchDescribeIndexRequest.mergeObjectAndArrayInsideMap(finalResult, map);
    }
  }

  private static Map<String, OpenSearchDataType> prepareMap(int recursive, String prefix) {
    Map<String, OpenSearchDataType> map = new HashMap<>();
    Map<String, Object> innerMap = prepareRecursiveMap(recursive, prefix);
    map.put("name", OpenSearchDataType.of(OpenSearchDataType.MappingType.Object, innerMap));
    return map;
  }

  public static Map<String, Object> prepareRecursiveMap(int recursive, String prefix) {
    Map<String, Object> innerMap = new LinkedHashMap<>();
    if (recursive == 0) {
      innerMap.put("type", "string");
    } else {
      innerMap.put("type", "object");
      innerMap.put(
          "properties",
          Map.of(
              prefix + "_" + String.valueOf(recursive),
              Map.of("type", "text"),
              "recursive",
              prepareRecursiveMap(recursive - 1, prefix)));
    }
    return innerMap;
  }

  private static List<Map<String, OpenSearchDataType>> prepareListOfMaps(int listNumber) {
    List<Map<String, OpenSearchDataType>> list = new ArrayList<>();
    for (int i = 0; i < listNumber; i++) {
      list.add(prepareMap(15, "prefix" + i));
    }
    return list;
  }
}
