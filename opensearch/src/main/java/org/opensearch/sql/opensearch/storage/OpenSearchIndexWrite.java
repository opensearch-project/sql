/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.WriteOperator;

/**
 * OpenSearch index write operator.
 */
public class OpenSearchIndexWrite extends WriteOperator {

  private static final Logger LOG = LogManager.getLogger();

  private final OpenSearchClient client;

  private int count;

  public OpenSearchIndexWrite(OpenSearchClient client, PhysicalPlan input,
                              String tableName, List<String> columns) {
    super(input, tableName, columns);
    this.client = client;
  }

  @Override
  public void open() {
    super.open();

    List<Map<String, Object>> data = new ArrayList<>();

    Instant startS3 = Instant.now();
    while (input.hasNext()) {
      count++;

      ExprValue row = input.next();
      if (row.type() == ExprCoreType.ARRAY) { // from ValuesOperator
        Map<String, Object> colValues = new HashMap<>();
        List<ExprValue> values = row.collectionValue();
        for (int i = 0; i < values.size(); i++) {
          colValues.put(columns.get(i), values.get(i).value());
        }
        data.add(colValues);
      } else { // from normal ProjectOperator
        data.add(row.tupleValue().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().value())));
      }
    }
    LOG.info("S3 Read, size:{}, took:{} mills", data.size(), Duration.between(startS3,
        Instant.now()).toMillis());

    Instant startOS = Instant.now();
    client.bulk(tableName, data);
    LOG.info("Index Write, size:{}, took:{} mills", data.size(), Duration.between(startOS,
          Instant.now()).toMillis());
  }

  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(Arrays.asList(
        new ExecutionEngine.Schema.Column("message", "message", ExprCoreType.STRING)));
  }

  @Override
  public boolean hasNext() {
    return (count > 0);
  }

  @Override
  public ExprValue next() {
    ExprValue result = tupleValue(
        ImmutableMap.of("message", stringValue(count + " row(s) impacted")));
    count = 0;
    return result;
  }
}
