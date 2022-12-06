/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.write;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.executor.ExecutionEngine.Schema;
import static org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.write.TableWriteOperator;

/**
 * OpenSearch index write operator.
 */
public class OpenSearchIndexWrite extends TableWriteOperator {

  /** OpenSearch low-level client. */
  private final OpenSearchClient client;

  /** Plugin settings. */
  private final Settings settings;

  /** Index name. */
  private final IndexName indexName;

  /** Column name list. */
  private final List<String> columns;

  /** Counter of indexed documents. */
  private long count;

  public OpenSearchIndexWrite(PhysicalPlan child,
                              OpenSearchClient client,
                              Settings settings,
                              IndexName indexName,
                              List<String> columns) {
    super(child);
    this.client = client;
    this.settings = settings;
    this.indexName = indexName;
    this.columns = columns;
  }

  @Override
  public Schema schema() {
    return new Schema(Arrays.asList(
        new Schema.Column("message", "message", ExprCoreType.STRING)));
  }

  @Override
  public void open() {
    super.open();

    do {
      List<Map<String, Object>> nextBatch = loadNextBatch();
      if (nextBatch.isEmpty()) {
        break;
      }
      count += nextBatch.size();
      client.bulk(indexName.toString(), nextBatch);
    } while (true);
  }

  @Override
  public boolean hasNext() {
    return (count > 0);
  }

  @Override
  public ExprValue next() {
    ExprValue message = tupleValue(
        ImmutableMap.of("message", stringValue(count + " row(s) impacted")));
    count = 0;
    return message;
  }

  @Override
  public String explain() {
    return indexName.toString();
  }

  // TODO: Add buffer size setting in perf test branch
  private List<Map<String, Object>> loadNextBatch() {
    List<Map<String, Object>> nextBatch = new ArrayList<>();
    while (input.hasNext()) {
      ExprValue row = input.next();

      // ValuesOperator is special whose result is collection of literal values without names
      // Normally, tuple with map from column name to its value is returned from other operators
      if (row.type() == ExprCoreType.ARRAY) {
        nextBatch.add(loadValues(row));
      } else {
        nextBatch.add(loadTuples(row));
      }
    }
    return nextBatch;
  }

  private Map<String, Object> loadValues(ExprValue row) {
    // zip column names and values list
    List<ExprValue> values = row.collectionValue();
    return IntStream.range(0, values.size()).boxed()
        .collect(Collectors.toMap(columns::get, i -> values.get(i).value()));
  }

  private Map<String, Object> loadTuples(ExprValue row) {
    return row.tupleValue().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().value()));
  }
}
