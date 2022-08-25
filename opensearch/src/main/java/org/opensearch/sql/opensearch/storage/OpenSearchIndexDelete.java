/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.planner.physical.DeleteOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;


public class OpenSearchIndexDelete extends DeleteOperator {

  private final OpenSearchClient client;

  private int count;

  public OpenSearchIndexDelete(OpenSearchClient client, PhysicalPlan input, String tableName) {
    super(input, tableName);
    this.client = client;
  }

  @Override
  public void open() {
    client.delete(tableName);
    count = 1;
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

  @Override
  public ExecutionEngine.Schema schema() {
    return new ExecutionEngine.Schema(Arrays.asList(
        new ExecutionEngine.Schema.Column("message", "message", ExprCoreType.STRING)));
  }

  @Override
  public void close() {
    // do nothing
  }
}
