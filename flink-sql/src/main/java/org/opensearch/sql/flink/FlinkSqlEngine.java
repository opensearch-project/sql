/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.flink;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

public class FlinkSqlEngine {

  private final StreamTableEnvironment tableEnv;

  public FlinkSqlEngine() {
    LocalStreamEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
    this.tableEnv = StreamTableEnvironment.create(execEnv);

    tableEnv.registerCatalog("my_s3", new GenericInMemoryCatalog("my_s3"));

    // ?
    tableEnv.executeSql("SELECT 123").print();
  }

  public String execute(String sql) {
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    tableEnv.executeSql("SELECT 'hello' ").print();

    tableEnv.executeSql(sql).print();

    return "";
  }
}
