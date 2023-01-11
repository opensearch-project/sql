/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.flink;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSqlEngine {

  public void initialize() {
    /*
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    EnvironmentSettings setting = EnvironmentSettings.newInstance()
        .withClassLoader(classloader)
        .build();
     */

    LocalStreamEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(execEnv);

    tableEnv.executeSql("SELECT 123").print();
  }

}
