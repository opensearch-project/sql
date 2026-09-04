/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.Capability.CHAINED_STREAMSTATS_BY;
import static org.opensearch.sql.util.Capability.DOC_MUTATION;
import static org.opensearch.sql.util.Capability.STREAMSTATS_SORT_NOT_HONORED;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

public class CalciteStreamstatsCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.STATE_COUNTRY_ORDERED);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL_ORDERED);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL_SINGLE_SHARD);
    loadIndex(Index.BANK_TWO);
    loadIndex(Index.LOGS);
  }

  // streamstats computes running/window aggregates over the input stream in encounter order. On a
  // multi-shard index that order is not deterministic, so the per-row cumulative values diverge
  // between runs. To assert exact stream semantics we feed a deterministic in-memory stream via
  // `makeresults format=csv` (typed inline rows, single relation, no shards) that mirrors the
  // STATE_COUNTRY / STATE_COUNTRY_WITH_NULL fixtures row-for-row and in the same column order the
  // index presents (name, country, state, month, year, age). The expected values are unchanged --
  // only the source is made deterministic. Real multi-shard index coverage is retained separately
  // by testStreamstatsIndexMultiShardCoverage below, which asserts order-independent properties.
  private static final String SC =
      "name:string,country:string,state:string,month:int,year:int,age:int\\n"
          + "Jake,USA,California,4,2023,70\\n"
          + "Hello,USA,New York,4,2023,30\\n"
          + "John,Canada,Ontario,4,2023,25\\n"
          + "Jane,Canada,Quebec,4,2023,20";

  // Doc-mutation fixtures: the original tests PUT an extra "Jay" doc as the last document. These
  // deterministic streams preserve that order without mutating a shared index.
  private static final String SC_JAY40 = SC + "\\nJay,USA,Quebec,4,2023,40";
  private static final String SC_JAY28 = SC + "\\nJay,USA,Quebec,4,2023,28";

  private static final String LOGS_STREAM =
      "created_at:string,server:string,@timestamp:string,message:string,level:string\\n"
          + "2023-01-05T00:00:00.000Z,server1,2023-01-01T00:00:00.000Z,Database connection"
          + " failed,ERROR\\n"
          + "2023-01-04T00:00:00.000Z,server2,2023-01-02T00:00:00.000Z,Service started,INFO\\n"
          + "2023-01-03T00:00:00.000Z,server1,2023-01-03T00:00:00.000Z,High memory usage,WARN\\n"
          + "2023-01-02T00:00:00.000Z,server3,2023-01-04T00:00:00.000Z,Disk space low,ERROR\\n"
          + "2023-01-01T00:00:00.000Z,server2,2023-01-05T00:00:00.000Z,Backup completed,INFO";

  @Test
  public void testStreamstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats count() as cnt, avg(age) as avg,"
                    + " min(age) as min, max(age) as max | fields name, country, state, month,"
                    + " year, age, cnt, avg, min, max",
                SC));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"),
        schema("min", "int"),
        schema("max", "int"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3, 41.666666666666664, 25, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4, 36.25, 20, 70));
  }

  @Test
  // Multi-shard determinism: streamstats orders by encounter order, which is non-deterministic
  // across shards. Sourcing the seq-augmented fixture and adding `| sort seq` restores the
  // single-shard encounter order on any shard layout; the AE route ignores that sort
  // (STREAMSTATS_SORT_NOT_HONORED), so this exact-value assertion is gated to the routes that
  // honor it. The expected rows are unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats count() as cnt, avg(age) as avg, min(age) as"
                    + " min, max(age) as max | fields name, country, state, month, year, age, cnt,"
                    + " avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"),
        schema("min", "int"),
        schema("max", "int"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3, 41.666666666666664, 25, 70),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4, 36.25, 20, 70),
        rows(null, "Canada", null, 4, 2023, 10, 5, 31, 10, 70),
        rows("Kevin", null, null, 4, 2023, null, 6, 31, 10, 70));
  }

  @Test
  public void testStreamstatsBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats count() as cnt, avg(age) as avg,"
                    + " min(age) as min, max(age) as max by country | fields name, country, state,"
                    + " month, year, age, cnt, avg, min, max",
                SC));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"),
        schema("min", "int"),
        schema("max", "int"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25));
  }

  @Test
  // See testStreamstatsWithNull: `| sort seq` on the seq-augmented fixture restores the
  // single-shard encounter order across shards; gated to routes that honor a sort before
  // streamstats. Expected rows are unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsByWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats count() as cnt, avg(age) as avg, min(age) as"
                    + " min, max(age) as max by country | fields name, country, state, month, year,"
                    + " age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"),
        schema("min", "int"),
        schema("max", "int"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 3, 18.333333333333332, 10, 25),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null));

    actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats count() as cnt, avg(age) as avg, min(age) as"
                    + " min, max(age) as max by state | fields name, country, state, month, year,"
                    + " age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));
    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 2, 10, 10, 10));
  }

  @Test
  // Multi-shard determinism: streamstats orders by encounter order, which is non-deterministic
  // across shards. Sourcing the seq-augmented fixture and adding `| sort seq` restores the
  // single-shard encounter order on any shard layout (the Calcite/Lucene route honors a sort
  // before streamstats). The AE route ignores that sort (STREAMSTATS_SORT_NOT_HONORED), so this
  // exact-value assertion is gated to the routes that honor it; the expected rows are unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsByWithNullBucket() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats bucket_nullable=false count() as cnt, avg(age)"
                    + " as avg, min(age) as min, max(age) as max by country | fields name, country,"
                    + " state, month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("cnt", "bigint"),
        schema("avg", "double"),
        schema("min", "int"),
        schema("max", "int"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2, 50, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 3, 18.333333333333332, 10, 25),
        rows("Kevin", null, null, 4, 2023, null, null, null, null, null));

    actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats bucket_nullable=false count() as cnt, avg(age)"
                    + " as avg, min(age) as min, max(age) as max by state | fields name, country,"
                    + " state, month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));
    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows(null, "Canada", null, 4, 2023, 10, null, null, null, null),
        rows("Kevin", null, null, 4, 2023, null, null, null, null, null));
  }

  @Test
  public void testStreamstatsBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats count() as cnt, avg(age) as avg,"
                    + " min(age) as min, max(age) as max by span(age, 10) as age_span | fields"
                    + " name, country, state, month, year, age, cnt, avg, min, max",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25));
  }

  @Test
  public void testStreamstatsBySpanWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt, avg(age) as avg, min(age) as min, max(age)"
                    + " as max by span(age, 10) as age_span | fields name, country, state, month,"
                    + " year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null));
  }

  @Test
  public void testStreamstatsByMultiplePartitions1() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats count() as cnt, avg(age) as avg,"
                    + " min(age) as min, max(age) as max by span(age, 10) as age_span, country |"
                    + " fields name, country, state, month, year, age, cnt, avg, min, max",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25));
  }

  @Test
  public void testStreamstatsByMultiplePartitions2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats count() as cnt, avg(age) as avg,"
                    + " min(age) as min, max(age) as max by span(age, 10) as age_span, state |"
                    + " fields name, country, state, month, year, age, cnt, avg, min, max",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20));
  }

  @Test
  public void testStreamstatsByMultiplePartitionsWithNull1() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats bucket_nullable=false count() as cnt, avg(age) as avg,"
                    + " min(age) as min, max(age) as max by span(age, 10) as age_span, country |"
                    + " fields name, country, state, month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, null, null, null, null));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | streamstats bucket_nullable=true count() as cnt, avg(age) as avg,"
                    + " min(age) as min, max(age) as max by span(age, 10) as age_span, country |"
                    + " fields name, country, state, month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2, 22.5, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null));
  }

  @Test
  public void testStreamstatsByMultiplePartitionsWithNull2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats bucket_nullable=false count() as cnt, avg(age) as avg,"
                    + " min(age) as min, max(age) as max by span(age, 10) as age_span, state |"
                    + " fields name, country, state, month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows(null, "Canada", null, 4, 2023, 10, null, null, null, null),
        rows("Kevin", null, null, 4, 2023, null, null, null, null, null));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | streamstats bucket_nullable=true count() as cnt, avg(age) as avg,"
                    + " min(age) as min, max(age) as max by span(age, 10) as age_span, state |"
                    + " fields name, country, state, month, year, age, cnt, avg, min, max",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, 1, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1, 30, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 1, 20, 20, 20),
        rows(null, "Canada", null, 4, 2023, 10, 1, 10, 10, 10),
        rows("Kevin", null, null, 4, 2023, null, 1, null, null, null));
  }

  @Test
  public void testStreamstatsCurrent() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats current=false avg(age) as prev_avg"
                    + " | fields name, country, state, month, year, age, prev_avg",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 50),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 41.666666666666664));
  }

  @Test
  // See testStreamstatsWithNull: `| sort seq` on the seq-augmented fixture restores the
  // single-shard encounter order across shards; gated to routes that honor a sort before
  // streamstats. Expected rows are unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsCurrentWithNUll() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats current=false avg(age) as prev_avg | fields"
                    + " name, country, state, month, year, age, prev_avg",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 50),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 41.666666666666664),
        rows(null, "Canada", null, 4, 2023, 10, 36.25),
        rows("Kevin", null, null, 4, 2023, null, 31));
  }

  @Test
  public void testStreamstatsWindow() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats window = 3 avg(age) as avg | fields"
                    + " name, country, state, month, year, age, avg",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 41.666666666666664),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 25));
  }

  @Test
  // See testStreamstatsWithNull: `| sort seq` on the seq-augmented fixture restores the
  // single-shard encounter order across shards; gated to routes that honor a sort before
  // streamstats. Expected rows are unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsWindowWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats window = 3 avg(age) as avg | fields name,"
                    + " country, state, month, year, age, avg",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 41.666666666666664),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 25),
        rows(null, "Canada", null, 4, 2023, 10, 18.333333333333332),
        rows("Kevin", null, null, 4, 2023, null, 15));
  }

  @Test
  public void testStreamstatsBigWindow() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats window = 10 avg(age) as avg |"
                    + " fields name, country, state, month, year, age, avg",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 41.666666666666664),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 36.25));
  }

  @Test
  public void testStreamstatsWindowError() {
    Throwable e =
        assertThrowsWithReplace(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | streamstats window=-1 avg(age) as avg",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(e, "Window size must be >= 0, but got: -1");
  }

  @Test
  public void testStreamstatsCurrentAndWindow() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats current = false window = 2 avg(age)"
                    + " as avg | fields name, country, state, month, year, age, avg",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 50),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 27.5));
  }

  @Test
  // See testStreamstatsWithNull: `| sort seq` on the seq-augmented fixture restores the
  // single-shard encounter order across shards; gated to routes that honor a sort before
  // streamstats. Expected rows are unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsCurrentAndWindowWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats current = false window = 2 avg(age) as avg |"
                    + " fields name, country, state, month, year, age, avg",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 70),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 50),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 27.5),
        rows(null, "Canada", null, 4, 2023, 10, 22.5),
        rows("Kevin", null, null, 4, 2023, null, 15));
  }

  @Test
  public void testStreamstatsGlobal() throws IOException {
    // Jay (age 40) is inlined as the trailing row of the deterministic stream, matching the
    // original test which PUT it as the last document (encountered last) then DELETEd it.
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats window=2 global=false avg(age) as"
                    + " avg by country | fields name, country, state, month, year, age, avg",
                SC_JAY40));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows("Jay", "USA", "Quebec", 4, 2023, 40, 35));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats window=2 global=true avg(age) as"
                    + " avg by country | fields name, country, state, month, year, age, avg",
                SC_JAY40));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows("Jay", "USA", "Quebec", 4, 2023, 40, 40));
  }

  @Test
  @RequiresCapability({DOC_MUTATION, STREAMSTATS_SORT_NOT_HONORED})
  public void testStreamstatsGlobalWithNull() throws IOException {
    // Jay is PUT with seq 7 (one past the fixture's 6 rows) so `| sort seq` keeps it in the
    // trailing encounter position the original single-shard test relied on, while the sort makes
    // the order deterministic across shards. Mutation stays DOC_MUTATION-gated; the added sort is
    // ignored on the AE route (STREAMSTATS_SORT_NOT_HONORED). Expected rows are unchanged.
    final int docId = 7;
    Request insertRequest =
        new Request(
            "PUT",
            String.format(
                "/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED, docId));
    insertRequest.setJsonEntity(
        "{\"name\": \"Jay\",\"age\": 40,\"state\":"
            + " \"Quebec\",\"country\": \"USA\",\"year\": 2023,\"month\":"
            + " 4,\"seq\": 7}\n");
    client().performRequest(insertRequest);
    try {
      JSONObject actual =
          executeQuery(
              String.format(
                  "source=%s | sort seq | streamstats window=2 global=false avg(age) as avg by"
                      + " country | fields name, country, state, month, year, age, avg",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

      verifyDataRows(
          actual,
          rows("Jake", "USA", "California", 4, 2023, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 50),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
          rows(null, "Canada", null, 4, 2023, 10, 15),
          rows("Kevin", null, null, 4, 2023, null, null),
          rows("Jay", "USA", "Quebec", 4, 2023, 40, 35));

      JSONObject actual2 =
          executeQuery(
              String.format(
                  "source=%s | sort seq | streamstats window=2 global=true avg(age) as avg by"
                      + " country | fields name, country, state, month, year, age, avg",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

      verifyDataRows(
          actual2,
          rows("Jake", "USA", "California", 4, 2023, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 50),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
          rows(null, "Canada", null, 4, 2023, 10, 15),
          rows("Kevin", null, null, 4, 2023, null, null),
          rows("Jay", "USA", "Quebec", 4, 2023, 40, 40));
    } finally {
      Request deleteRequest =
          new Request(
              "DELETE",
              String.format(
                  "/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED, docId));
      client().performRequest(deleteRequest);
    }
  }

  @Test
  @RequiresCapability(DOC_MUTATION)
  public void testStreamstatsGlobalWithNullBucket() throws IOException {
    final int docId = 7;
    Request insertRequest =
        new Request(
            "PUT",
            String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_WITH_NULL, docId));
    insertRequest.setJsonEntity(
        "{\"name\": \"Jay\",\"age\": 40,\"state\":"
            + " \"Quebec\",\"country\": \"USA\",\"year\": 2023,\"month\":"
            + " 4}\n");
    client().performRequest(insertRequest);
    try {
      JSONObject actual =
          executeQuery(
              String.format(
                  "source=%s | streamstats bucket_nullable=false window=2 global=true avg(age) as"
                      + " avg by state | fields name, country, state, month, year, age, avg",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL));

      verifyDataRows(
          actual,
          rows("Jake", "USA", "California", 4, 2023, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 30),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
          rows(null, "Canada", null, 4, 2023, 10, null),
          rows("Kevin", null, null, 4, 2023, null, null),
          rows("Jay", "USA", "Quebec", 4, 2023, 40, 40));

      JSONObject actual2 =
          executeQuery(
              String.format(
                  "source=%s | streamstats bucket_nullable=true window=2 global=true avg(age) as"
                      + " avg by state | fields name, country, state, month, year, age, avg",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL));

      verifyDataRows(
          actual2,
          rows("Jake", "USA", "California", 4, 2023, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 30),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
          rows(null, "Canada", null, 4, 2023, 10, 10),
          rows("Kevin", null, null, 4, 2023, null, 10),
          rows("Jay", "USA", "Quebec", 4, 2023, 40, 40));
    } finally {
      Request deleteRequest =
          new Request(
              "DELETE",
              String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_WITH_NULL, docId));
      client().performRequest(deleteRequest);
    }
  }

  @Test
  public void testStreamstatsReset() throws IOException {
    // Jay (age 28) is inlined as the trailing row of the deterministic stream, matching the
    // original test which PUT it as the last document (encountered last) then DELETEd it.
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats window=2 reset_before=age>29"
                    + " avg(age) as avg by country | fields name, country, state, month, year, age,"
                    + " avg",
                SC_JAY28));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats window=2 reset_after=age>22"
                    + " avg(age) as avg by country | fields name, country, state, month, year, age,"
                    + " avg",
                SC_JAY28));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
        rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));
  }

  @Test
  @RequiresCapability(DOC_MUTATION)
  public void testStreamstatsResetWithNull() throws IOException {
    // reset_before/reset_after streamstats builds a self-correlated plan that the physical compiler
    // cannot combine with an upstream `sort` (planner IndexOutOfBounds), so the seq-sort trick used
    // by the other WithNull streamstats tests is unavailable here. Instead this drives a
    // single-shard fixture whose encounter order is the deterministic insertion order on any run,
    // reproducing the original single-shard behavior without injecting a sort. Jay is PUT as the
    // last document (docId 7), matching the original trailing-encounter position. Expected rows are
    // unchanged.
    final int docId = 7;
    Request insertRequest =
        new Request(
            "PUT",
            String.format(
                "/%s/_doc/%d?refresh=true",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_SINGLE_SHARD, docId));
    insertRequest.setJsonEntity(
        "{\"name\": \"Jay\",\"age\": 28,\"state\":"
            + " \"Quebec\",\"country\": \"USA\",\"year\": 2023,\"month\":"
            + " 4}\n");
    client().performRequest(insertRequest);
    try {
      JSONObject actual =
          executeQuery(
              String.format(
                  "source=%s | streamstats window=2 reset_before=age>29 avg(age) as avg by country"
                      + " | fields name, country, state, month, year, age, avg",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL_SINGLE_SHARD));

      verifyDataRows(
          actual,
          rows("Jake", "USA", "California", 4, 2023, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 30),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
          rows(null, "Canada", null, 4, 2023, 10, 15),
          rows("Kevin", null, null, 4, 2023, null, null),
          rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));

      JSONObject actual2 =
          executeQuery(
              String.format(
                  "source=%s | streamstats window=2 reset_after=age>22 avg(age) as avg by country"
                      + " | fields name, country, state, month, year, age, avg",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL_SINGLE_SHARD));

      verifyDataRows(
          actual2,
          rows("Jake", "USA", "California", 4, 2023, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 30),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
          rows(null, "Canada", null, 4, 2023, 10, 15),
          rows("Kevin", null, null, 4, 2023, null, null),
          rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));
    } finally {
      Request deleteRequest =
          new Request(
              "DELETE",
              String.format(
                  "/%s/_doc/%d?refresh=true",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL_SINGLE_SHARD, docId));
      client().performRequest(deleteRequest);
    }
  }

  @Test
  @RequiresCapability(DOC_MUTATION)
  public void testStreamstatsResetWithNullBucket() throws IOException {
    // See testStreamstatsResetWithNull: reset_before/reset_after streamstats builds a
    // self-correlated plan whose segment id and sliding window frame are both defined over a global
    // ROW_NUMBER() sequence. That sequence follows the raw scan (encounter) order, which is
    // non-deterministic across shards, and the reset plan cannot be combined with an upstream
    // `sort` (planner IndexOutOfBounds), so the seq-sort trick used by the other WithNull tests is
    // unavailable here. Driving the single-shard fixture makes the encounter order the insertion
    // order on any run, reproducing the original single-shard behavior. Jay is PUT as the last
    // document (docId 7), matching the original trailing-encounter position. Expected rows are
    // unchanged.
    final int docId = 7;
    Request insertRequest =
        new Request(
            "PUT",
            String.format(
                "/%s/_doc/%d?refresh=true",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_SINGLE_SHARD, docId));
    insertRequest.setJsonEntity(
        "{\"name\": \"Jay\",\"age\": 28,\"state\":"
            + " \"Quebec\",\"country\": \"USA\",\"year\": 2023,\"month\":"
            + " 4}\n");
    client().performRequest(insertRequest);
    try {
      JSONObject actual =
          executeQuery(
              String.format(
                  "source=%s | streamstats bucket_nullable=true window=2 reset_before=age>29"
                      + " avg(age) as avg by state | fields name, country, state, month, year,"
                      + " age, avg",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL_SINGLE_SHARD));

      verifyDataRows(
          actual,
          rows("Jake", "USA", "California", 4, 2023, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 30),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
          rows(null, "Canada", null, 4, 2023, 10, 10),
          rows("Kevin", null, null, 4, 2023, null, 10),
          rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));

      JSONObject actual2 =
          executeQuery(
              String.format(
                  "source=%s | streamstats bucket_nullable=false window=2 reset_after=age>22"
                      + " avg(age) as avg by state | fields name, country, state, month, year,"
                      + " age, avg",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL_SINGLE_SHARD));

      verifyDataRows(
          actual2,
          rows("Jake", "USA", "California", 4, 2023, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 30),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
          rows(null, "Canada", null, 4, 2023, 10, null),
          rows("Kevin", null, null, 4, 2023, null, null),
          rows("Jay", "USA", "Quebec", 4, 2023, 28, 28));
    } finally {
      Request deleteRequest =
          new Request(
              "DELETE",
              String.format(
                  "/%s/_doc/%d?refresh=true",
                  TEST_INDEX_STATE_COUNTRY_WITH_NULL_SINGLE_SHARD, docId));
      client().performRequest(deleteRequest);
    }
  }

  @Test
  public void testUnsupportedWindowFunctions() {
    List<String> unsupported = List.of("PERCENTILE_APPROX", "PERCENTILE");
    for (String u : unsupported) {
      Throwable e =
          assertThrowsWithReplace(
              UnsupportedOperationException.class,
              () ->
                  executeQuery(
                      String.format(
                          "source=%s | streamstats %s(age)", TEST_INDEX_STATE_COUNTRY, u)));
      verifyErrorMessageContains(e, "Unexpected window function: " + u);
    }
  }

  @Test
  @RequiresCapability(CHAINED_STREAMSTATS_BY)
  public void testMultipleStreamstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats avg(age) as avg_age by state,"
                    + " country | streamstats avg(avg_age) as avg_state_age by country | fields"
                    + " name, country, state, month, year, age, avg_age, avg_state_age",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20, 22.5));
  }

  @Test
  @RequiresCapability({CHAINED_STREAMSTATS_BY, STREAMSTATS_SORT_NOT_HONORED})
  public void testMultipleStreamstatsWithWindow() throws IOException {
    // Test case from GitHub issue #4800: chained streamstats with window=2.
    // `| sort seq` on the seq-augmented fixture makes the encounter order deterministic across
    // shards; gated to routes that honor a sort before streamstats. Expected rows are unchanged.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats window=2 avg(age) as avg_age by state, country"
                    + " | streamstats window=2 avg(avg_age) as avg_state_age by country | fields"
                    + " name, country, state, month, year, age, avg_age, avg_state_age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("avg_age", "double"),
        schema("avg_state_age", "double"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20, 22.5),
        rows(null, "Canada", null, 4, 2023, 10, 10, 15),
        rows("Kevin", null, null, 4, 2023, null, null, null));
  }

  // TODO: Fix chained reset_before + window streamstats (nested correlate issue, see #4800)
  // The reset path still uses correlate, and the window self-join copies it into the right side,
  // causing Calcite's RelDecorrelator to fail on duplicate correlate references.

  @Test
  @RequiresCapability({CHAINED_STREAMSTATS_BY, STREAMSTATS_SORT_NOT_HONORED})
  public void testMultipleStreamstatsWithNull1() throws IOException {
    // `| sort seq` on the seq-augmented fixture makes the encounter order deterministic across
    // shards; gated to routes that honor a sort before streamstats. Expected rows are unchanged.
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats avg(age) as avg_age by state, country |"
                    + " streamstats avg(avg_age) as avg_state_age by country | fields name,"
                    + " country, state, month, year, age, avg_age, avg_state_age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20, 22.5),
        rows(null, "Canada", null, 4, 2023, 10, 10, 18.333333333333332),
        rows("Kevin", null, null, 4, 2023, null, null, null));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats bucket_nullable=false avg(age) as avg_age by"
                    + " state, country | streamstats bucket_nullable=false avg(avg_age) as"
                    + " avg_state_age by country | fields name, country, state, month, year, age,"
                    + " avg_age, avg_state_age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20, 22.5),
        rows(null, "Canada", null, 4, 2023, 10, null, 22.5),
        rows("Kevin", null, null, 4, 2023, null, null, null));
  }

  @Test
  @RequiresCapability({DOC_MUTATION, CHAINED_STREAMSTATS_BY, STREAMSTATS_SORT_NOT_HONORED})
  public void testMultipleStreamstatsWithNull2() throws IOException {
    // Jay is PUT as the last document (seq 5, one past the fixture's 4 rows) so `| sort seq` keeps
    // it in the trailing encounter position the original single-shard test relied on, while the
    // sort makes the order deterministic across shards. Mutation stays DOC_MUTATION-gated.
    final int docId = 5;
    Request insertRequest =
        new Request(
            "PUT",
            String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_ORDERED, docId));
    insertRequest.setJsonEntity(
        "{\"name\": \"Jay\",\"age\": 28,"
            + " \"country\": \"USA\",\"year\": 2023,\"month\":"
            + " 4,\"seq\": 5}\n");
    client().performRequest(insertRequest);
    try {
      JSONObject actual =
          executeQuery(
              String.format(
                  "source=%s | sort seq | streamstats avg(age) as avg_age by state, country |"
                      + " streamstats avg(avg_age) as avg_state_age by country | fields name,"
                      + " country, state, month, year, age, avg_age, avg_state_age",
                  TEST_INDEX_STATE_COUNTRY_ORDERED));

      verifyDataRows(
          actual,
          rows("Jake", "USA", "California", 4, 2023, 70, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 30, 50),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20, 22.5),
          rows("Jay", "USA", null, 4, 2023, 28, 28, 42.666666666666664));

      JSONObject actual2 =
          executeQuery(
              String.format(
                  "source=%s | sort seq | streamstats bucket_nullable=false avg(age) as avg_age by"
                      + " state, country | streamstats bucket_nullable=false avg(avg_age) as"
                      + " avg_state_age by country | fields name, country, state, month, year, age,"
                      + " avg_age, avg_state_age",
                  TEST_INDEX_STATE_COUNTRY_ORDERED));

      verifyDataRows(
          actual2,
          rows("Jake", "USA", "California", 4, 2023, 70, 70, 70),
          rows("Hello", "USA", "New York", 4, 2023, 30, 30, 50),
          rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 25),
          rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20, 22.5),
          rows("Jay", "USA", null, 4, 2023, 28, null, 50));
    } finally {
      Request deleteRequest =
          new Request(
              "DELETE",
              String.format("/%s/_doc/%d?refresh=true", TEST_INDEX_STATE_COUNTRY_ORDERED, docId));
      client().performRequest(deleteRequest);
    }
  }

  @Test
  public void testStreamstatsAndEventstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | eventstats avg(age) as avg_age| streamstats"
                    + " avg(age) as avg_age_stream | fields name, country, state, month, year,"
                    + " age, avg_age, avg_age_stream",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 36.25, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, 36.25, 50),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 36.25, 41.666666666666664),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 36.25, 36.25));
  }

  @Test
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsAndSort() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort age | streamstats window = 2 avg(age) as avg_age | fields name,"
                    + " country, state, month, year, age, avg_age",
                TEST_INDEX_STATE_COUNTRY));

    verifyDataRows(
        actual,
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 20),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 22.5),
        rows("Hello", "USA", "New York", 4, 2023, 30, 27.5),
        rows("Jake", "USA", "California", 4, 2023, 70, 50));
  }

  @Test
  // The streamstats lives in the JOIN right-subsearch, whose avg_age depends on the subsearch
  // encounter order. Sourcing the seq-augmented fixture and adding `| sort seq` before streamstats
  // restores the single-shard encounter order across shards; gated to routes that honor a sort
  // before streamstats. The left source stays natural-order because verifyDataRows is
  // order-insensitive and the left order does not change the joined row set. Expected rows are
  // unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testLeftJoinWithStreamstats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s as l | left join left=l right=r on l.country = r.country [ source=%s |"
                    + " sort seq | streamstats window=2 avg(age) as avg_age] | fields l.name,"
                    + " l.country, l.state, l.month, l.year, l.age, r.name, r.country, r.state,"
                    + " r.month, r.year, r.age, avg_age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifyDataRows(
        actual,
        rows(
            "John", "Canada", "Ontario", 4, 2023, 25, "John", "Canada", "Ontario", 4, 2023, 25,
            27.5),
        rows(
            "John", "Canada", "Ontario", 4, 2023, 25, "Jane", "Canada", "Quebec", 4, 2023, 20,
            22.5),
        rows("John", "Canada", "Ontario", 4, 2023, 25, null, "Canada", null, 4, 2023, 10, 15),
        rows(
            "Jane", "Canada", "Quebec", 4, 2023, 20, "John", "Canada", "Ontario", 4, 2023, 25,
            27.5),
        rows(
            "Jane", "Canada", "Quebec", 4, 2023, 20, "Jane", "Canada", "Quebec", 4, 2023, 20, 22.5),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, null, "Canada", null, 4, 2023, 10, 15),
        rows(
            "Jake", "USA", "California", 4, 2023, 70, "Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Jake", "USA", "California", 4, 2023, 70, "Hello", "USA", "New York", 4, 2023, 30, 50),
        rows("Hello", "USA", "New York", 4, 2023, 30, "Jake", "USA", "California", 4, 2023, 70, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, "Hello", "USA", "New York", 4, 2023, 30, 50));
  }

  @Test
  public void testWhereInWithStreamstatsSubquery() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where country in [ source=%s | streamstats window=2 avg(age) as"
                    + " avg_age | where avg_age > 40 | fields country ] | fields name, country,"
                    + " state, month, year, age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70),
        rows("Hello", "USA", "New York", 4, 2023, 30));
  }

  @Test
  @RequiresCapability(CHAINED_STREAMSTATS_BY)
  public void testMultipleStreamstatsWithEval() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats avg(age) as avg_age by country,"
                    + " state, name | eval avg_age_divide_20 = avg_age - 20 | streamstats"
                    + " avg(avg_age_divide_20) as avg_state_age by country, state | where"
                    + " avg_state_age > 0 | streamstats count(avg_state_age) as"
                    + " count_country_age_greater_20 by country | fields name, country, state,"
                    + " month, year, age, avg_age, avg_age_divide_20, avg_state_age,"
                    + " count_country_age_greater_20",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 70, 50, 50, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 30, 10, 10, 2),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 25, 5, 5, 1));
  }

  @Test
  public void testMultipleStreamstatsWithEval2() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval new_state=lower(state), new_country=lower(country) | streamstats"
                    + " bucket_nullable=false avg(age) as avg_age by new_state, new_country |"
                    + " fields name, country, state, month, year, age, new_state, new_country,"
                    + " avg_age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("new_state", "string"),
        schema("new_country", "string"),
        schema("avg_age", "double"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, "california", "usa", 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, "new york", "usa", 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, "ontario", "canada", 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, "quebec", "canada", 20),
        rows(null, "Canada", null, 4, 2023, 10, null, "canada", null),
        rows("Kevin", null, null, 4, 2023, null, null, null, null));

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | eval new_state=lower(state), new_country=lower(country) | streamstats"
                    + " bucket_nullable=true avg(age) as avg_age by new_state, new_country |"
                    + " fields name, country, state, month, year, age, new_state, new_country,"
                    + " avg_age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifyDataRows(
        actual2,
        rows("Jake", "USA", "California", 4, 2023, 70, "california", "usa", 70),
        rows("Hello", "USA", "New York", 4, 2023, 30, "new york", "usa", 30),
        rows("John", "Canada", "Ontario", 4, 2023, 25, "ontario", "canada", 25),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, "quebec", "canada", 20),
        rows(null, "Canada", null, 4, 2023, 10, null, "canada", 10),
        rows("Kevin", null, null, 4, 2023, null, null, null, null));
  }

  @Test
  public void testStreamstatsEmptyRows() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name = 'non-existed' | streamstats count(), avg(age), min(age),"
                    + " max(age), stddev_pop(age), stddev_samp(age), var_pop(age), var_samp(age)",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifyNumOfRows(actual, 0);

    JSONObject actual2 =
        executeQuery(
            String.format(
                "source=%s | where name = 'non-existed' | streamstats count(), avg(age), min(age),"
                    + " max(age), stddev_pop(age), stddev_samp(age), var_pop(age), var_samp(age) by"
                    + " country",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));
    verifyNumOfRows(actual2, 0);
  }

  @Test
  public void testStreamstatsVariance() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats stddev_pop(age), stddev_samp(age),"
                    + " var_pop(age), var_samp(age) | fields name, country, state, month, year,"
                    + " age, `stddev_pop(age)`, `stddev_samp(age)`, `var_pop(age)`,"
                    + " `var_samp(age)`",
                SC));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("stddev_pop(age)", "double"),
        schema("stddev_samp(age)", "double"),
        schema("var_pop(age)", "double"),
        schema("var_samp(age)", "double"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 0, null, 0, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800),
        rows(
            "John",
            "Canada",
            "Ontario",
            4,
            2023,
            25,
            20.138409955990955,
            24.66441431158124,
            405.55555555555566,
            608.3333333333335),
        rows(
            "Jane",
            "Canada",
            "Quebec",
            4,
            2023,
            20,
            19.803724397193573,
            22.86737122335374,
            392.1875,
            522.9166666666666));
  }

  @Test
  // See testStreamstatsWithNull: `| sort seq` on the seq-augmented fixture restores the
  // single-shard encounter order across shards; gated to routes that honor a sort before
  // streamstats. Expected rows are unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsVarianceWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats stddev_pop(age), stddev_samp(age),"
                    + " var_pop(age), var_samp(age) | fields name, country, state, month, year,"
                    + " age, `stddev_pop(age)`, `stddev_samp(age)`, `var_pop(age)`,"
                    + " `var_samp(age)`",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("stddev_pop(age)", "double"),
        schema("stddev_samp(age)", "double"),
        schema("var_pop(age)", "double"),
        schema("var_samp(age)", "double"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 0, null, 0, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800),
        rows(
            "John",
            "Canada",
            "Ontario",
            4,
            2023,
            25,
            20.138409955990955,
            24.66441431158124,
            405.55555555555566,
            608.3333333333335),
        rows(
            "Jane",
            "Canada",
            "Quebec",
            4,
            2023,
            20,
            19.803724397193573,
            22.86737122335374,
            392.1875,
            522.9166666666666),
        rows(null, "Canada", null, 4, 2023, 10, 20.591260281974, 23.021728866442675, 424, 530),
        rows("Kevin", null, null, 4, 2023, null, 20.591260281974, 23.021728866442675, 424, 530));
  }

  @Test
  public void testStreamstatsVarianceBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats stddev_pop(age), stddev_samp(age),"
                    + " var_pop(age), var_samp(age) by country | fields name, country, state,"
                    + " month, year, age, `stddev_pop(age)`, `stddev_samp(age)`, `var_pop(age)`,"
                    + " `var_samp(age)`",
                SC));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 0, null, 0, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 0, null, 0, null),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2.5, 3.5355339059327378, 6.25, 12.5));
  }

  @Test
  public void testStreamstatsVarianceBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | where country != 'USA' | streamstats"
                    + " stddev_samp(age) by span(age, 10) | fields name, country, state, month,"
                    + " year, age, `stddev_samp(age)`",
                SC));

    verifyDataRows(
        actual,
        rows("John", "Canada", "Ontario", 4, 2023, 25, null),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 3.5355339059327378));
  }

  @Test
  // See testStreamstatsWithNull: `| sort seq` on the seq-augmented fixture restores the
  // single-shard encounter order across shards; gated to routes that honor a sort before
  // streamstats. Expected rows are unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsVarianceWithNullBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats stddev_pop(age), stddev_samp(age),"
                    + " var_pop(age), var_samp(age) by country | fields name, country, state,"
                    + " month, year, age, `stddev_pop(age)`, `stddev_samp(age)`, `var_pop(age)`,"
                    + " `var_samp(age)`",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 0, null, 0, null),
        rows("Hello", "USA", "New York", 4, 2023, 30, 20, 28.284271247461902, 400, 800),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 0, null, 0, null),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2.5, 3.5355339059327378, 6.25, 12.5),
        rows(
            null,
            "Canada",
            null,
            4,
            2023,
            10,
            6.2360956446232345,
            7.6376261582597325,
            38.88888888888888,
            58.333333333333314),
        rows("Kevin", null, null, 4, 2023, null, null, null, null, null));
  }

  @Test
  public void testStreamstatsDistinctCount() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats dc(state) as dc_state | fields"
                    + " name, country, state, month, year, age, dc_state",
                SC));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dc_state", "bigint"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4));
  }

  @Test
  public void testStreamstatsDistinctCountByCountry() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats dc(state) as dc_state by country |"
                    + " fields name, country, state, month, year, age, dc_state",
                SC));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dc_state", "bigint"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 1),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2));
  }

  @Test
  public void testStreamstatsDistinctCountFunction() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | streamstats distinct_count(country) as"
                    + " dc_country | fields name, country, state, month, year, age, dc_country",
                SC));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dc_country", "bigint"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 1),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 2),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 2));
  }

  @Test
  // See testStreamstatsWithNull: `| sort seq` on the seq-augmented fixture restores the
  // single-shard encounter order across shards; gated to routes that honor a sort before
  // streamstats. Expected rows are unchanged.
  @RequiresCapability(STREAMSTATS_SORT_NOT_HONORED)
  public void testStreamstatsDistinctCountWithNull() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort seq | streamstats dc(state) as dc_state | fields name, country,"
                    + " state, month, year, age, dc_state",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL_ORDERED));

    verifySchemaInOrder(
        actual,
        schema("name", "string"),
        schema("country", "string"),
        schema("state", "string"),
        schema("month", "int"),
        schema("year", "int"),
        schema("age", "int"),
        schema("dc_state", "bigint"));

    verifyDataRows(
        actual,
        rows("Jake", "USA", "California", 4, 2023, 70, 1),
        rows("Hello", "USA", "New York", 4, 2023, 30, 2),
        rows("John", "Canada", "Ontario", 4, 2023, 25, 3),
        rows("Jane", "Canada", "Quebec", 4, 2023, 20, 4),
        rows(null, "Canada", null, 4, 2023, 10, 4),
        rows("Kevin", null, null, 4, 2023, null, 4));
  }

  @Test
  public void testStreamstatsEarliestAndLatest() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "makeresults format=csv data='%s' | eval created_at=cast(created_at as timestamp),"
                    + " `@timestamp`=cast(`@timestamp` as timestamp) | streamstats"
                    + " earliest(message), latest(message) by server | fields created_at, server,"
                    + " `@timestamp`, message, level, `earliest(message)`, `latest(message)`",
                LOGS_STREAM));
    verifySchema(
        actual,
        schema("created_at", "timestamp"),
        schema("server", "string"),
        schema("@timestamp", "timestamp"),
        schema("message", "string"),
        schema("level", "string"),
        schema("earliest(message)", "string"),
        schema("latest(message)", "string"));
    verifyDataRows(
        actual,
        rows(
            "2023-01-05 00:00:00",
            "server1",
            "2023-01-01 00:00:00",
            "Database connection failed",
            "ERROR",
            "Database connection failed",
            "Database connection failed"),
        rows(
            "2023-01-04 00:00:00",
            "server2",
            "2023-01-02 00:00:00",
            "Service started",
            "INFO",
            "Service started",
            "Service started"),
        rows(
            "2023-01-03 00:00:00",
            "server1",
            "2023-01-03 00:00:00",
            "High memory usage",
            "WARN",
            "Database connection failed",
            "High memory usage"),
        rows(
            "2023-01-02 00:00:00",
            "server3",
            "2023-01-04 00:00:00",
            "Disk space low",
            "ERROR",
            "Disk space low",
            "Disk space low"),
        rows(
            "2023-01-01 00:00:00",
            "server2",
            "2023-01-05 00:00:00",
            "Backup completed",
            "INFO",
            "Service started",
            "Backup completed"));
  }

  @Test
  public void testStreamstatsEarliestLatestIndexMultiShardCoverage() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | streamstats earliest(message), latest(message) by server | fields"
                    + " server, message, `earliest(message)`, `latest(message)`",
                TEST_INDEX_LOGS));

    Map<String, Set<String>> messagesByServer =
        Map.of(
            "server1", Set.of("Database connection failed", "High memory usage"),
            "server2", Set.of("Service started", "Backup completed"),
            "server3", Set.of("Disk space low"));
    JSONArray rows = actual.getJSONArray("datarows");
    assertEquals(5, rows.length());
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      String server = row.getString(0);
      Set<String> validMessages = messagesByServer.get(server);
      assertNotNull("unexpected server: " + server, validMessages);
      assertTrue(validMessages.contains(row.getString(1)));
      assertTrue(validMessages.contains(row.getString(2)));
      assertTrue(validMessages.contains(row.getString(3)));
    }
  }

  @Test
  public void testStreamstatsIndexMultiShardCoverage() throws IOException {
    // Retains real multi-document, multi-shard index coverage for streamstats. streamstats' running
    // count() visits every document exactly once, so regardless of shard encounter order the cnt
    // column is always the multiset {1, 2, 3, 4} and the age column is the full fixture multiset.
    // These are order-independent properties, so they are stable across shard layouts.
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | streamstats count() as cnt | fields age, cnt",
                TEST_INDEX_STATE_COUNTRY));
    verifySchema(result, schema("age", "int"), schema("cnt", "bigint"));

    JSONArray datarows = result.getJSONArray("datarows");
    assertEquals(4, datarows.length());
    List<Integer> ages = new ArrayList<>();
    List<Integer> cnts = new ArrayList<>();
    for (int i = 0; i < datarows.length(); i++) {
      ages.add(datarows.getJSONArray(i).getInt(0));
      cnts.add(datarows.getJSONArray(i).getInt(1));
    }
    Collections.sort(ages);
    Collections.sort(cnts);
    assertEquals(List.of(20, 25, 30, 70), ages);
    assertEquals(List.of(1, 2, 3, 4), cnts);
  }
}
