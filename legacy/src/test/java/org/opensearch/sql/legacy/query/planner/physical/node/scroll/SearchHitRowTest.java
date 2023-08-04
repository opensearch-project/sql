/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical.node.scroll;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.query.planner.physical.Row.RowKey;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.search.SearchHit;

public class SearchHitRowTest {

  @Test
  public void testKeyWithObjectField() {
    SearchHit hit = new SearchHit(1);
    hit.sourceRef(new BytesArray("{\"id\": {\"serial\": 3}}"));
    SearchHitRow row = new SearchHitRow(hit, "a");
    RowKey key = row.key(new String[]{"id.serial"});

    Object[] data = key.keys();
    assertEquals(1, data.length);
    assertEquals(3, data[0]);
  }

  @Test
  public void testKeyWithUnexpandedObjectField() {
    SearchHit hit = new SearchHit(1);
    hit.sourceRef(new BytesArray("{\"attributes.hardware.correlate_id\": 10}"));
    SearchHitRow row = new SearchHitRow(hit, "a");
    RowKey key = row.key(new String[]{"attributes.hardware.correlate_id"});

    Object[] data = key.keys();
    assertEquals(1, data.length);
    assertEquals(10, data[0]);
  }

  @Test
  public void testRetainWithObjectField() {
    SearchHit hit = new SearchHit(1);
    hit.sourceRef(new BytesArray("{\"a.id\": {\"serial\": 3}}"));
    SearchHitRow row = new SearchHitRow(hit, "");
    row.retain(ImmutableMap.of("a.id.serial", ""));

    SearchHit expected = new SearchHit(1);
    expected.sourceRef(new BytesArray("{\"a.id\": {\"serial\": 3}}"));
    assertEquals(expected, row.data());
  }

  @Test
  public void testRetainWithUnexpandedObjectField() {
    SearchHit hit = new SearchHit(1);
    hit.sourceRef(new BytesArray("{\"a.attributes.hardware.correlate_id\": 10}"));
    SearchHitRow row = new SearchHitRow(hit, "");
    row.retain(ImmutableMap.of("a.attributes.hardware.correlate_id", ""));

    SearchHit expected = new SearchHit(1);
    expected.sourceRef(new BytesArray("{\"a.attributes.hardware.correlate_id\": 10}"));
    assertEquals(expected, row.data());
  }
}
