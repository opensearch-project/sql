/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.metrics.BasicCounter;
import org.opensearch.sql.legacy.metrics.Metric;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.metrics.NumericMetric;

public class MetricsTest {

  @Test
  public void registerMetric() {
    Metrics.getInstance().clear();
    Metrics.getInstance().registerMetric(new NumericMetric("test", new BasicCounter()));

    assertThat(Metrics.getInstance().getAllMetrics().size(), equalTo(1));
  }

  @Test
  public void unRegisterMetric() {
    Metrics.getInstance().clear();
    Metrics.getInstance().registerMetric(new NumericMetric("test1", new BasicCounter()));
    Metrics.getInstance().registerMetric(new NumericMetric("test2", new BasicCounter()));
    assertThat(Metrics.getInstance().getAllMetrics().size(), equalTo(2));

    Metrics.getInstance().unregisterMetric("test2");
    assertThat(Metrics.getInstance().getAllMetrics().size(), equalTo(1));
  }

  @Test
  public void getMetric() {
    Metrics.getInstance().clear();
    Metrics.getInstance().registerMetric(new NumericMetric("test1", new BasicCounter()));
    Metric metric = Metrics.getInstance().getMetric("test1");

    assertThat(metric, notNullValue());
  }

  @Test
  public void getAllMetric() {
    Metrics.getInstance().clear();
    Metrics.getInstance().registerMetric(new NumericMetric("test1", new BasicCounter()));
    Metrics.getInstance().registerMetric(new NumericMetric("test2", new BasicCounter()));
    List list = Metrics.getInstance().getAllMetrics();

    assertThat(list.size(), equalTo(2));
  }

  @Test
  public void collectToJSON() {
    Metrics.getInstance().clear();
    Metrics.getInstance().registerMetric(new NumericMetric("test1", new BasicCounter()));
    Metrics.getInstance().registerMetric(new NumericMetric("test2", new BasicCounter()));
    String res = Metrics.getInstance().collectToJSON();
    JSONObject jsonObject = new JSONObject(res);

    assertThat(jsonObject.getLong("test1"), equalTo(0L));
    assertThat(jsonObject.getInt("test2"), equalTo(0));
  }
}
