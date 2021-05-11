/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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
