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

package org.opensearch.sql.legacy.query.planner.core;

import static org.opensearch.sql.legacy.query.planner.core.ExecuteParams.ExecuteParamType.CLIENT;
import static org.opensearch.sql.legacy.query.planner.core.ExecuteParams.ExecuteParamType.RESOURCE_MANAGER;
import static org.opensearch.sql.legacy.query.planner.core.ExecuteParams.ExecuteParamType.TIMEOUT;

import java.util.List;
import org.opensearch.client.Client;
import org.opensearch.search.SearchHit;
import org.opensearch.sql.legacy.executor.join.MetaSearchResult;
import org.opensearch.sql.legacy.query.planner.explain.Explanation;
import org.opensearch.sql.legacy.query.planner.explain.JsonExplanationFormat;
import org.opensearch.sql.legacy.query.planner.logical.LogicalPlan;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalPlan;
import org.opensearch.sql.legacy.query.planner.resource.ResourceManager;
import org.opensearch.sql.legacy.query.planner.resource.Stats;

/**
 * Query planner that driver the logical planning, physical planning, execute and explain.
 */
public class QueryPlanner {

    /**
     * Connection to ElasticSearch
     */
    private final Client client;

    /**
     * Query plan configuration
     */
    private final Config config;

    /**
     * Optimized logical plan
     */
    private final LogicalPlan logicalPlan;

    /**
     * Best physical plan to execute
     */
    private final PhysicalPlan physicalPlan;

    /**
     * Statistics collector
     */
    private Stats stats;

    /**
     * Resource monitor and statistics manager
     */
    private ResourceManager resourceMgr;


    public QueryPlanner(Client client, Config config, QueryParams params) {
        this.client = client;
        this.config = config;
        this.stats = new Stats(client);
        this.resourceMgr = new ResourceManager(stats, config);

        logicalPlan = new LogicalPlan(config, params);
        logicalPlan.optimize();

        physicalPlan = new PhysicalPlan(logicalPlan);
        physicalPlan.optimize();
    }

    /**
     * Execute query plan
     *
     * @return response of the execution
     */
    public List<SearchHit> execute() {
        ExecuteParams params = new ExecuteParams();
        params.add(CLIENT, client);
        params.add(TIMEOUT, config.timeout());
        params.add(RESOURCE_MANAGER, resourceMgr);
        return physicalPlan.execute(params);
    }

    /**
     * Explain query plan
     *
     * @return explanation string of the plan
     */
    public String explain() {
        return new Explanation(
                logicalPlan, physicalPlan,
                new JsonExplanationFormat(4)
        ).toString();
    }

    public MetaSearchResult getMetaResult() {
        return resourceMgr.getMetaResult();
    }

    /**
     * Setter for unit test
     */
    public void setStats(Stats stats) {
        this.stats = stats;
        this.resourceMgr = new ResourceManager(stats, config);
    }
}
