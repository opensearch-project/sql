/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.opensearch.planner.physical;

import org.opensearch.client.node.NodeClient;
import org.opensearch.geospatial.action.IpEnrichmentActionClient;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.OpenSearchFunctions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class OpenSearchEvalProcessor {

    public static ExprValue process (OpenSearchFunctions.OpenSearchFunction funcExpression, Environment<Expression, ExprValue> env, NodeClient nodeClient) {

        if (BuiltinFunctionName.GEOIP.getName().equals(funcExpression.getFunctionName())) {
            // Rewrite to encapsulate the try catch.
            return fetchIpEnrichment(funcExpression.getArguments(), env, nodeClient);
        } else {
            return null;
        }

    }

    private static ExprValue fetchIpEnrichment(
            List<Expression> arguments, Environment<Expression, ExprValue> env, NodeClient nodeClient) {
        final Set<String> PERMITTED_OPTIONS =
                Set.of(
                        "country_iso_code",
                        "country_name",
                        "continent_name",
                        "region_iso_code",
                        "region_name",
                        "city_name",
                        "time_zone",
                        "location");
        IpEnrichmentActionClient ipClient = new IpEnrichmentActionClient(nodeClient);
        String dataSource = StringUtils.unquoteText(arguments.get(0).toString());
        String ipAddress = arguments.get(1).valueOf(env).stringValue();
        final Set<String> options = new HashSet<>();
        if (arguments.size() > 2) {
            String option = StringUtils.unquoteText(arguments.get(2).toString());
            // Convert the option into a set.
            options.addAll(
                    Arrays.stream(option.split(","))
                            .filter(PERMITTED_OPTIONS::contains)
                            .collect(Collectors.toSet()));
        }
        try {
            Map<String, Object> geoLocationData = ipClient.getGeoLocationData(ipAddress, dataSource);
            Map<String, ExprValue> enrichmentResult =
                    geoLocationData.entrySet().stream()
                            .filter(entry -> options.isEmpty() || options.contains(entry.getKey()))
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey, v -> new ExprStringValue(v.getValue().toString())));
            return ExprTupleValue.fromExprValueMap(enrichmentResult);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
