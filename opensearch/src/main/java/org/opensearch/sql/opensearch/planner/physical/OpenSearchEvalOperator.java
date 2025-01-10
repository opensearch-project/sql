/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.opensearch.planner.physical;

import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.expression.env.Environment.extendEnv;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.client.node.NodeClient;
import org.opensearch.geospatial.action.IpEnrichmentActionClient;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.ip.OpenSearchFunctionExpression;
import org.opensearch.sql.planner.physical.EvalOperator;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * OpenSearch version of eval operator, which contains nodeClient, in order to perform OpenSearch
 * related operation during the eval process.
 */
public class OpenSearchEvalOperator extends EvalOperator {

  @Getter private final NodeClient nodeClient;

  public OpenSearchEvalOperator(
      PhysicalPlan input,
      List<Pair<ReferenceExpression, Expression>> expressionList,
      NodeClient nodeClient) {
    super(input, expressionList);
    this.nodeClient = nodeClient;
  }

  @Override
  public ExprValue next() {
    ExprValue inputValue = this.getInput().next();
    Map<String, ExprValue> evalMap = eval(inputValue.bindingTuples());
    if (STRUCT == inputValue.type()) {
      ImmutableMap.Builder<String, ExprValue> resultBuilder = new ImmutableMap.Builder<>();
      Map<String, ExprValue> tupleValue = ExprValueUtils.getTupleValue(inputValue);
      for (Map.Entry<String, ExprValue> valueEntry : tupleValue.entrySet()) {
        if (evalMap.containsKey(valueEntry.getKey())) {
          resultBuilder.put(valueEntry.getKey(), evalMap.get(valueEntry.getKey()));
          evalMap.remove(valueEntry.getKey());
        } else {
          resultBuilder.put(valueEntry);
        }
      }
      resultBuilder.putAll(evalMap);
      return ExprTupleValue.fromExprValueMap(resultBuilder.build());
    } else {
      return inputValue;
    }
  }

  /**
   * Evaluate the expression in the {@link EvalOperator} with {@link Environment}.
   *
   * @param env {@link Environment}
   * @return The mapping of reference and {@link ExprValue} for each expression.
   */
  private Map<String, ExprValue> eval(Environment<Expression, ExprValue> env) {
    Map<String, ExprValue> evalResultMap = new LinkedHashMap<>();
    for (Pair<ReferenceExpression, Expression> pair : this.getExpressionList()) {
      ReferenceExpression var = pair.getKey();
      Expression valueExpr = pair.getValue();
      ExprValue value;
      if (valueExpr instanceof OpenSearchFunctionExpression openSearchFuncExpression) {
        if ("geoip".equals(openSearchFuncExpression.getFunctionName().getFunctionName())) {
          // Rewrite to encapsulate the try catch.
          value = fetchIpEnrichment(openSearchFuncExpression.getArguments(), env);
        } else {
          return null;
        }
      } else {
        value = pair.getValue().valueOf(env);
      }
      env = extendEnv(env, var, value);
      evalResultMap.put(var.toString(), value);
    }
    return evalResultMap;
  }

  private ExprValue fetchIpEnrichment(
      List<Expression> arguments, Environment<Expression, ExprValue> env) {
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
