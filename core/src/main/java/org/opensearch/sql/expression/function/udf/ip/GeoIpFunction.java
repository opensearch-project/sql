/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.ip;

import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.geospatial.action.IpEnrichmentActionClient;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.transport.client.node.NodeClient;

/**
 * {@code GEOIP(dataSourceName, ipAddress[, options])} looks up location information from given IP
 * addresses via OpenSearch GeoSpatial plugin API. The options is a comma-separated list of fields
 * to be returned. If not specified, all fields are returned.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>(STRING, STRING) -> MAP
 *   <li>(STRING, STRING, STRING) -> MAP
 * </ul>
 */
public class GeoIpFunction extends ImplementorUDF {
  public GeoIpFunction(NodeClient nodeClient) {
    super(new GeoIPImplementor(nodeClient), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return op -> {
      RelDataTypeFactory typeFactory = op.getTypeFactory();
      RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
      return typeFactory.createMapType(varcharType, anyType);
    };
  }

  public static class GeoIPImplementor implements NotNullImplementor {
    @Getter private static NodeClient nodeClient;

    public GeoIPImplementor(NodeClient nodeClient) {
      GeoIPImplementor.nodeClient = nodeClient;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (getNodeClient() == null) {
        throw new IllegalStateException("nodeClient is null.");
      }
      List<Expression> operandsWithClient = new ArrayList<>(translatedOperands);
      // Since a NodeClient cannot be passed as a parameter using Expressions.constant,
      // it is instead provided through a function call.
      operandsWithClient.add(Expressions.call(GeoIPImplementor.class, "getNodeClient"));
      return Expressions.call(GeoIPImplementor.class, "fetchIpEnrichment", operandsWithClient);
    }

    public static Object fetchIpEnrichment(
        String dataSource, String ipAddress, NodeClient nodeClient) {
      return fetchIpEnrichment(dataSource, ipAddress, "", nodeClient);
    }

    public static Object fetchIpEnrichment(
        String dataSource, String ipAddress, String commaSeparatedOptions, NodeClient nodeClient) {
      IpEnrichmentActionClient ipClient = new IpEnrichmentActionClient(nodeClient);
      dataSource = StringUtils.unquoteText(dataSource);
      String option = StringUtils.unquoteText(commaSeparatedOptions);
      // Convert the option into a set.
      final Set<String> options = new HashSet<>();
      if (!commaSeparatedOptions.isEmpty()) {
        options.addAll(
            Arrays.stream(option.split(",")).map(String::trim).collect(Collectors.toSet()));
      }

      try {
        Map<String, Object> geoLocationData = ipClient.getGeoLocationData(ipAddress, dataSource);
        Map<String, ExprValue> enrichmentResult =
            geoLocationData.entrySet().stream()
                .filter(entry -> options.isEmpty() || options.contains(entry.getKey()))
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, v -> new ExprStringValue(v.getValue().toString())));
        return ExprTupleValue.fromExprValueMap(enrichmentResult).valueForCalcite();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
