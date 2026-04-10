/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.functions;

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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.NonNull;
import org.opensearch.geospatial.action.IpEnrichmentActionClient;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;
import org.opensearch.transport.client.node.NodeClient;

/**
 * {@code GEOIP(dataSourceName, ipAddress[, options])} looks up location information from given IP
 * addresses via OpenSearch GeoSpatial plugin API. The options is a comma-separated list of fields
 * to be returned. If not specified, all fields are returned.
 *
 * <p>Signatures:
 *
 * <ul>
 *   <li>(STRING, IP) -> MAP
 *   <li>(STRING, IP, STRING) -> MAP
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

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(
        new SqlOperandTypeChecker() {
          @Override
          public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!getOperandCountRange().isValidCount(callBinding.getOperandCount())) {
              return false;
            }
            boolean valid =
                OpenSearchTypeUtil.isCharacter(callBinding.getOperandType(0))
                    && OpenSearchTypeUtil.isIp(callBinding.getOperandType(1), true);
            if (callBinding.getOperandCount() == 3) {
              valid = valid && OpenSearchTypeUtil.isCharacter(callBinding.getOperandType(2));
            }
            return valid;
          }

          @Override
          public SqlOperandCountRange getOperandCountRange() {
            return SqlOperandCountRanges.between(2, 3);
          }

          @Override
          public String getAllowedSignatures(SqlOperator op, String opName) {
            return "GEOIP(<STRING>, <IP>), GEOIP(<STRING>, <IP>, <STRING>)";
          }
        });
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

    public static Map<String, ?> fetchIpEnrichment(
        String dataSource, ExprIpValue ipAddress, NodeClient nodeClient) {
      return fetchIpEnrichment(
          dataSource, ipAddress.toString(), Collections.emptySet(), nodeClient);
    }

    public static Map<String, ?> fetchIpEnrichment(
        String dataSource, Object ipAddress, NodeClient nodeClient) {
      return fetchIpEnrichment(
          dataSource, ipAddress.toString(), Collections.emptySet(), nodeClient);
    }

    public static Map<String, ?> fetchIpEnrichment(
        String dataSource,
        ExprIpValue ipAddress,
        String commaSeparatedOptions,
        NodeClient nodeClient) {
      return fetchIpEnrichment(dataSource, ipAddress.toString(), commaSeparatedOptions, nodeClient);
    }

    public static Map<String, ?> fetchIpEnrichment(
        String dataSource, String ipAddress, String commaSeparatedOptions, NodeClient nodeClient) {
      String unquotedOptions = StringUtils.unquoteText(commaSeparatedOptions);
      final Set<String> options =
          Arrays.stream(unquotedOptions.split(",")).map(String::trim).collect(Collectors.toSet());
      return fetchIpEnrichment(dataSource, ipAddress, options, nodeClient);
    }

    private static Map<String, ?> fetchIpEnrichment(
        String dataSource, String ipAddress, Set<String> options, NodeClient nodeClient) {
      IpEnrichmentActionClient ipClient = new IpEnrichmentActionClient(nodeClient);
      dataSource = StringUtils.unquoteText(dataSource);
      try {
        Map<String, Object> geoLocationData = ipClient.getGeoLocationData(ipAddress, dataSource);
        Map<String, ExprValue> enrichmentResult =
            geoLocationData.entrySet().stream()
                .filter(entry -> options.isEmpty() || options.contains(entry.getKey()))
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, v -> new ExprStringValue(v.getValue().toString())));
        @SuppressWarnings("unchecked")
        Map<String, ?> result =
            (Map<String, ?>) ExprTupleValue.fromExprValueMap(enrichmentResult).valueForCalcite();
        return result;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
