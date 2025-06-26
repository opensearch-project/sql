/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.JsonBuilder;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

@Getter
public class RelJsonSerializer {

  private final RelOptCluster cluster;

  public static final String EXPR = "expr";
  public static final String FIELD_TYPES = "fieldTypes";
  public static final String ROW_TYPE = "rowType";
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final TypeReference<LinkedHashMap<String, Object>> TYPE_REF =
      new TypeReference<>() {};
  private static final SqlOperatorTable pplSqlOperatorTable =
      SqlOperatorTables.chain(
          PPLBuiltinOperators.instance(),
          SqlStdOperatorTable.instance(),
          // Add a list of necessary SqlLibrary if needed
          SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
              SqlLibrary.MYSQL, SqlLibrary.BIG_QUERY, SqlLibrary.SPARK, SqlLibrary.POSTGRESQL));

  static {
    mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
  }

  public RelJsonSerializer(RelOptCluster cluster) {
    this.cluster = cluster;
  }

  public String serialize(
      RexNode rexNode, RelDataType relDataType, Map<String, ExprType> fieldTypes) {
    try {
      // Serialize RexNode and RelDataType by JSON
      JsonBuilder jsonBuilder = new JsonBuilder();
      RelJson relJson = RelJson.create().withJsonBuilder(jsonBuilder);
      String rexNodeJson = jsonBuilder.toJsonString(relJson.toJson(rexNode));
      String rowTypeJson = jsonBuilder.toJsonString(relJson.toJson(relDataType));
      // Construct envelope of serializable objects
      Map<String, Object> envelope =
          Map.of(EXPR, rexNodeJson, FIELD_TYPES, fieldTypes, ROW_TYPE, rowTypeJson);

      // Write bytes of all serializable contents
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(envelope);
      objectOutput.flush();
      return Base64.getEncoder().encodeToString(output.toByteArray());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to serialize RexNode: " + rexNode, e);
    }
  }

  public Map<String, Object> deserialize(String struct) {
    try {
      // Recover Map object from bytes
      ByteArrayInputStream input = new ByteArrayInputStream(Base64.getDecoder().decode(struct));
      ObjectInputStream objectInput = new ObjectInputStream(input);
      Map<String, Object> objectMap = (Map<String, Object>) objectInput.readObject();

      // PPL Expr types are all serializable
      Map<String, ExprType> fieldTypes = (Map<String, ExprType>) objectMap.get(FIELD_TYPES);
      // Deserialize RelDataType and RexNode by JSON
      RelJson relJson = RelJson.create();
      Map<String, Object> rowTypeMap = mapper.readValue((String) objectMap.get(ROW_TYPE), TYPE_REF);
      RelDataType rowType = relJson.toType(cluster.getTypeFactory(), rowTypeMap);
      OpenSearchRelInputTranslator inputTranslator = new OpenSearchRelInputTranslator(rowType);
      relJson = relJson.withInputTranslator(inputTranslator).withOperatorTable(pplSqlOperatorTable);
      Map<String, Object> exprMap = mapper.readValue((String) objectMap.get(EXPR), TYPE_REF);
      RexNode rexNode = relJson.toRex(cluster, exprMap);

      return Map.of(EXPR, rexNode, FIELD_TYPES, fieldTypes, ROW_TYPE, rowType);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to deserialize RexNode and its required structure: " + struct, e);
    }
  }
}
