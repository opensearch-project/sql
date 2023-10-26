package org.opensearch.sql.spark.functions.response;

import org.json.JSONArray;
import org.json.JSONObject;

/** visitor for the generic Json Object arriving from the flint's result payload */
public interface JsonVisitor<T> {

  T visitObject(JSONObject jsonObject);

  T visitArray(JSONArray jsonArray);

  T visitPrimitive(Object primitive);
}
