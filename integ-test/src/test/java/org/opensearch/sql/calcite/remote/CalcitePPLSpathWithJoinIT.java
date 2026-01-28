/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLSpathWithJoinIT extends CalcitePPLSpathTestBase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    // Index with JSON field - requires spath to extract fields
    putJsonItem(
        1,
        "user1",
        sj("{'userId': 'u1', 'name': 'Alice', 'role': 'admin', 'notes': 'from_json_index'}"));
    putJsonItem(
        2,
        "user2",
        sj("{'userId': 'u2', 'name': 'Bob', 'role': 'user', 'notes': 'from_json_index'}"));
    putJsonItem(
        3,
        "user3",
        sj("{'userId': 'u3', 'name': 'Charlie', 'role': 'user', 'notes': 'from_json_index'}"));

    // Index without JSON field - regular structured data
    putRegularItem(1, "u1", "order1", 100.0, "from_regular_index");
    putRegularItem(2, "u1", "order2", 150.0, "from_regular_index");
    putRegularItem(3, "u2", "order3", 200.0, "from_regular_index");
    putRegularItem(4, "u3", "order4", 75.0, "from_regular_index");

    // Index with conflicting field values for overwrite testing
    putJsonItemConflict(
        1,
        "profile1",
        sj("{'userId': 'u1', 'department': 'Engineering', 'notes': 'from_conflict_index'}"));
    putJsonItemConflict(
        2,
        "profile2",
        sj("{'userId': 'u2', 'department': 'Sales', 'notes': 'from_conflict_index'}"));
  }

  @Test
  public void testSpathLeftJoinRegularRight() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_JSON
                + " | spath input=userData | fields userId, name | join userId [source="
                + INDEX_WITHOUT_JSON
                + " | fields userId, orderId, amount] | fields userId, name, orderId, amount");
    verifySchema(
        result,
        schema("userId", "string"),
        schema("name", "string"),
        schema("orderId", "string"),
        schema("amount", "float"));
    verifyDataRows(
        result,
        rows("u1", "Alice", "order1", 100.0),
        rows("u1", "Alice", "order2", 150.0),
        rows("u2", "Bob", "order3", 200.0),
        rows("u3", "Charlie", "order4", 75.0));
  }

  @Test
  public void testRegularLeftJoinSpathRight() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_WITHOUT_JSON
                + " | fields userId, orderId, amount | join userId [source="
                + INDEX_JSON
                + " | spath input=userData | fields userId, name, role] | fields userId, orderId,"
                + " amount, name, role");
    verifySchema(
        result,
        schema("userId", "string"),
        schema("orderId", "string"),
        schema("amount", "float"),
        schema("name", "string"),
        schema("role", "string"));
    verifyDataRows(
        result,
        rows("u1", "order1", 100.0, "Alice", "admin"),
        rows("u1", "order2", 150.0, "Alice", "admin"),
        rows("u2", "order3", 200.0, "Bob", "user"),
        rows("u3", "order4", 75.0, "Charlie", "user"));
  }

  @Test
  public void testSpathBothSidesJoin() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_JSON
                + " | where category='user1' | spath input=userData | fields userId, name | join"
                + " userId [source="
                + INDEX_JSON
                + " | where category='user1' | spath input=userData | fields userId,"
                + " role] | fields userId, name, role");
    verifySchema(
        result, schema("userId", "string"), schema("name", "string"), schema("role", "string"));
    verifyDataRows(result, rows("u1", "Alice", "admin"));
  }

  @Test
  public void testSpathLeftJoinWithDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_JSON
                + " | spath input=userData | join userId [source="
                + INDEX_WITHOUT_JSON
                + " | fields userId, orderId, amount] | fields userId, name, *");
    verifySchema(
        result,
        schema("userId", "string"),
        schema("name", "string"),
        schema("amount", "string"),
        schema("category", "string"),
        schema("notes", "string"),
        schema("orderId", "string"),
        schema("role", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows(
            "u1",
            "Alice",
            "100.0",
            "user1",
            "from_json_index",
            "order1",
            "admin",
            sj("{'userId': 'u1', 'name': 'Alice', 'role': 'admin', 'notes': 'from_json_index'}")),
        rows(
            "u1",
            "Alice",
            "150.0",
            "user1",
            "from_json_index",
            "order2",
            "admin",
            sj("{'userId': 'u1', 'name': 'Alice', 'role': 'admin', 'notes': 'from_json_index'}")),
        rows(
            "u2",
            "Bob",
            "200.0",
            "user2",
            "from_json_index",
            "order3",
            "user",
            sj("{'userId': 'u2', 'name': 'Bob', 'role': 'user', 'notes': 'from_json_index'}")),
        rows(
            "u3",
            "Charlie",
            "75.0",
            "user3",
            "from_json_index",
            "order4",
            "user",
            sj("{'userId': 'u3', 'name': 'Charlie', 'role': 'user', 'notes': 'from_json_index'}")));
  }

  @Test
  public void testSpathRightJoinWithDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_WITHOUT_JSON
                + " | fields userId, orderId, amount | join userId [source="
                + INDEX_JSON
                + " | spath input=userData] | fields userId, name, *");
    verifySchema(
        result,
        schema("userId", "string"),
        schema("name", "string"),
        schema("amount", "string"),
        schema("category", "string"),
        schema("notes", "string"),
        schema("orderId", "string"),
        schema("role", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows(
            "u1",
            "Alice",
            "100.0",
            "user1",
            "from_json_index",
            "order1",
            "admin",
            sj("{'userId': 'u1', 'name': 'Alice', 'role': 'admin', 'notes': 'from_json_index'}")),
        rows(
            "u1",
            "Alice",
            "150.0",
            "user1",
            "from_json_index",
            "order2",
            "admin",
            sj("{'userId': 'u1', 'name': 'Alice', 'role': 'admin', 'notes': 'from_json_index'}")),
        rows(
            "u2",
            "Bob",
            "200.0",
            "user2",
            "from_json_index",
            "order3",
            "user",
            sj("{'userId': 'u2', 'name': 'Bob', 'role': 'user', 'notes': 'from_json_index'}")),
        rows(
            "u3",
            "Charlie",
            "75.0",
            "user3",
            "from_json_index",
            "order4",
            "user",
            sj("{'userId': 'u3', 'name': 'Charlie', 'role': 'user', 'notes': 'from_json_index'}")));
  }

  @Test
  public void testSpathBothSidesJoinWithDynamicFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_JSON
                + " | where category='user1' | spath input=userData | join userId [source="
                + INDEX_JSON
                + " | where category='user1' | spath input=userData] | fields userId,"
                + " name, role, *");
    verifySchema(
        result,
        schema("userId", "string"),
        schema("name", "string"),
        schema("role", "string"),
        schema("category", "string"),
        schema("notes", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows(
            "u1",
            "Alice",
            "admin",
            "user1",
            "from_json_index",
            sj("{'userId': 'u1', 'name': 'Alice', 'role': 'admin', 'notes': 'from_json_index'}")));
  }

  @Test
  public void testSpathJoinWithOverwriteTrue() throws IOException {
    // Default behavior (overwrite=true): right side values overwrite left side
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_JSON
                + " | spath input=userData | fields userId, name, role, notes | join"
                + " userId [source="
                + INDEX_JSON_CONFLICT
                + " | spath input=userData | fields userId, notes, department] | fields userId,"
                + " name, role, notes, department | head 1");
    verifySchema(
        result,
        schema("userId", "string"),
        schema("name", "string"),
        schema("role", "string"),
        schema("notes", "string"),
        schema("department", "string"));
    // Right side notes value should overwrite left side - notes should be from_conflict_index
    verifyDataRows(result, rows("u1", "Alice", "admin", "from_conflict_index", "Engineering"));
  }

  @Test
  public void testSpathJoinWithOverwriteFalse() throws IOException {
    // overwrite=false: left side values are preserved when there's a conflict
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_JSON
                + " | spath input=userData | fields userId, name, role, notes | join"
                + " overwrite=false userId [source="
                + INDEX_JSON_CONFLICT
                + " | spath input=userData | fields userId, notes, department] | fields userId,"
                + " name, role, notes, department | head 1");
    verifySchema(
        result,
        schema("userId", "string"),
        schema("name", "string"),
        schema("role", "string"),
        schema("notes", "string"),
        schema("department", "string"));
    // Left side notes value should be preserved - notes should be from_json_index
    verifyDataRows(result, rows("u1", "Alice", "admin", "from_json_index", "Engineering"));
  }

  @Test
  public void testSpathJoinWithCriteria() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_JSON
                + " | spath input=userData | join left=l right=r on"
                + " l.userId = r.userId [source="
                + INDEX_WITHOUT_JSON
                + " ] | fields userId, r.userId, * | head 1");
    verifySchema(
        result,
        schema("userId", "string"),
        schema("r.userId", "string"),
        schema("amount", "string"),
        schema("category", "string"),
        schema("name", "string"),
        schema("notes", "string"),
        schema("orderId", "string"),
        schema("role", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows(
            "u1",
            "u1",
            "100.0",
            "user1",
            "Alice",
            "from_json_index",
            "order1",
            "admin",
            sj("{'userId': 'u1', 'name': 'Alice', 'role': 'admin', 'notes': 'from_json_index'}")));
  }

  @Test
  public void testSpathJoinWithAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_JSON
                + " | spath input=userData | join userId [source="
                + INDEX_WITHOUT_JSON
                + " ] | stats sum(amount) as totalAmount by userId, name");
    verifySchema(
        result,
        schema("totalAmount", "double"),
        schema("userId", "string"),
        schema("name", "string"));
    verifyDataRows(
        result, rows(250.0, "u1", "Alice"), rows(200.0, "u2", "Bob"), rows(75.0, "u3", "Charlie"));
  }

  @Test
  public void testSpathJoinWithFilter() throws IOException {
    JSONObject result =
        executeQuery(
            "source="
                + INDEX_JSON
                + " | spath input=userData | join userId [source="
                + INDEX_WITHOUT_JSON
                + " | where amount > 100] | fields userId, name, *");
    verifySchema(
        result,
        schema("userId", "string"),
        schema("name", "string"),
        schema("amount", "string"),
        schema("category", "string"),
        schema("notes", "string"),
        schema("orderId", "string"),
        schema("role", "string"),
        schema("userData", "string"));
    verifyDataRows(
        result,
        rows(
            "u1",
            "Alice",
            "150.0",
            "user1",
            "from_regular_index",
            "order2",
            "admin",
            sj("{'userId': 'u1', 'name': 'Alice', 'role': 'admin', 'notes': 'from_json_index'}")),
        rows(
            "u2",
            "Bob",
            "200.0",
            "user2",
            "from_regular_index",
            "order3",
            "user",
            sj("{'userId': 'u2', 'name': 'Bob', 'role': 'user', 'notes': 'from_json_index'}")));
  }
}
