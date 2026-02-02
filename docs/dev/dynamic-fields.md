# Dynamic Fields in PPL

## Overview

Dynamic fields enable schema-on-read capabilities in PPL, allowing queries to work with fields that aren't known at query planning time.
The key idea is to map fields that are directly referred in the query to static fields, and others to dynamic fields.
And most of the command implementation only consider static fields, while very few commands such as join need to handle dynamic fields.

**Key Concepts:**
- **Field Resolution**: Analyzes query and determines which fields need to be mapped to static/dynamic fields.
- **Static Fields**: Known fields extracted as individual columns
- **Dynamic Fields**: Unknown/wildcard-matched fields stored in `_MAP` field
- **`_MAP` Field**: Special field containing all unmapped fields as a map

## Architecture

### Components

1. **FieldResolutionVisitor** (`core/src/main/java/org/opensearch/sql/ast/analysis/FieldResolutionVisitor.java`)
   - Analyzes PPL AST to determine field requirements
   - Propagates field requirements through query plan
   - Returns `FieldResolutionResult` for each node

2. **FieldResolutionResult** (`core/src/main/java/org/opensearch/sql/ast/analysis/FieldResolutionResult.java`)
   - Contains regular fields (static, known fields)
   - Contains wildcard patterns (e.g., `prefix*`)
   - Provides methods to combine/modify field requirements

3. **CalciteRelNodeVisitor** (`core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`)
   - Converts AST to Calcite RelNode
   - Uses field resolution results to build execution plan
   - Handles `_MAP` field creation/modification for dynamic fields

4. **DynamicFieldsHelper** (`core/src/main/java/org/opensearch/sql/calcite/DynamicFieldsHelper.java`)
   - Utility methods for dynamic field handling
   - Manages `_MAP` field operations
   - Handles field projection and merging

5. **DynamicFieldsResultProcessor** (`core/src/main/java/org/opensearch/sql/calcite/utils/DynamicFieldsResultProcessor.java`)
   - Expands `_MAP` field into individual columns before returning results to client
   - Collects all dynamic field names across result rows
   - Converts dynamic field values to STRING type for consistency
   - Creates expanded schema with both static and dynamic fields

## How Dynamic Fields Work

### Field Resolution Process

```
Query: source=logs | spath input=data | fields a, b, prefix*

1. FieldResolutionVisitor analyzes query:
   - Visits nodes bottom-up
   - Collects field requirements at each stage
   - Result: {regularFields: {a, b}, wildcard: prefix*}

2. CalciteRelNodeVisitor builds execution plan:
   - Extracts JSON: JSON_EXTRACT_ALL(data) → fullMap
   - Projects static fields: a, b
   - Creates _MAP: MAP_REMOVE(fullMap, ARRAY['a', 'b'])

3. Runtime execution:
   - Static fields become individual columns
   - _MAP contains all other fields as map
```

### Field Resolution Integration

Field resolution works through a stack-based traversal:

```java
// Example from FieldResolutionVisitor
@Override
public Node visitProject(Project node, FieldResolutionContext context) {
  // Get current requirements from downstream
  FieldResolutionResult current = context.getCurrentRequirements();
  
  // Analyze project fields
  Set<String> projectFields = extractFields(node.getProjectList());
  
  // Combine with current requirements
  context.pushRequirements(current.and(projectFields));
  
  // Visit children with updated requirements
  visitChildren(node, context);
  
  context.popRequirements();
  return node;
}
```

### `_MAP` Field Creation

When wildcards are present, the `_MAP` field stores unmapped attributes:

```java
// From CalciteRelNodeVisitor.spathExtractAll()
if (resolutionResult.hasWildcards()) {
  // Build array of static field names to remove
  RexNode keyArray = buildKeyArray(regularFields, context);
  
  // Create _MAP = MAP_REMOVE(fullMap, keyArray)
  RexNode dynamicMapField = makeCall(
    context, 
    BuiltinFunctionName.MAP_REMOVE, 
    fullMap, 
    keyArray
  );
  
  fields.add(context.relBuilder.alias(dynamicMapField, "_MAP"));
}
```

### Result Processing and Dynamic Field Expansion

After query execution, the `DynamicFieldsResultProcessor` expands the `_MAP` field into individual columns before returning results to the client:

```java
// From DynamicFieldsResultProcessor.expandDynamicFields()
public static QueryResponse expandDynamicFields(QueryResponse response) {
  if (!hasDynamicFields(response)) {
    return response;
  }

  // 1. Collect all dynamic field names across all result rows
  Map<String, ExprType> dynamicFieldTypes = getDynamicFieldTypes(response.getResults());
  
  // 2. Create expanded schema: static fields + sorted dynamic fields
  Schema expandedSchema = createExpandedSchema(response.getSchema(), dynamicFieldTypes);
  
  // 3. Expand each row: extract _MAP values into individual columns
  List<ExprValue> expandedRows = expandResultRows(response.getResults(), expandedSchema);
  
  return new QueryResponse(expandedSchema, expandedRows, response.getCursor());
}
```

**Key Processing Steps:**

1. **Dynamic Field Collection**: Scans all result rows to collect unique field names from `_MAP` fields
2. **Schema Expansion**: Creates new schema with static fields followed by sorted dynamic fields
3. **Row Expansion**: For each row, extracts values from `_MAP` and creates individual columns
4. **Type Conversion**: Converts all dynamic field values to STRING type for consistency

**Example:**

```
Before expansion (internal representation):
| a   | b   | _MAP                                    |
|-----|-----|-----------------------------------------|
| v1  | v2  | {prefix_x: "val1", prefix_y: "val2"}  |
| v3  | v4  | {prefix_x: "val3", other: "val4"}     |

After expansion (returned to client):
| a   | b   | other | prefix_x | prefix_y |
|-----|-----|-------|----------|----------|
| v1  | v2  | NULL  | val1     | val2     |
| v3  | v4  | val4  | val3     | NULL     |
```

**Important Notes:**
- Dynamic fields are sorted alphabetically in the output schema
- Missing dynamic fields in a row are filled with NULL values
- All dynamic field values are converted to STRING type
- Static fields always appear before dynamic fields in the result
- The `_MAP` field itself is removed from the final output

## Implementing Dynamic Fields for New PPL Commands

### Step 1: Add Field Resolution Support

Implement visitor method in `FieldResolutionVisitor`:

```java
@Override
public Node visitYourCommand(YourCommand node, FieldResolutionContext context) {
  // 1. Extract fields used by this command
  Set<String> commandFields = extractFieldsFromExpression(node.getField());
  
  // 2. Get current requirements from downstream
  FieldResolutionResult current = context.getCurrentRequirements();
  
  // 3. Combine requirements (OR for filter-like, AND for project-like)
  context.pushRequirements(current.or(commandFields));
  
  // 4. Visit children
  visitChildren(node, context);
  
  // 5. Restore previous requirements
  context.popRequirements();
  return node;
}
```

**Key Methods:**
- `current.or(fields)`: Union - command needs current fields OR new fields (filters, sorts)
- `current.and(fields)`: Intersection - command outputs only these fields (projections)
- `current.exclude(fields)`: Remove fields - command generates these fields (eval, parse)

### Step 2: Handle Dynamic Fields in Calcite Visitor

Implement visitor method in `CalciteRelNodeVisitor`:

**Note**: Command implementation does not need to handle dynamic fields unless:
  - The command takes multiple inputs (e.g. join, append)
  - The command works against all the fields

```java
@Override
public RelNode visitYourCommand(YourCommand node, CalcitePlanContext context) {
  visitChildren(node, context);
  
  // Check if dynamic fields exist
  if (DynamicFieldsHelper.isDynamicFieldsExists(context)) {
    // Option A: Reject wildcards if not supported
    if (hasWildcardInInput(node)) {
      throw new IllegalArgumentException(
        "Command does not support wildcard fields");
    }
    
    // Option B: Handle dynamic fields explicitly
    handleDynamicFields(node, context);
  }
  
  // Regular command implementation
  // ...
  
  return context.relBuilder.peek();
}
```

### Step 3: Test Dynamic Fields Support

Add integration tests in `integ-test/`:

```java
@Test
public void testYourCommandWithWildcard() {
  String query = "source=logs | spath input=data | fields a, prefix* " +
                 "| yourcommand ...";
  
  JSONObject result = executeQuery(query);
  
  // Verify static/dynamic fields
  verifyColumn(result, "a", expectedValues);
}
```

## Limitations and Considerations

### Current Limitations

1. **Type Casting**: All extracted fields are cast to VARCHAR (temporary limitation)
2. **Wildcard Filtering**: `_MAP` contains ALL unmapped fields, not just wildcard matches
3. **Field Ordering**: Wildcard `*` must be at end of field list

### Performance Considerations

1. **JSON Parsing**: `JSON_EXTRACT_ALL` parses entire JSON document
2. **Map Operations**: `MAP_REMOVE` creates new map for each row
3. **Field Access**: Accessing `_MAP` fields requires map lookup at runtime

### Best Practices

1. **Minimize Wildcards**: Use specific field names when possible
2. **Early Filtering**: Apply filters before spath to reduce data volume
3. **Static Fields First**: List known fields explicitly before wildcards
4. **Test Coverage**: Add integration tests for wildcard scenarios

## Examples

### Example 1: Basic Wildcard Usage

```ppl
source=logs | spath input=data | fields a, b, *
```

**Field Resolution:**
- Regular fields: `{a, b}`
- Wildcard: `*`

**Execution Plan:**
```
Project(a, b, _MAP)
├─ a = CAST(ITEM(map, 'a'), VARCHAR)
├─ b = CAST(ITEM(map, 'b'), VARCHAR)
└─ _MAP = MAP_REMOVE(map, ARRAY['a', 'b'])
   └─ map = JSON_EXTRACT_ALL(data)
```

### Example 2: Multiple Commands with Wildcards

```ppl
source=logs 
| spath input=data 
| fields a, * 
| where a > 10
| stats count() by a
```

**Field Resolution Flow:**
1. `stats`: needs `{a}` (group-by field)
2. `where`: needs `{a}` (filter field)
3. `fields`: outputs `{a, *}`
4. `spath`: extracts `{a}` + creates `_MAP` for `*`

### Example 3: Join with Dynamic Fields

```ppl
source=logs1 
| spath input=data1 
| join id logs2
```

**Handling:**
- Both sides may have `_MAP` fields
- Join merges `_MAP` fields based on overwrite mode
- Static fields take precedence over dynamic fields

## Future Enhancements

### Related RFCs

- [RFC #4984](https://github.com/opensearch-project/sql/issues/4984): Schema-on-Read support
- Step 1: Field resolution for spath (completed)
- Step 2: Dynamic spath with `_MAP` field (in progress)
- Step 3: Schema-on-read with static types (planned)
- Step 4: ANY type support (planned)

## References

- **Field Resolution**: `core/src/main/java/org/opensearch/sql/ast/analysis/FieldResolutionVisitor.java`
- **Calcite Integration**: `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`
- **Dynamic Fields Helper**: `core/src/main/java/org/opensearch/sql/calcite/DynamicFieldsHelper.java`
- **Integration Tests**: `integ-test/src/test/java/org/opensearch/sql/calcite/remote/CalcitePPLSpathWithJoinIT.java`
