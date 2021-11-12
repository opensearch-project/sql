/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.jdbc.internal.results;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Cursor {
    private Schema schema;
    private List<Row> rows;
    private int currentRow = -1;
    private Map<String, Integer> labelToIndexMap;

     public Cursor(Schema schema, List<Row> rows) {
         this.schema = schema;
         this.rows = rows;
         initLabelToIndexMap();
     }

    public Schema getSchema() {
        return schema;
    }

    public Object getColumn(int index) {
        if (index < 0 || index >= getColumnCount())
            throw new IllegalArgumentException("Column Index out of range: " + index);
        return rows.get(currentRow).get(index);
    }

    public int getColumnCount() {
         return schema.getNumberOfColumns();
    }

    public boolean next() {
        if (currentRow < rows.size() - 1) {
            currentRow++;
            return true;
        } else {
            return false;
        }
    }

    public Integer findColumn(String label) {
         return labelToIndexMap.get(label);
    }

    private void initLabelToIndexMap() {
        labelToIndexMap = new HashMap<>();
        for (int i=0; i < schema.getNumberOfColumns(); i++) {
            ColumnMetaData columnMetaData = schema.getColumnMetaData(i);
            labelToIndexMap.put(columnMetaData.getLabel(), i);
        }
    }
}
