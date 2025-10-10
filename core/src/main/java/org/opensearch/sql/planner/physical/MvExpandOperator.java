/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical;
//
// import java.util.Collections;
// import java.util.Iterator;
// import java.util.List;
// import java.util.Map;
// import java.util.NoSuchElementException;
// import java.util.Optional;
// import java.util.ArrayList;
// import java.util.LinkedHashMap;
// import lombok.EqualsAndHashCode;
// import lombok.Getter;
// import lombok.ToString;
// import org.opensearch.sql.data.model.ExprTupleValue;
// import org.opensearch.sql.data.model.ExprValue;
// import org.opensearch.sql.data.model.ExprValueUtils;
//
// @EqualsAndHashCode(callSuper = false)
// @ToString
// public class MvExpandOperator extends PhysicalPlan {
//    @Getter private final PhysicalPlan input;
//    @Getter private final String fieldName;
//    @Getter private final Optional<Integer> limit;
//    @ToString.Exclude private Iterator<ExprValue> expandedValuesIterator =
// Collections.emptyIterator();
//    @ToString.Exclude private ExprValue next = null;
//    @ToString.Exclude private boolean nextPrepared = false;
//
//    public MvExpandOperator(PhysicalPlan input, String fieldName, Optional<Integer> limit) {
//        this.input = input;
//        this.fieldName = fieldName;
//        this.limit = limit;
//    }
//
//    @Override
//    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
//        return visitor.visitMvExpandOperator(this, context);
//    }
//
//    @Override
//    public List<PhysicalPlan> getChild() {
//        return Collections.singletonList(input);
//    }
//
//    @Override
//    public void open() {
//        input.open();
//        expandedValuesIterator = Collections.emptyIterator();
//        next = null;
//        nextPrepared = false;
//    }
//
//    @Override
//    public void close() {
//        input.close();
//    }
//
//    @Override
//    public boolean hasNext() {
//        if (!nextPrepared) {
//            prepareNext();
//        }
//        return next != null;
//    }
//
//    @Override
//    public ExprValue next() {
//        if (!nextPrepared) {
//            prepareNext();
//        }
//        if (next == null) {
//            throw new NoSuchElementException("No more values in MvExpandOperator");
//        }
//        ExprValue result = next;
//        next = null;
//        nextPrepared = false;
//        return result;
//    }
//
//    private void prepareNext() {
//        while (true) {
//            if (expandedValuesIterator != null && expandedValuesIterator.hasNext()) {
//                next = expandedValuesIterator.next();
//                nextPrepared = true;
//                return;
//            }
//            if (!input.hasNext()) {
//                next = null;
//                nextPrepared = true;
//                return;
//            }
//            ExprValue value = input.next();
//            expandedValuesIterator = expandRow(value);
//        }
//    }
//
//    private Iterator<ExprValue> expandRow(ExprValue value) {
//        if (value == null || value.isMissing()) {
//            return Collections.emptyIterator();
//        }
//        Map<String, ExprValue> tuple = value.tupleValue();
//
//        if (fieldName.startsWith("_")) {
//            return Collections.singletonList(value).iterator();
//        }
//
//        ExprValue fieldVal = tuple.get(fieldName);
//        if (fieldVal == null || fieldVal.isMissing() || fieldVal.isNull()) {
//            return Collections.emptyIterator();
//        }
//
//        // If not a collection, just return the row as is
//        if (!(fieldVal instanceof org.opensearch.sql.data.model.ExprCollectionValue)) {
//            return Collections.singletonList(value).iterator();
//        }
//
//        // Get the list of ExprValue from the collection
//        List<ExprValue> values = fieldVal.collectionValue();
//        if (values.isEmpty()) {
//            return Collections.emptyIterator();
//        }
//
//        int max = limit.orElse(values.size());
//        List<ExprValue> expandedRows = new ArrayList<>();
//        int count = 0;
//        for (ExprValue v : values) {
//            if (max > 0 && count >= max) break;
//            count++;
//            LinkedHashMap<String, ExprValue> newTuple = new LinkedHashMap<>(tuple);
//            newTuple.put(fieldName, v);
//            expandedRows.add(new ExprTupleValue(newTuple));
//        }
//        return expandedRows.iterator();
//    }
// }
