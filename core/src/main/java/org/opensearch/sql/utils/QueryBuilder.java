package org.opensearch.sql.utils;

/**
 * this Query builder interface defines the contract needed to be implemented by any query language builder factory
 * @param <T>
 */
public interface QueryBuilder<T> {

    T build();

    QueryBuilder withName(String name);

}