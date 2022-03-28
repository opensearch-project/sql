/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.domain;


/**
 * Represents the from clause.
 * Contains index and type which the
 * query refer to.
 */
public class From {
    private String index;
    private String type;
    private String alias;

    /**
     * Extract index and type from the 'from' string
     *
     * @param from The part after the FROM keyword.
     */
    public From(String from) {
        if (from.startsWith("<")) {
            index = from;
            if (!from.endsWith(">")) {
                int i = from.lastIndexOf('/');
                if (-1 < i) {
                    index = from.substring(0, i);
                    type = from.substring(i + 1);
                }
            }
            return;
        }
        String[] parts = from.split("/");
        this.index = parts[0].trim();
        if (parts.length == 2) {
            this.type = parts[1].trim();
        }
    }

    public From(String from, String alias) {
        this(from);
        this.alias = alias;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(index);
        if (type != null) {
            str.append('/').append(type);
        }
        if (alias != null) {
            str.append(" AS ").append(alias);
        }
        return str.toString();
    }
}
