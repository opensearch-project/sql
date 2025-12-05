package org.opensearch.sql.api;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;

public class CustomSettingsTest {

    /**
     * Simple Settings implementation for standalone usage.
     * Pre-configures settings to allow joins.
     */
    private static class CustomSettings extends Settings {

        private final Map<Key, Object> settingsMap;

        public CustomSettings() {
            settingsMap = new HashMap<>();
            settingsMap.put(Key.CALCITE_SUPPORT_ALL_JOIN_TYPES, true);
            settingsMap.put(Key.CALCITE_ENGINE_ENABLED, true);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T getSettingValue(Key key) {
            return (T) settingsMap.get(key);
        }

        @Override
        public List<String> getSettings() {
            return settingsMap.keySet().stream().map(Key::getKeyValue).toList();
        }
    }

    static class TestSchema extends AbstractSchema {

        private Table newTableWithId() {
            return new AbstractTable() {
                @Override
                public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                    return typeFactory.createStructType(
                        List.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
                        List.of("id")
                    );
                }
            };
        }

        @Override
        protected Map<String, Table> getTableMap() {
            return Map.of(
                "users",
                newTableWithId(),
                "transactions",
                newTableWithId()
            );
        }
    }

    static UnifiedQueryPlanner planner = UnifiedQueryPlanner.builder()
        .language(QueryType.PPL)
        .catalog("opensearch", new TestSchema())
        .defaultNamespace("opensearch")
        .cacheMetadata(true)
        .settings(new CustomSettings())
        .build();

    @Test
    public void testSimpleQueries() {
        planner.plan("source=users | where id > 42");
        planner.plan("source=transactions | stats avg(id)");
    }

    @Test
    public void testJoinQuery() {
        planner.plan(
            "source=users | join on users.id = transactions.id transactions"
        );
    }
}
