package org.opensearch.graph.index.template;

import org.opensearch.common.settings.Settings;

public class SettingBuilder {
    private Settings.Builder builder;

    /**
     * create a new setting object
     * @return
     */
    public static SettingBuilder create() {
        return new SettingBuilder(Settings.builder());
    }

    /**
     * use the given setting object
     * @return
     */
    public static SettingBuilder use(Settings.Builder builder) {
        return new SettingBuilder(builder);
    }

    private SettingBuilder(Settings.Builder builder) {
        this.builder = builder;
    }

    public SettingBuilder shards(int shards) {
        Settings.builder()
                .put("index.number_of_shards", shards);
        return this;
    }

    public SettingBuilder sortByField(String fieldName, boolean asc) {
        builder.put("sort.field", fieldName)
                .put("sort.order", asc ? "asc" : "desc");
        return this;
    }

    public SettingBuilder replicas(int shards) {
        Settings.builder()
                .put("index.number_of_replicas", shards);
        return this;
    }

    public Settings build() {
        return this.builder.build();
    }
}
