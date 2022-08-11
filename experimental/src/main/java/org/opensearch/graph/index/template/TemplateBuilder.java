package org.opensearch.graph.index.template;

import org.opensearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TemplateBuilder {
    private Map<String, Object> content;
    private String type;
    private String version;

    private TemplateType templateType = TemplateType.STANDARD;
    private List<String> patterns;
    private List<String> aliases;
    private List<String> composition;
    private Settings templateSettings;

    public static TemplateBuilder create(String type) {
        return new TemplateBuilder(type);
    }

    private TemplateBuilder(String type) {
        this.type = type;
        this.patterns = new ArrayList<>();
        this.aliases = new ArrayList<>();
        this.composition = new ArrayList<>();
        // default minimalistic settings
        this.templateSettings = SettingBuilder.create()
                .shards(3)
                .replicas(1)
                .build();
    }

    public TemplateBuilder settings(Settings settings) {
        this.templateSettings = settings;
        return this;
    }

    public TemplateBuilder pattern(String pattern) {
        this.patterns.add(pattern);
        return this;
    }

    public TemplateBuilder composedOf(String template) {
        this.composition.add(template);
        return this;
    }

    public TemplateBuilder alias(String alias) {
        this.aliases.add(alias);
        return this;
    }

    public TemplateBuilder version(String version) {
        this.version = version;
        return this;
    }


    /**
     * compose the builder into the target index template structure
     *
     * @return
     */
    public Map<String, Object> build() {
        return Collections.emptyMap();
    }

    /**
     * template type
     */
    public enum TemplateType {
        COMPONENT,
        COMPOSITE,
        STANDARD
    }
}
