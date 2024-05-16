package com.joeyfrazee.nifi.reporting;

import java.util.Optional;

/**
 * Enum representing environment variables that can be used for configuration of default values.
 *
 * @author brightSPARK Labs
 */
public enum PluginEnvironmentVariable {
    /** The environment variable defining the page size for scrolling through the provenance repository. */
    PAGE_SIZE,
    /** The environment variable defining how far back to look into the provenance repository to index provenance events. */
    MAXIMUM_HISTORY,
    /** The environment variable defining the address for Elasticsearch. */
    ELASTICSEARCH_URL,
    /** The environment variable defining the name of the Elasticsearch index. */
    ELASTICSEARCH_INDEX,
    /** The environment variable defining the HTTP CA certificate SHA-256 fingerprint for Elasticsearch. */
    ELASTICSEARCH_CA_CERT_FINGERPRINT,
    /** The environment variable defining the username for Elasticsearch authentication. */
    ELASTICSEARCH_USERNAME,
    /** The environment variable defining the password for Elasticsearch authentication. */
    ELASTICSEARCH_PASSWORD,
    /**
     * The environment variable defining the comma-separated list of attributes to include in the
     * data sent to Elasticsearch.
     */
    ELASTICSEARCH_ATTRIBUTE_INCLUSION_LIST,
    /**
     * The environment variable defining the comma-separated list of attributes to exclude from the
     * data sent to Elasticsearch.
     */
    ELASTICSEARCH_ATTRIBUTE_EXCLUSION_LIST,
    /**
     * The environment variable defining the comma-separated list of component ids used to filter
     * which events are sent to Elasticsearch.
     */
    ELASTICSEARCH_COMPONENT_ID_FILTER_INCLUSION_LIST,
    /**
     * The environment variable defining the comma-separated list of component ids used to filter
     * which events are not sent to Elasticsearch.
     */
    ELASTICSEARCH_COMPONENT_ID_FILTER_EXCLUSION_LIST,
    /**
     * The environment variable defining the comma-separated list of event types used to filter
     * which events are sent to Elasticsearch.
     */
    ELASTICSEARCH_EVENT_TYPE_FILTER_INCLUSION_LIST,
    /**
     * The environment variable defining the comma-separated list of event types used to filter
     * which events are not sent to Elasticsearch.
     */
    ELASTICSEARCH_EVENT_TYPE_FILTER_EXCLUSION_LIST,
    ;

    /** The name of the environment variable within the system. */
    private final String name;

    /** The value associated with the environment variable within the system. */
    private final Optional<String> value;

    /** The prefix prepended to the names of the environment variables within the system. */
    private static final String ENV_VAR_PREFIX = "NIFI_PROVENANCE_REPORTING_";

    /** Default constructor. */
    PluginEnvironmentVariable() {
        this.name = ENV_VAR_PREFIX + this.name();
        final String rawValue = System.getenv(this.name);
        this.value = Optional.ofNullable(rawValue);
    }

    /**
     * Get the value of the environment variable in the system.
     *
     * @return The value of the environment variable, or null if it has no value.
     */
    public Optional<String> getValue() {
        return this.value;
    }

    /**
     * Get the name of the environment variable in the system.
     *
     * @return The name of the environment variable.
     */
    public String getName() {
        return this.name;
    }
}
