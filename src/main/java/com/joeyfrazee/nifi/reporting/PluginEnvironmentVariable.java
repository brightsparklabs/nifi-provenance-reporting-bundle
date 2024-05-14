/*
 * Maintained by brightSPARK Labs.
 * www.brightsparklabs.com
 * _____________________________________________________________________________
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.joeyfrazee.nifi.reporting;

import java.util.Optional;

/**
 * Enum representing environment variables that can be used for configuration of default values.
 *
 * @author brightSPARK Labs
 */
public enum PluginEnvironmentVariable {
    /**
     * The environment variable defining the page size for scrolling through the provenance
     * repository.
     */
    PAGE_SIZE,
    /**
     * The environment variable defining how far back to look into the provenance repository to
     * index provenance events.
     */
    MAXIMUM_HISTORY,
    /** The environment variable defining the address for Elasticsearch. */
    ELASTICSEARCH_URL,
    /** The environment variable defining the name of the Elasticsearch index. */
    ELASTICSEARCH_INDEX,
    /**
     * The environment variable defining the HTTP CA certificate SHA-256 fingerprint for
     * Elasticsearch.
     */
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
