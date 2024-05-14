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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportUtils;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.elasticsearch.client.RestClient;

@Tags({"elasticsearch", "provenance"})
// Ideally we would use an environment variable to set the default run schedule. Unfortunately this
// would require either modifications to the NiFi source code, or multiple authenticated HTTP
// requests using the NiFi REST API after the reporting task is created. Neither solution is
// ideal, so instead we use a more sensible default of 1 min (instead of 5 min).
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
@CapabilityDescription("A provenance reporting task that writes to Elasticsearch")
public class ElasticsearchProvenanceReporter extends AbstractProvenanceReporter {
    // -------------------------------------------------------------------------
    // CONSTANTS
    // -------------------------------------------------------------------------

    /** Splits a string based on comma delimiters, ignoring surrounding whitespace. */
    private static final Splitter COMMA_SPLITTER =
            Splitter.onPattern(",").trimResults().omitEmptyStrings();

    /** The address for Elasticsearch. */
    public static final PropertyDescriptor ELASTICSEARCH_URL =
            new PropertyDescriptor.Builder()
                    .name("Elasticsearch URL")
                    .displayName("Elasticsearch URL")
                    .description(
                            "The address for Elasticsearch."
                                    + defaultEnvironmentVariableDescription(
                                            PluginEnvironmentVariable.ELASTICSEARCH_URL))
                    .required(true)
                    .defaultValue(
                            PluginEnvironmentVariable.ELASTICSEARCH_URL
                                    .getValue()
                                    .orElse("http://localhost:9200"))
                    .addValidator(StandardValidators.URL_VALIDATOR)
                    .build();

    /** The name of the Elasticsearch index. */
    public static final PropertyDescriptor ELASTICSEARCH_INDEX =
            new PropertyDescriptor.Builder()
                    .name("Elasticsearch Index")
                    .displayName("Elasticsearch Index")
                    .description(
                            "The name of the Elasticsearch index."
                                    + defaultEnvironmentVariableDescription(
                                            PluginEnvironmentVariable.ELASTICSEARCH_INDEX))
                    .required(true)
                    .defaultValue(
                            PluginEnvironmentVariable.ELASTICSEARCH_INDEX.getValue().orElse("nifi"))
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    /** The HTTP CA certificate SHA-256 fingerprint for Elasticsearch. Required for HTTPS. */
    public static final PropertyDescriptor ELASTICSEARCH_CA_CERT_FINGERPRINT =
            new PropertyDescriptor.Builder()
                    .name("Elasticsearch CA Certificate Fingerprint")
                    .displayName("Elasticsearch CA Certificate Fingerprint")
                    .description(
                            "The HTTP CA certificate SHA-256 fingerprint for Elasticsearch. "
                                    + "Required for HTTPS."
                                    + defaultEnvironmentVariableDescription(
                                            PluginEnvironmentVariable
                                                    .ELASTICSEARCH_CA_CERT_FINGERPRINT))
                    .defaultValue(
                            PluginEnvironmentVariable.ELASTICSEARCH_CA_CERT_FINGERPRINT
                                    .getValue()
                                    .orElse(null))
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    /** The username for Elasticsearch authentication. Required for HTTPS. */
    public static final PropertyDescriptor ELASTICSEARCH_USERNAME =
            new PropertyDescriptor.Builder()
                    .name("Elasticsearch Username")
                    .displayName("Elasticsearch Username")
                    .description(
                            "The username for Elasticsearch authentication. Required for HTTPS."
                                    + defaultEnvironmentVariableDescription(
                                            PluginEnvironmentVariable.ELASTICSEARCH_USERNAME))
                    .defaultValue(
                            PluginEnvironmentVariable.ELASTICSEARCH_USERNAME
                                    .getValue()
                                    .orElse(null))
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    /** The password for Elasticsearch authentication. Required for HTTPS. */
    public static final PropertyDescriptor ELASTICSEARCH_PASSWORD =
            new PropertyDescriptor.Builder()
                    .name("Elasticsearch Password")
                    .displayName("Elasticsearch Password")
                    .description(
                            "The password for Elasticsearch authentication. Required for HTTPS."
                                    + defaultEnvironmentVariableDescription(
                                            PluginEnvironmentVariable.ELASTICSEARCH_PASSWORD)
                                    + " NOTE: The field will display 'No value set' when set via "
                                    + "environment variable.")
                    .sensitive(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .build();

    /**
     * The default password for Elasticsearch authentication. This value is defined outside the
     * property descriptor, because the NiFi UI leaks default sensitive values.
     */
    private static final String DEFAULT_ELASTICSEARCH_PASSWORD =
            PluginEnvironmentVariable.ELASTICSEARCH_PASSWORD.getValue().orElse(null);

    /**
     * The comma-separated list of attributes to include in the data sent to Elasticsearch. Mutually
     * exclusive with `ELASTICSEARCH_ATTRIBUTE_EXCLUSION_LIST`.
     */
    public static final PropertyDescriptor ELASTICSEARCH_ATTRIBUTE_INCLUSION_LIST =
            new PropertyDescriptor.Builder()
                    .name("Elasticsearch Attribute Inclusion List")
                    .displayName("Elasticsearch Attribute Inclusion List")
                    .description(
                            "The comma-separated list of attributes to include in the data sent "
                                    + "to Elasticsearch. All other attributes will be excluded. "
                                    + "This property is mutually exclusive with the Elasticsearch"
                                    + " Exclusion List."
                                    + defaultEnvironmentVariableDescription(
                                            PluginEnvironmentVariable
                                                    .ELASTICSEARCH_ATTRIBUTE_INCLUSION_LIST))
                    .defaultValue(
                            PluginEnvironmentVariable.ELASTICSEARCH_ATTRIBUTE_INCLUSION_LIST
                                    .getValue()
                                    .orElse(null))
                    .addValidator(
                            StandardValidators.createListValidator(
                                    true, true, StandardValidators.ATTRIBUTE_KEY_VALIDATOR))
                    .build();

    /**
     * The comma-separated list of attributes to exclude from the data sent to Elasticsearch.
     * Mutually exclusive with `ELASTICSEARCH_ATTRIBUTE_INCLUSION_LIST`.
     */
    public static final PropertyDescriptor ELASTICSEARCH_ATTRIBUTE_EXCLUSION_LIST =
            new PropertyDescriptor.Builder()
                    .name("Elasticsearch Attribute Exclusion List")
                    .displayName("Elasticsearch Attribute Exclusion List")
                    .description(
                            "The comma-separated list of attributes to exclude from the data sent "
                                    + "to Elasticsearch. All other attributes will be included. "
                                    + "This property is mutually exclusive with the Elasticsearch"
                                    + " Inclusion List."
                                    + defaultEnvironmentVariableDescription(
                                            PluginEnvironmentVariable
                                                    .ELASTICSEARCH_ATTRIBUTE_EXCLUSION_LIST))
                    .defaultValue(
                            PluginEnvironmentVariable.ELASTICSEARCH_ATTRIBUTE_EXCLUSION_LIST
                                    .getValue()
                                    .orElse(null))
                    .addValidator(
                            StandardValidators.createListValidator(
                                    true, true, StandardValidators.ATTRIBUTE_KEY_VALIDATOR))
                    .build();

    // -------------------------------------------------------------------------
    // PUBLIC METHODS
    // -------------------------------------------------------------------------

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        descriptors = super.getSupportedPropertyDescriptors();
        descriptors.add(ELASTICSEARCH_URL);
        descriptors.add(ELASTICSEARCH_INDEX);
        descriptors.add(ELASTICSEARCH_CA_CERT_FINGERPRINT);
        descriptors.add(ELASTICSEARCH_USERNAME);
        descriptors.add(ELASTICSEARCH_PASSWORD);
        descriptors.add(ELASTICSEARCH_ATTRIBUTE_INCLUSION_LIST);
        descriptors.add(ELASTICSEARCH_ATTRIBUTE_EXCLUSION_LIST);
        return descriptors;
    }

    @Override
    public void indexEvent(final Map<String, Object> event, final ReportingContext context)
            throws IOException {
        // Get properties from context.
        final URL elasticsearchUrl = new URL(context.getProperty(ELASTICSEARCH_URL).getValue());
        final String elasticsearchIndex =
                context.getProperty(ELASTICSEARCH_INDEX).evaluateAttributeExpressions().getValue();
        final String elasticsearchCACertFingerprint =
                context.getProperty(ELASTICSEARCH_CA_CERT_FINGERPRINT).getValue();
        final String elasticsearchUsername = context.getProperty(ELASTICSEARCH_USERNAME).getValue();
        final String elasticsearchPassword = getElasticsearchPassword(context);

        // Create the Elasticsearch API client.
        final String protocol = elasticsearchUrl.getProtocol();
        // Wrap RestClient in a try-with-resources block to auto-close it.
        try (final RestClient restClient =
                protocol.equals("https")
                        ? getSecureRestClient(
                                elasticsearchUrl,
                                elasticsearchCACertFingerprint,
                                elasticsearchUsername,
                                elasticsearchPassword)
                        : getRestClient(elasticsearchUrl)) {
            final ElasticsearchClient client = getElasticsearchClient(restClient);

            // Filter event fields based on attribute inclusion/exclusion list.
            final ImmutableMap<String, Object> filteredEvent = filterEventFields(event, context);

            // Index the event.
            final String id = Long.toString((Long) event.get("event_id"));
            final IndexRequest<Map<String, Object>> indexRequest =
                    new IndexRequest.Builder<Map<String, Object>>()
                            .index(elasticsearchIndex)
                            .id(id)
                            .document(filteredEvent)
                            .build();
            client.index(indexRequest);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(
            final ValidationContext validationContext) {
        final List<ValidationResult> errors = new ArrayList<>();

        final String inclusionListString =
                validationContext.getProperty(ELASTICSEARCH_ATTRIBUTE_INCLUSION_LIST).getValue();
        final String exclusionListString =
                validationContext.getProperty(ELASTICSEARCH_ATTRIBUTE_EXCLUSION_LIST).getValue();

        // Ensure the Elasticsearch inclusion and exclusion lists are mutually exclusive.
        if (!Strings.isNullOrEmpty(inclusionListString)
                && !Strings.isNullOrEmpty(exclusionListString)) {
            errors.add(
                    new ValidationResult.Builder()
                            // The validation error message is displayed in the NiFi UI as
                            // "'<subject>' is invalid because <explanation>"
                            .subject(
                                    "Mutual exclusion required for `Elasticsearch Inclusion "
                                            + "List` & `Elasticsearch Exclusion List`.")
                            .explanation(
                                    "the inclusion and exclusion lists are mutually exclusive "
                                            + "(i.e. only one can be specified).")
                            .valid(false)
                            .build());
        }
        return errors;
    }

    // -------------------------------------------------------------------------
    // PRIVATE METHODS
    // -------------------------------------------------------------------------

    /**
     * Fetch the Elasticsearch password from its property descriptor. If no password is set, the
     * value set within the `ELASTICSEARCH_PASSWORD` environment variable will be returned instead.
     *
     * @param context The reporting context.
     * @return The Elasticsearch password, or null.
     */
    private String getElasticsearchPassword(final ReportingContext context) {
        String elasticsearchPassword = context.getProperty(ELASTICSEARCH_PASSWORD).getValue();
        if (elasticsearchPassword == null) {
            elasticsearchPassword = DEFAULT_ELASTICSEARCH_PASSWORD;
        }
        return elasticsearchPassword;
    }

    /**
     * Create an HTTPS REST client for connecting to the given Elasticsearch URL.
     *
     * @param elasticsearchUrl The Elasticsearch URL.
     * @param elasticsearchCACertFingerprint The Elasticsearch CA certificate SHA-256 fingerprint.
     * @param elasticsearchUsername The Elasticsearch username.
     * @param elasticsearchPassword The Elasticsearch password.
     * @return The RestClient object.
     */
    private RestClient getSecureRestClient(
            final URL elasticsearchUrl,
            final String elasticsearchCACertFingerprint,
            final String elasticsearchUsername,
            final String elasticsearchPassword) {
        final SSLContext sslContext =
                TransportUtils.sslContextFromCaFingerprint(elasticsearchCACertFingerprint);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(elasticsearchUsername, elasticsearchPassword));
        return RestClient.builder(
                        new HttpHost(
                                elasticsearchUrl.getHost(), elasticsearchUrl.getPort(), "https"))
                .setHttpClientConfigCallback(
                        hc ->
                                hc.setSSLContext(sslContext)
                                        .setDefaultCredentialsProvider(credentialsProvider))
                .build();
    }

    /**
     * Create a REST client for connecting to the given Elasticsearch URL.
     *
     * @param elasticsearchUrl The Elasticsearch URL.
     * @return The RestClient object.
     */
    private RestClient getRestClient(final URL elasticsearchUrl) {
        final HttpHost httpHost =
                new HttpHost(elasticsearchUrl.getHost(), elasticsearchUrl.getPort());
        return RestClient.builder(httpHost).build();
    }

    /**
     * Create an Elasticsearch client from the given Elasticsearch REST client.
     *
     * @param restClient The Elasticsearch REST client.
     * @return The ElasticsearchClient object.
     */
    private ElasticsearchClient getElasticsearchClient(final RestClient restClient) {
        final ElasticsearchTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    /**
     * Filter the given event to remove fields based on the `Elasticsearch Attribute Inclusion List`
     * or `Elasticsearch Attribute Exclusion List`. If both lists are empty, the event is returned
     * with fields unmodified.
     *
     * @param event The event to filter.
     * @param context The reporting context.
     * @return The filtered event.
     */
    private ImmutableMap<String, Object> filterEventFields(
            final Map<String, Object> event, final ReportingContext context) {
        // Process inclusion rules if present.
        final String inclusionListString =
                context.getProperty(ELASTICSEARCH_ATTRIBUTE_INCLUSION_LIST).getValue();
        if (!Strings.isNullOrEmpty(inclusionListString)) {
            // Only include fields which ARE in the field list.
            final ImmutableSet<String> fieldsToInclude = extractFieldNames(inclusionListString);
            final Map<String, Object> filteredMap =
                    Maps.filterKeys(event, fieldsToInclude::contains);
            return ImmutableMap.copyOf(filteredMap);
        }

        // Process exclusion rules if present.
        final String exclusionListString =
                context.getProperty(ELASTICSEARCH_ATTRIBUTE_EXCLUSION_LIST).getValue();
        if (!Strings.isNullOrEmpty(exclusionListString)) {
            // Only include fields which ARE NOT in the field list.
            final ImmutableSet<String> fieldsToExclude = extractFieldNames(exclusionListString);
            final Map<String, Object> filteredMap =
                    Maps.filterKeys(event, k -> !fieldsToExclude.contains(k));
            return ImmutableMap.copyOf(filteredMap);
        }

        // No filtering required.
        return ImmutableMap.copyOf(event);
    }

    /**
     * Extract a set of field names from given comma-separated list.
     *
     * @param fieldsString The field names, as a comma-separated list.
     * @return The set of field names.
     */
    private ImmutableSet<String> extractFieldNames(final String fieldsString) {
        // Convert comma-separated list into array, while ignoring whitespace.
        final Iterable<String> split = COMMA_SPLITTER.split(fieldsString);
        return ImmutableSet.copyOf(split);
    }
}
