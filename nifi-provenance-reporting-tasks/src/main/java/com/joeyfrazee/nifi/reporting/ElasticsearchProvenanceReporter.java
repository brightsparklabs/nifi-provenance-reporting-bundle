/*
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

import co.elastic.clients.transport.TransportUtils;
import java.io.*;
import java.net.URL;
import java.util.*;

import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;

import org.elasticsearch.client.RestClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

@Tags({"elasticsearch", "provenance"})
@CapabilityDescription("A provenance reporting task that writes to Elasticsearch")
public class ElasticsearchProvenanceReporter extends AbstractProvenanceReporter {
    public static final PropertyDescriptor ELASTICSEARCH_URL = new PropertyDescriptor
            .Builder().name("Elasticsearch URL")
            .displayName("Elasticsearch URL")
            .description("The address for Elasticsearch")
            .required(true)
            .defaultValue(EnvironmentVariable.ELASTICSEARCH_URL.getValue() != null ? EnvironmentVariable.ELASTICSEARCH_URL.getValue() : "http://localhost:9200")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ELASTICSEARCH_INDEX = new PropertyDescriptor
            .Builder().name("Elasticsearch Index")
            .displayName("Elasticsearch Index")
            .description("The name of the Elasticsearch index")
            .required(true)
            .defaultValue(EnvironmentVariable.ELASTICSEARCH_INDEX.getValue() != null ? EnvironmentVariable.ELASTICSEARCH_INDEX.getValue() : "nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ELASTICSEARCH_CA_CERT_FINGERPRINT = new PropertyDescriptor
            .Builder().name("Elasticsearch CA Certificate Fingerprint")
            .displayName("Elasticsearch CA Certificate Fingerprint")
            .description("The HTTP CA certificate SHA-256 fingerprint for Elasticsearch")
            .defaultValue(EnvironmentVariable.ELASTICSEARCH_CA_CERT_FINGERPRINT.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ELASTICSEARCH_USER = new PropertyDescriptor
            .Builder().name("Elasticsearch Username")
            .displayName("Elasticsearch Username")
            .description("The username for Elasticsearch authentication")
            .defaultValue(EnvironmentVariable.ELASTICSEARCH_USERNAME.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ELASTICSEARCH_PASSWORD = new PropertyDescriptor
            .Builder().name("Elasticsearch Password")
            .displayName("Elasticsearch Password")
            .description("The password for Elasticsearch authentication")
            .sensitive(true)
            .defaultValue(EnvironmentVariable.ELASTICSEARCH_PASSWORD.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private RestClient getRestClient(URL url) {
        return RestClient.builder(new HttpHost(url.getHost(), url.getPort())).build();
    }

    private RestClient getSecureRestClient(URL url, String caCertFingerprint, String username, String password) {
        final SSLContext sslContext = TransportUtils.sslContextFromCaFingerprint(caCertFingerprint);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        return RestClient
                .builder(new HttpHost(url.getHost(), url.getPort(), "https"))
                .setHttpClientConfigCallback(hc -> hc
                        .setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(credentialsProvider)
                )
                .build();
    }

    private ElasticsearchClient getElasticsearchClient(RestClient restClient) {
        final ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        descriptors = super.getSupportedPropertyDescriptors();
        descriptors.add(ELASTICSEARCH_URL);
        descriptors.add(ELASTICSEARCH_INDEX);
        descriptors.add(ELASTICSEARCH_CA_CERT_FINGERPRINT);
        descriptors.add(ELASTICSEARCH_USER);
        descriptors.add(ELASTICSEARCH_PASSWORD);
        return descriptors;
    }

    public void indexEvent(final Map<String, Object> event, final ReportingContext context) throws IOException {
        // Get properties from context.
        final URL elasticsearchUrl = new URL(context.getProperty(ELASTICSEARCH_URL).getValue());
        final String elasticsearchIndex = context.getProperty(ELASTICSEARCH_INDEX).evaluateAttributeExpressions()
                .getValue();
        final String elasticsearchCACertFingerprint = context.getProperty(ELASTICSEARCH_CA_CERT_FINGERPRINT).getValue();
        final String elasticsearchUser = context.getProperty(ELASTICSEARCH_USER).getValue();
        final String elasticsearchPassword = context.getProperty(ELASTICSEARCH_PASSWORD).getValue();

        // Create the Elasticsearch API client.
        final String protocol = elasticsearchUrl.getProtocol();
        final RestClient restClient = (protocol.equals("https")) ? getSecureRestClient(elasticsearchUrl, elasticsearchCACertFingerprint, elasticsearchUser, elasticsearchPassword) : getRestClient(elasticsearchUrl);
        final ElasticsearchClient client = getElasticsearchClient(restClient);

        // Index the event.
        final String id = Long.toString((Long) event.get("event_id"));
        final IndexRequest<Map<String, Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
                .index(elasticsearchIndex)
                .id(id)
                .document(event)
                .build();
        client.index(indexRequest);
    }
}
