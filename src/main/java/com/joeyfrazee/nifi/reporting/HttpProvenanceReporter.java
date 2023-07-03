/*
 * Maintained by brightSPARK Labs.
 * www.brightsparklabs.com
 *
 * Refer to LICENSE at repository root for license details.
 */

package com.joeyfrazee.nifi.reporting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"http", "provenance"})
@CapabilityDescription("A provenance reporting task that posts to an HTTP server")
public class HttpProvenanceReporter extends AbstractProvenanceReporter {
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

    public static final PropertyDescriptor URL =
            new PropertyDescriptor.Builder()
                    .name("URL")
                    .displayName("URL")
                    .description("The URL to post to")
                    .required(true)
                    .addValidator(StandardValidators.URL_VALIDATOR)
                    .build();

    private final AtomicReference<OkHttpClient> client = new AtomicReference<>();

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        descriptors = super.getSupportedPropertyDescriptors();
        descriptors.add(URL);
        return descriptors;
    }

    @OnScheduled
    public void setup(final ConfigurationContext context) throws IOException {
        client.set(new OkHttpClient());
    }

    private OkHttpClient getHttpClient() {
        return client.get();
    }

    private void post(String json, String url) throws IOException {
        final RequestBody body = RequestBody.create(JSON, json);
        final Request request = new Request.Builder().url(url).post(body).build();
        final Response response = getHttpClient().newCall(request).execute();
        getLogger()
                .info(
                        "{} {} {}",
                        new Object[] {
                            response.code(), response.message(), response.body().string()
                        });
    }

    @Override
    public void indexEvent(final Map<String, Object> event, final ReportingContext context)
            throws IOException {
        final String url = context.getProperty(URL).getValue();
        final String json = new ObjectMapper().writeValueAsString(event);
        post(json, url);
    }
}
