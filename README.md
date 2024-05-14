[![Build Status](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/actions/workflows/test.yml/badge.svg)](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/actions/workflows/test.yml)
[![Maven](https://img.shields.io/maven-central/v/com.brightsparklabs/nifi-provenance-reporting-nar)](https://search.maven.org/artifact/com.brightsparklabs/nifi-provenance-reporting-nar)

# nifi-provenance-reporting-bundle

NiFi provenance reporting tasks.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Tasks](#tasks)
    - [ElasticsearchProvenanceReporter](#elasticsearchprovenancereporter)
    - [HttpProvenanceReporter [DEPRECATED]](#httpprovenancereporter-deprecated)
- [License](#license)

## Prerequisites

* Java 17
* NiFi 1.25.0

## Installation

```sh
$ gradle nar
$ cp build/libs/nifi-provenance-reporting-nar-<version>.nar $NIFI_HOME/lib
$ nifi restart
```

## Tasks

### ElasticsearchProvenanceReporter

Reporting task to write provenance events to an Elasticsearch index.

#### Reporting Task Properties

Properties can be set via the NiFi UI, or with environment variables (required properties are **bold**):

| Property                                 | Description                                                                                                                                                                                                                        | Default Value           | Environment Variable                                                                                                                                                                                      |
|------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Page Size**                            | Page size for scrolling through the provenance repository.                                                                                                                                                                         | `100`                   | `NIFI_PROVENANCE_REPORTING_PAGE_SIZE`                                                                                                                                                                     |
| **Maximum History**                      | How far back to look into the provenance repository to index provenance events.                                                                                                                                                    | `10000`                 | `NIFI_PROVENANCE_REPORTING_MAXIMUM_HISTORY`                                                                                                                                                               |
| **Elasticsearch URL**                    | The address for Elasticsearch.                                                                                                                                                                                                     | `http://localhost:9200` | `NIFI_PROVENANCE_REPORTING_ELASTICSEARCH_URL`                                                                                                                                                             |
| **Elasticsearch Index**                  | The name of the Elasticsearch index.                                                                                                                                                                                               | `nifi`                  | `NIFI_PROVENANCE_REPORTING_ELASTICSEARCH_INDEX`                                                                                                                                                           |
| Elasticsearch CA Certificate Fingerprint | The HTTP CA certificate SHA-256 fingerprint for Elasticsearch. Required for HTTPS.                                                                                                                                                 |                         | `NIFI_PROVENANCE_REPORTING_ELASTICSEARCH_CA_CERT_FINGERPRINT`                                                                                                                                             |
| Elasticsearch Username                   | The username for Elasticsearch authentication. Required for HTTPS.                                                                                                                                                                 |                         | `NIFI_PROVENANCE_REPORTING_ELASTICSEARCH_USERNAME`                                                                                                                                                        |
| Elasticsearch Password                   | The password for Elasticsearch authentication. Required for HTTPS.                                                                                                                                                                 |                         | `NIFI_PROVENANCE_REPORTING_ELASTICSEARCH_PASSWORD`<br/><sub>**NOTE**: When setting the Elasticsearch Password via environment variable, the field in the NiFi UI will still display 'No value set'.</sub> |
| Elasticsearch Attribute Inclusion List   | The comma-separated list of attributes to include in the data sent to Elasticsearch. All other attributes will be excluded. <br/><sub>**NOTE**: This property is mutually exclusive with the Elasticsearch Exclusion List.</sub>   |                         | `NIFI_PROVENANCE_REPORTING_ELASTICSEARCH_ATTRIBUTE_INCLUSION_LIST`                                                                                                                                        |
| Elasticsearch Attribute Exclusion List   | The comma-separated list of attributes to exclude from the data sent to Elasticsearch. All other attributes will be included. <br/><sub>**NOTE**: This property is mutually exclusive with the Elasticsearch Inclusion List.</sub> |                         | `NIFI_PROVENANCE_REPORTING_ELASTICSEARCH__ATTRIBUTEEXCLUSION_LIST`                                                                                                                                        |

**NOTE**: Separate Elasticsearch indices should be used when Swapping between the Elasticsearch 
Attribute Inclusion List and Elasticsearch Attribute Exclusion List so that fields are correctly mapped.

### HttpProvenanceReporter [DEPRECATED]

**NOTE**: This task was deprecated in version 1.0.0.

Reporting task to POST provenance events to an HTTP web service.

#### Reporting Task Properties

This reporting task can be configured to POST provenance events to an arbitrary web service. Here is an example of using it with Solr (10s commits):

<img src="http_provenance_reporter_properties.png" width=600 />

## License

Copyright (c) 2023 brightSPARK Labs (from commit `477827d4818d475e23801006dc0e9273b70fd159`
onwards).

Copyright (c) 2016 Joey Frazee (to and including commit `477827d4818d475e23801006dc0e9273b70fd159`).

nifi-provenance-reporting-bundle is released under the Apache License Version 2.0.
