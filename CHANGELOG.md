# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The changelog is applicable from version `1.0.0` onwards.

## [2.6.0] - 2024-12-02

### Changed

- APED-139: Dependency patching.

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/2.5.1...2.6.0)

## [2.5.1] - 2024-10-17

### Fixed

- APED-138: Fix publish to Maven Central.

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/2.5.0...2.5.1)

## [2.5.0] - 2024-10-17

### Changed

- APED-138: Dependency patching.

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/2.4.0...2.5.0)

---

## [2.4.0] - 2024-05-21

### Added

- APED-123: Added inclusion and exclusion lists to filter events based on `component_id` and `event_type` values.

### Changed

- APED-123: Updated the inclusion and exclusion lists to specify their focus on Attributes.
- APED-123: Updated README.

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/2.3.0...2.4.0)

---

## [2.3.0] - 2024-03-25

### Changed

- APED-115: Major dependency patching.

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/2.2.0...2.3.0)

---

## [2.2.0] - 2023-12-08

### Changed

- APED-98: Minor dependency patching.

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/2.1.1...2.2.0)

---

## [2.1.1] - 2023-10-27

### Fixed

- APED-74: Fix memory issue causing NiFi to crash.
- APED-66: Fix tag bug workaround in `publish.yml` workflow.

### Changed

- APED-73: Updated README.

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/2.1.0...2.1.1)

---

## [2.1.0] - 2023-09-19

### Added

- APED-33: Added Changelog Enforcer.
- APED-44: Add attribute inclusion and exclusion lists for output to Elasticsearch.

### Changed

- APED-34: Updated README.
- APED-43: Set default run schedule for `ElasticsearchProvenanceReporter` to `"1 min"`.

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/2.0.0...2.1.0)

---

## [2.0.0] - 2023-07-04

### Added

- APED-18: Migrate to Java 17 and latest NiFi.
- APED-20: Added gradle-baseline support.
- APED-23: Support dependabot updates.

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/1.0.0...2.0.0)

---

## [1.0.0] - 2023-07-03

### Added

- APED-15: Added mutual TLS support for connecting to Elasticsearch.
- APED-19: Support overriding properties via environment variables.
- APED-24: Support publishing to Maven Central for posterity.

### Changed

- APED-14: Dependency updates.
- APED-16: Migrated from maven to gradle.

---

# Template

## [Unreleased] - YYYY-MM-DD

### Added

### Fixed

### Changed

[Commits](https://github.com/brightsparklabs/nifi-provenance-reporting-bundle/compare/2.0.0...)

---
