# Project Vision

## The Goal

This project is building the data foundation for short-horizon prediction of
the Australian-region K-index, a measure of local geomagnetic disturbance. The
eventual modelling system will combine historical K-index observations with
candidate predictors from NASA's OMNI dataset, including solar-wind and
interplanetary magnetic-field measurements.

The immediate challenge is not choosing an algorithm. It is establishing
whether the observations required for a modelling request exist, agree, and
can be traced back to their source. The project therefore begins with a
trustworthy data-readiness system before progressing to feature engineering,
model training, or prediction serving.

The eventual modelling objective remains deliberately broad: predict K-index
over a short future interval. Exploratory analysis will determine whether the
first useful formulation should be regression, classification, or another
well-defined objective.

## Current Data-Readiness MVP

The current minimum viable product answers a concrete question:

> For a requested modelling period, do we have the K-index targets, historical
> K-index lags, and OMNI lookback observations needed to construct the dataset?

The MVP is complete when the system can:

- preserve each K-index and OMNI ingestion run as immutable raw evidence;
- consolidate source responses into audit-friendly tables without erasing run
  history, duplicate reports, empty responses, or source fill values;
- reconcile repeated reports into canonical K-index and OMNI tables with one
  downstream observation per canonical key;
- report missing, null, and conflicting observations at each source's native
  cadence;
- assess a modelling-dataset request and explain whether identified coverage
  issues block construction;
- persist the request, reports, issues, input fingerprints, and manifest as an
  assessment artifact; and
- demonstrate the completed flow through a credential-free, read-only public
  dashboard using frozen example artifacts.

This is **production-style data engineering**, not yet a production forecasting
service. Here, production-style means explicit contracts, immutable inputs,
traceable transformations, configuration and CLI boundaries, deterministic
artifacts, and contract-focused tests. It does not imply operational
forecasting, cloud-scale availability, or a deployed prediction API.

## System Flow and Trust Model

K-index targets and OMNI predictors begin in separate pipelines because their
sources, schemas, and native cadences differ. K-index observations describe
three-hour intervals for a requested location. OMNI provides minute-level,
parameter-oriented measurements. Each source passes through the same broad
trust-building stages before the pipelines meet at dataset assessment.

The complete architecture is shown in the
[data-readiness architecture diagram](../specs/presentation.svg).

The stages have distinct responsibilities:

- **Raw** preserves what an ingestion run returned, along with its request and
  run metadata. It is evidence and is not cleaned in place.
- **Audit** consolidates successful runs while retaining provenance and
  source-reported absences, fills, duplicates, and disagreements.
- **Canonical** reconciles audit history into one downstream record per key
  while retaining quality and conflict information.
- **Coverage** compares a request's expected time grid with canonical records
  and identifies missing, null, or conflicting requirements.
- **Eligibility** applies explicit policy to those findings and returns either
  readiness or contextual issues. It does not silently repair or re-ingest
  data.

### Forecast-origin contract

A modelling request is expressed as a sequence of forecast origins rather than
as one universal timestamp shared by both datasets. For each origin `t`:

- the target is the half-open K-index interval `[t, t + 3 hours)`;
- requested K-index lags occupy consecutive three-hour slots before `t`; and
- OMNI predictors come from the half-open lookback window
  `[t - lookback, t)`.

This contract preserves the meaning and cadence of each source. It also makes
the information boundary explicit: predictor windows end at the forecast
origin, while the target describes the interval beginning there. The current
coverage system verifies whether these source requirements are represented;
constructing and aggregating the final modelling features is the next stage.

## Roadmap

### Now — data readiness complete

- K-index ingestion with immutable run artifacts.
- K-index audit and canonical tables with provenance and conflict handling.
- OMNI ingestion with parameter and time-window metadata.
- OMNI audit and canonical long tables with fill and conflict handling.
- Native-cadence K-index and OMNI coverage reporting.
- Request-level dataset assessment, input fingerprinting, persisted evidence,
  and human-readable summaries.
- Contract tests and user-facing entrypoints for the implemented pipeline.
- A public architecture diagram and read-only dashboard demonstrating lineage
  and readiness without API credentials.

### Next — model-ready data and feature engineering

- Construct the wide modelling dataset for an eligible request.
- Finalize target semantics and leakage-safe information boundaries.
- Explore the distributions, missingness, conflicts, and relationships in the
  candidate observations.
- Engineer and compare K-index lag features and OMNI lookback summaries without
  forcing minute-level observations into a premature fixed aggregation.
- Establish a reproducible split and evaluation design suitable for ordered
  time-series data.

### Later — modelling and delivery

- Select the modelling formulation using evidence from exploratory analysis.
- Establish transparent baselines and evaluation metrics.
- Train and compare candidate models, then track reproducible experiments.
- Package approved model artifacts with their feature and data contracts.
- Consider a local or deployed serving interface only after offline behavior is
  sufficiently understood and validated.

## Boundaries and Claims

The current system does not:

- train or select a machine-learning model;
- produce K-index forecasts or operational space-weather warnings;
- establish that the candidate OMNI parameters are predictive;
- claim scientific validation or state-of-the-art forecasting performance; or
- expose a public prediction service.

The public dashboard uses frozen examples whose displayed numeric observations
have been modified for demonstration. They are not historical measurements and
must not be used for scientific analysis or operational forecasting. A separate
constructed K-index conflict illustrates reconciliation behavior and is clearly
identified as synthetic.

The project links to the BoM Space Weather API as its K-index source but does
not assert an unconfirmed licence for reuse. Source attribution and public
fixture limitations belong in the README and dashboard alongside the examples.

## Supporting Documentation

This document is the repository's high-level source of truth for project
direction, scope, and milestone status. Detailed behavior belongs in the
relevant specifications:

- [Coverage reporting](../specs/spec-08-coverage-reporting.md) defines the
  native-cadence K-index and OMNI coverage contracts.
- [Dataset assessment](../specs/spec-09-dataset-assessment.md) defines request
  validation, sample planning, eligibility, persistence, and presentation.
- [Public presentation bundle](../specs/spec-presentation.md) defines the
  architecture asset, dashboard, frozen examples, disclosures, and attribution.

The README should remain the concise public entry point and keep its status
checklist aligned with the roadmap above. Specifications define feature
contracts, ADRs record durable design decisions, and this vision guides
priorities when future work introduces competing choices.
