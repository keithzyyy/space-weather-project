# Project Vision

This project is a production-style K-index/Kp disturbance prediction service. The MVP should tell one coherent story: ingest historical space-weather data, preprocess it into model-ready features, train an offline model, track the experiment, save a model artifact, and serve predictions through a local containerized API.

The goal is not to build a state-of-the-art geomagnetic forecasting system. The goal is to practice a realistic machine-learning system shape: reproducible data flow, explicit artifacts, experiment tracking, tested contracts, and a small prediction service.

## MVP Definition Of Done

The MVP is done when:

- A historical dataset can be ingested and preprocessed into model-ready features.
- A training command can train from a fixed historical dataset.
- The training run creates a saved model artifact and enough metadata to serve it safely.
- The training run logs a lightweight experiment record to Weights & Biases.
- A serving API can load the saved artifact and return a prediction.
- Docker can run the serving API locally without relying on the local Conda environment.
- Tests cover the main contracts for data, features, training, experiment tracking, artifact loading, and API prediction behavior.
- This document and the README explain what the system does, what "production-style" means here, and what is intentionally out of scope.

## What Production-Style Means Here

Production-style means the project is built like a small deployable ML service:

- Runtime behavior is config-driven, with per-run choices passed through CLI arguments where appropriate.
- Secrets are not committed.
- Data contracts are explicit and tested.
- Raw data is preserved, and downstream stages create new artifacts instead of mutating raw records.
- Model artifacts and feature metadata are saved explicitly.
- Training and serving use compatible feature logic.
- The serving API can run in a clean Docker environment.

Production-style does not mean cloud-scale production for the MVP. The MVP does not require high availability, authentication, rate limiting, automated retraining, online learning, cloud deployment, or enterprise monitoring.

## Training Versus Serving Stance

The primary MVP objective is serving predictions through an API. Training is still required, but it exists to produce a model artifact that the serving API can load.

Training responsibilities:

- Build features from historical data.
- Train and evaluate one model objective.
- Save the model artifact and feature metadata.
- Log a lightweight experiment record to Weights & Biases.

Serving responsibilities:

- Load an existing model artifact.
- Accept prediction input through an API request.
- Build or validate the required feature shape.
- Return a prediction response.
- Run without requiring Weights & Biases to be available at prediction time.

## MVP Flow

```text
historical data
  -> preprocessing
  -> features
  -> offline training
  -> tracked experiment
  -> saved artifact
  -> prediction API
  -> Dockerized local serving
```

Every must-have requirement should support this flow. If a proposed feature does not help this flow, it should usually be `Should have`, `Could have`, or out of scope for the MVP.

## MoSCoW Matrix

| Stage | Requirement | Status | Definition of done |
|---|---|---|---|
| Must have | Historical ingestion and preprocessing for K-index/Kp target data | Partially adopted | Historical observations can be ingested, raw data is preserved, and preprocessing produces clean target data for modelling. |
| Must have | Feature engineering for one fixed modelling objective | Not started | A reproducible feature table can be generated for training and reused or validated by serving. |
| Must have | One modelling objective | Adopted as scope | The project targets short-term K-index/Kp disturbance prediction, initially as direct regression or threshold classification. |
| Must have | Offline train/evaluate/save pipeline | Not started | A command trains from fixed historical data, evaluates the model, and writes a model artifact plus metadata. |
| Must have | Simple Weights & Biases experiment tracking | Not started | Training logs config, metrics, feature schema or feature list, objective details, artifact reference, run ID, and timestamp. |
| Must have | Saved model artifact with serving metadata | Not started | Serving can load the artifact without retraining and can verify the expected feature shape. |
| Must have | FastAPI prediction endpoint | Not started | A local API endpoint loads the saved model and returns prediction output for valid input. |
| Must have | Dockerized local serving API | Not started | Docker can run the API in a clean Python runtime without relying on local Conda. |
| Must have | Contract-focused tests | In progress | Tests cover data contracts, feature shape, training artifact creation, W&B logging boundary, and API prediction response. |
| Should have | Docker Compose local demo | Not started | Compose can run the API service and optionally a one-shot training service. |
| Should have | Detailed entrypoint logging | Partially adopted | Entrypoints follow the shared running/success/error logging lifecycle. |
| Should have | Basic prediction request logging | Not started | Prediction requests record enough context to debug inputs, model version, and outputs without leaking secrets. |
| Should have | Basic model evaluation report | Not started | Training writes a readable local report with the key metrics and dataset split details. |
| Should have | BoM API 10k-record truncation guardrail | Not started | Ingestion warns when a requested window risks API truncation. |
| Should have | README reproducible local demo | Not started | README explains the MVP flow and the commands to train, serve, and test locally. |
| Could have | Simple CI | Not started | Tests run automatically on push or pull request. |
| Could have | W&B sweeps or richer experiment comparison | Not started | Hyperparameter search or richer comparison is available, but not required for the MVP. |
| Could have | Online evaluation after delayed labels arrive | Not started | Predictions can later be compared with observed labels when they become available. |
| Could have | Drift checks | Not started | Basic feature or prediction drift checks are produced after the MVP is stable. |
| Could have | Monitoring dashboard | Not started | A small dashboard shows predictions, metrics, or drift summaries. |
| Could have | Hybrid regression/classification for high K-index/Kp events | Not started | The project evaluates whether combined modelling improves high-disturbance handling. |
| Could have | Cloud deployment | Not started | The API can be deployed beyond the local machine. |
| Won't have for MVP | Automated retraining service | Deferred | Retraining remains a manual command during the MVP. |
| Won't have for MVP | Online learning | Deferred | The model is not updated continuously from live observations. |
| Won't have for MVP | Airflow-style orchestration | Deferred | The MVP avoids a full workflow orchestrator. |
| Won't have for MVP | Enterprise-grade production monitoring | Deferred | Monitoring stays lightweight and local until the core service is coherent. |
| Won't have for MVP | Authentication, rate limiting, and multi-user API hardening | Deferred | The API is a local demo service, not a public multi-user product. |
| Won't have for MVP | State-of-the-art space weather forecasting claims | Deferred | The model is evaluated honestly as a project artifact, not positioned as operational scientific forecasting. |

## Weights & Biases Boundary

Weights & Biases is a must-have for lightweight training experiment tracking only.

Minimum W&B tracking:

- Training config snapshot.
- Train, validation, and test metrics.
- Model objective and threshold, if classification is used.
- Feature list or feature schema version.
- Model artifact path or W&B artifact reference.
- Run ID and timestamp.

Not required for the MVP:

- W&B sweeps.
- W&B model registry promotion workflow.
- Automated retraining triggers.
- Online production monitoring through W&B.
- Dashboard polish beyond being able to inspect runs.

The serving API must not require W&B at prediction time. W&B belongs to training and evaluation; serving should load a saved artifact and run independently.

## Maintenance Rules

- Keep this document as the repo-side source of truth for project scope and MVP done-ness.
- Keep Notion as a planning mirror or backlog unless a future decision explicitly makes Notion canonical again.
- Use specs for feature behavior, ADRs for durable design decisions, and this vision document for priority and tiebreakers.
- Update the MoSCoW matrix whenever a major feature changes stage or status.
- If a new requirement becomes `Must have`, confirm that it supports the MVP flow before promoting it.
- If a project decision changes what "production-style" means, capture the durable reasoning in an ADR and update this document.
