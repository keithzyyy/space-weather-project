---
status: Draft
owner: Keith
branch: docs/presentation-layer
related_adrs: []
related_specs:
  - specs/spec-02-k-index-preproc.md
  - specs/spec-06-omni-preproc.md
  - specs/spec-07-omni-canonical.md
  - specs/spec-09-dataset-assessment.md
supersedes: []
---

# Spec: `public-presentation-bundle`

## 1. Purpose

Provide a credential-free, read-only presentation of the project's current
data-readiness milestone. A general technical reader should be able to
understand how K-index targets and OMNI predictors move from saved source
responses through audit and canonical tables into a modelling-dataset
assessment without first reading the implementation specifications.

The public presentation bundle consists of:

- one editable architecture diagram and its exported SVG;
- a five-page Streamlit dashboard;
- frozen, modified demonstration source fixtures;
- regenerated audit, canonical, and assessment artifacts;
- provenance and attribution notes; and
- a public README that links the architecture and explains current scope.

This is a retrospective MVP contract for the implementation already present in
`dashboard/` and `examples/dashboard/`. It makes that implementation reviewable
and defines what future presentation changes must preserve.

This feature does not perform ingestion, preprocessing, canonicalization,
coverage calculation, dataset construction, feature engineering, modelling,
or forecasting. It does not accept arbitrary user paths, make programmatic
requests to BoM or NASA, disclose exact fixture-masking transformations, or
claim that the displayed values are suitable for historical, scientific, or
operational use.

## 2. Context Check

- Specs 02, 06, and 07 define the audit and canonical semantics represented by
  the lineage pages.
- Spec 09 defines the assessment request, persisted artifact bundle, and
  eligibility meaning represented by the assessment page.
- The ordinary runtime raw lake remains immutable. The source files under
  `examples/dashboard/` are explicitly modified presentation copies and must
  not be described as untouched ingestion runs.
- `specs/presentation.drawio` is the editable architecture source and
  `specs/presentation.svg` is its rendered public asset.
- `README.md` is the canonical public entry point for the architecture,
  dashboard, current project status, and source attribution.
- The dashboard is implemented as repository-local Streamlit modules rather
  than reusable application services. The user-visible presentation behavior
  is the primary contract.

The architecture deliberately abstracts low-level helpers. It shows two
native-cadence source lanes—three-hour K-index targets and one-minute OMNI
predictors—passing through ingestion, audit, and canonical stages before
converging at dataset-request assessment. Model-ready construction and later
ML work remain visibly marked as future work.

This specification is authoritative for presentation behavior. The Draw.io
pages and preparation notebook remain explanatory design artifacts when they
disagree with this document.

One-time branch cleanup, squash mechanics, commit identifiers, and local
staging decisions are not product behavior and remain documented only in the
presentation checkpoint.

## 3. High-Level Approach

### 3.1 Presentation flow

The application resolves the project root from `dashboard/app.py` and exposes
these sidebar pages in order:

```text
System overview
    -> K-index lineage
    -> OMNI lineage
    -> Dataset assessment
    -> Data sources & attribution
```

The System overview is the default page. Each later page includes enough local
context for a reader who did not study the overview first.

### 3.2 Data flow

```text
committed modified source fixtures
                |
                v
     committed audit artifacts
                |
                v
   committed canonical artifacts
                |
                v
 committed assessment artifact bundle
                |
                v
 focused read-only dashboard projections
```

Dashboard loaders select only the records needed for each worked example,
validate that the expected story is still present, and return
presentation-ready objects. Renderers explain and display those objects but do
not mutate or regenerate them.

### 3.3 Ownership boundaries

- Pipeline source modules remain authoritative for ingestion, audit,
  canonicalization, coverage, and eligibility semantics.
- The dashboard may calculate display-only projections and summaries from
  already persisted artifacts. It must not reimplement or change pipeline
  policy.
- The committed example bundle is authoritative for the public worked example.
- The README summarizes the feature and links to deeper evidence rather than
  reproducing the complete specifications.

## 4. Expected Behavior

### 4.1 System overview

The default page renders `specs/presentation.svg` at the available page width
with accessible alternative text. The diagram must:

- distinguish the K-index target and OMNI predictor pipelines;
- show raw, audit, and canonical stages before assessment;
- show the request inputs needed by assessment;
- show persisted assessment artifacts; and
- distinguish current data-readiness work from future modelling work.

If the SVG is missing, the page displays a visible error and stops instead of
showing an incomplete overview.

### 4.2 K-index lineage

The page introduces its own request, defines modified source response, audit
table, canonical table, run ID, canonical key, and conflict flag, then presents
three scenarios:

1. **Data returned**
   - follow one modified source-derived observation through all three stages;
   - keep project-assigned `run_id` out of the source-shaped response;
   - show one audit row and one non-conflicting canonical observation.
2. **No data returned**
   - distinguish successful request completion from returned observations;
   - show one audit sentinel with null observation fields;
   - show that no canonical observation is invented.
3. **Values disagree**
   - visibly label the later report as a constructed synthetic fixture;
   - retain both reports in audit history;
   - select the latest report canonically and set the conflict flag.

The page displays a permanent warning that its numeric K-index values are
modified, non-historical demonstration values that must not be used for
scientific analysis or operational forecasting.

### 4.3 OMNI lineage

The page explains the positional HAPI response, wide-to-long audit
transformation, canonical key, source-fill treatment, and why `BX_GSE` and
`BZ_GSE` are shown. The parameter pair is illustrative and must not be
described as scientifically optimal.

It presents three scenarios:

1. **Numeric readings**
   - project one source minute for the two displayed parameters;
   - reshape it into parameter-minute audit rows;
   - show numeric, non-fill, non-conflicting canonical rows.
2. **Source says missing**
   - show the documented HAPI fill placeholder in the source and audit views;
   - convert it to canonical null while retaining `is_source_fill=True`;
   - distinguish represented missingness from an absent parameter-minute row.
3. **No predictor values**
   - explain that the successful run requested only `Time`;
   - preserve one null-key audit sentinel;
   - exclude that sentinel from canonical observations.

The page displays the same permanent non-historical, non-scientific, and
non-operational warning as the K-index page.

### 4.4 Dataset assessment

The page explains the fixed modelling-dataset request before presenting its
result. It defines modelling sample, K-index lag, OMNI lookback, and readiness
locally.

It exposes three presentation levels derived from the same persisted bundle:

- **Summary:** readiness, request facts, K-index coverage metrics, OMNI
  coverage overview, and eligibility issue summary. Wide one-row metric tables
  may be transposed for terminal-style vertical readability.
- **Issues:** K-index gap/conflict intervals, problematic OMNI windows, and
  contextual issue records. A ready bundle displays an explicit no-issues
  message rather than an unexplained empty table.
- **Full evidence:** sample plan, all K-index interval reports, complete
  forecast-origin/parameter OMNI summary, and issue records.

The three tabs are display choices only. They do not recalculate assessment or
represent different assessment results.

### 4.5 Data sources and attribution

The page must:

- state that the application reads committed artifacts and makes no live API
  requests;
- link the official BoM Space Weather API documentation without asserting a
  specific licence for returned observations;
- cite NASA SPDF `OMNI_HRO2_1MIN`, DOI `10.48322/mj0k-fq60`, and the SPDF
  data-use policy;
- distinguish modified source-derived fixtures from the constructed K-index
  conflict; and
- state the historical, scientific, and operational-use limitations.

Hyperlinks are allowed. The application itself must not initiate network
requests.

### 4.6 README integration

The public README must:

- embed or link `specs/presentation.svg`;
- summarize raw provenance, auditability, canonical reconciliation, and
  dataset readiness;
- distinguish completed pipeline work from future modelling work;
- offer a credential-free route to the frozen demonstration;
- use the same attribution and limitation language as the dashboard; and
- avoid claiming completed forecasting performance.

## 5. Invariants

- Rendering is read-only: it creates, updates, or deletes no source, Parquet,
  manifest, assessment, configuration, or log file.
- No dashboard page calls an external API or pipeline entrypoint.
- Project-owned artifacts are resolved from a fixed project root; visitors
  cannot select arbitrary local paths.
- Every lineage page explains its own terms and does not require navigation
  back to the overview.
- Both lineage pages keep their modification warning visible outside tabs and
  collapsed sections.
- Displayed numeric observations are never called historical measurements.
- The synthetic K-index conflict is always labelled constructed and is never
  attributed to BoM.
- Exact masking mappings, offsets, and formulas are absent from dashboard and
  README content.
- Source, audit, canonical, and assessment examples remain mutually coherent
  after any fixture update.
- Source-fill cells remain represented missingness; they are not relabelled as
  absent rows.
- Successful-empty sentinels remain audit evidence and never become canonical
  observations.
- Canonical conflict flags remain independent of numeric availability.
- No API key, username, secret-bearing configuration, or absolute local path is
  present in public presentation artifacts.
- BoM attribution makes no unconfirmed licensing claim.
- NASA attribution retains the dataset identifier, DOI, and SPDF policy link.
- The editable Draw.io source, exported SVG, dashboard overview, and README
  architecture reference remain synchronized.
- Future work remains distinguishable from implemented behavior.

## 6. Edge Cases

- A successful K-index request may contain zero observations. The dashboard
  shows its audit sentinel and empty canonical result explicitly.
- A successful OMNI request may contain timestamp rows but no non-time
  parameters. It is described as having no predictor values, not as an empty
  HTTP response.
- An OMNI parameter-minute may be represented by a documented source fill.
  The dashboard preserves that distinction from an absent canonical row.
- Two K-index runs may disagree for the same canonical key. Both audit rows
  remain visible while canonicalization produces one flagged result.
- A ready assessment has an empty issue list. The dashboard renders an explicit
  success explanation instead of treating the empty result as missing data.
- Large underlying OMNI tables must not be rendered in full. The lineage page
  selects a compact parameter/time slice and the assessment page uses persisted
  summaries.
- A reader may enter the application on a lineage or assessment page. Each page
  therefore repeats the minimum terminology required to understand it.

## 7. Failure Modes

| Failure | Required handling |
| --- | --- |
| Architecture SVG missing | Display the resolved missing path, then stop the page. |
| Required source, Parquet, manifest, or provenance file missing | Display an actionable page-level error and stop dependent rendering. |
| JSON or manifest malformed | Display the loader error and stop; do not substitute fabricated content. |
| Parquet schema incompatible | Surface the pandas or DuckDB failure as a page error and stop. |
| Pinned run, timestamp, parameter, or assessment ID absent | Raise a validation error, display it, and stop. |
| Expected sentinel, source fill, count, or conflict relationship changed | Fail validation rather than presenting a stale explanation. |
| Assessment manifest status, schema version, or artifact set invalid | Reject the bundle and stop the assessment page. |
| External link unavailable | Local dashboard rendering remains usable; no automatic retry or network validation occurs. |

The dashboard does not silently select a different run or assessment bundle
when pinned artifacts fail validation.

## 8. Data Contracts

### 8.1 Public bundle layout

```text
dashboard/
    app.py
    kindex_page.py
    omni_page.py
    assessment_page.py
    data_sources_page.py
specs/
    presentation.drawio
    presentation.svg
examples/dashboard/
    source/
        kindex/raw/run_id=<run-id>/
        omni/raw/OMNI_HRO2_1MIN/run_id=<run-id>/
    derived/
        kindex/audit/
        kindex/canonical/
        omni/audit/OMNI_HRO2_1MIN/long-observations/
        omni/canonical/OMNI_HRO2_1MIN/canonical-long-table/
    assessment/
        assessment_id=<assessment-id>/
README.md
```

### 8.2 Fixed demonstration identities

| Dataset | Role | Identifier |
| --- | --- | --- |
| K-index | modified happy path | `20260307T050056Z` |
| K-index | successful empty | `20260312T004356Z` |
| K-index | constructed conflict | `20261003T080000Z` |
| OMNI | dataset | `OMNI_HRO2_1MIN` |
| OMNI | modified happy path | `20260801T021300Z` |
| OMNI | successful Time-only | `20260806T052236Z` |
| Assessment | authoritative bundle | `20261006T070041353715Z` |

The displayed OMNI parameter subset is exactly:

```text
BX_GSE
BZ_GSE
```

### 8.3 Assessment request

```text
location:                Australian region
target interval:         [2025-01-02 00:00:00,
                          2025-01-02 21:00:00)
OMNI parameters:         BX_GSE, BZ_GSE
OMNI lookback:           180 minutes
K-index lag count:       1
forecast origins:        7
OMNI summary rows:       14
expected result:         ready, with no issue records
```

The persisted bundle contains:

```text
_manifest.json
issues.json
sample_plan.parquet
kindex/summary.parquet
kindex/covered_intervals.parquet
kindex/gap_intervals.parquet
kindex/conflict_intervals.parquet
omni/summary.parquet
```

The manifest must not contain absolute local input or artifact paths in the
public copy.

### 8.4 Fixture provenance

- The modified K-index happy-run directory contains
  `_MODIFIED_FIXTURE.md`, which states that values were replaced through a
  deterministic one-to-one transformation without publishing the mapping.
- The modified OMNI happy-run directory contains `_MODIFIED_FIXTURE.md`, which
  states that non-fill values were deterministically reassigned only within
  each parameter without publishing the method.
- The synthetic K-index directory contains `_SYNTHETIC_FIXTURE.md` and manifest
  metadata identifying it as constructed.
- Modified numeric values are not historical measurements.
- Timestamps, schemas, row presence, fill positions, and the relationships
  needed by the presentation remain coherent.

### 8.5 Rebuild contract

When a public source fixture changes, every affected audit, canonical, and
assessment artifact must be rebuilt through the existing entrypoints with
paths explicitly overridden into `examples/dashboard/`. Hand-editing derived
Parquet or assessment outputs is not permitted.

The authoritative assessment must be regenerated after either canonical input
changes. The dashboard's pinned identifiers and validation expectations must
then be updated in the same change.

## 9. Interface Design

The application-level renderer interfaces are:

```python
def render_kindex_page(*, project_root: Path) -> None:
    """Render the frozen K-index lineage demonstration."""


def render_omni_page(*, project_root: Path) -> None:
    """Render the frozen OMNI lineage demonstration."""


def render_assessment_page(*, project_root: Path) -> None:
    """Render the pinned persisted dataset assessment."""


def render_data_sources_page() -> None:
    """Render source attribution and public-use limitations."""
```

`dashboard/app.py` owns project-root discovery, page configuration, overview
rendering, page registration, navigation order, and execution of the selected
page.

Loaders, focused query helpers, display projections, validation helpers, and
page-data containers remain implementation details. They may be refactored
without changing this contract as long as observable behavior and artifact
validation remain intact.

There is no CLI or configuration interface for the dashboard in this MVP.
Run it from the repository root with:

```text
python -m streamlit run dashboard/app.py
```

Streamlit remains a development/presentation dependency. A minimum supported
version is not fixed by this draft.

## 10. Test Blueprint

Tests are deferred until the public fixture shape is stable. When implemented,
use built-in `unittest` under `tests/presentation/`:

- `test_dashboard_pages.py` for isolated renderer and navigation behavior;
- `test_presentation_artifacts.py` for real committed-artifact integration;
- `test_dashboard_smoke.py` for the assembled Streamlit application.

Use small explicit page-data objects for ordinary renderer tests. Use the
committed `examples/dashboard/` bundle only for presentation-specific
integration tests, because those tests exist to prove the public example is
coherent. Do not read ignored runtime data, regenerate pipelines, call external
services, or require API credentials.

Real pandas, DuckDB, PyArrow/Parquet, filesystem reads, and Streamlit
`AppTest` are allowed. Patch objects where used. Do not assert incidental
Streamlit-generated element identifiers, dataframe styling internals, or exact
exception wording unless the wording is part of the public limitation notice.

| Test group | Test name | Level | Fixture / boundary | Patches | Minimum assertions |
| --- | --- | --- | --- | --- | --- |
| Navigation | `test_app_registers_pages_in_public_order` | Streamlit integration | Real `dashboard/app.py` and SVG | None | Overview is default; five page titles appear in the specified order; no exception. |
| Overview | `test_overview_missing_svg_stops_with_visible_error` | Renderer | Temporary missing SVG path | Patch `dashboard.app.diagram_path` and observe `dashboard.app.st.stop` | Error is visible; rendering stops; no fallback image. |
| K-index loader | `test_kindex_committed_artifacts_preserve_three_scenarios` | Filesystem integration | Real committed K-index source/audit/canonical fixtures | None | Fixed run IDs resolve; normal, empty, and conflict relationships match the contract; no fixture changes. |
| K-index renderer | `test_kindex_page_discloses_modification_and_synthetic_conflict` | Renderer | Small explicit `KIndexPageData` | Patch `dashboard.kindex_page._load_kindex_page_data` | Permanent modification warning, three scenario tabs, and constructed-conflict wording are present; exact transformation explanation is absent. |
| OMNI loader | `test_omni_committed_artifacts_preserve_numeric_fill_and_time_only_scenarios` | Filesystem integration | Real committed OMNI source/audit/canonical fixtures | None | Dataset and run IDs resolve; only the two selected parameters are projected; numeric, fill, and sentinel relationships match; no fixture changes. |
| OMNI renderer | `test_omni_page_explains_source_fill_without_calling_it_absent` | Renderer | Small explicit `OmniPageData` | Patch `dashboard.omni_page._load_omni_page_data` | Permanent warning and three tabs appear; fill is explained as represented missingness; exact masking method is absent. |
| Assessment loader | `test_assessment_committed_bundle_matches_pinned_request` | Filesystem integration | Real authoritative assessment bundle | None | ID, status, request bounds, lookback, lag, seven samples, fourteen OMNI rows, readiness, empty issues, and artifact schemas match. |
| Assessment renderer | `test_assessment_ready_bundle_renders_three_evidence_levels` | Renderer | Small explicit ready `AssessmentPageData` | Patch `dashboard.assessment_page._load_assessment_page_data` | Summary, Issues, and Full evidence tabs render; ready state and no-issues explanation are visible; input frames are unchanged. |
| Attribution | `test_attribution_page_contains_required_sources_and_limitations` | Renderer | Static page | None or isolated AppTest script | BoM documentation, OMNI dataset record, DOI, SPDF policy, modified-fixture distinction, synthetic distinction, and use limitations are present; no BoM licence claim. |
| Failure handling | `test_page_loader_failure_is_visible_and_stops_page` | Renderer; named K-index, OMNI, and assessment cases | Loader raises `FileNotFoundError`, `ValueError`, or DuckDB error | Patch `dashboard.kindex_page._load_kindex_page_data`, `dashboard.omni_page._load_omni_page_data`, or `dashboard.assessment_page._load_assessment_page_data` where used | Visible page error; `st.stop` reached; later evidence is not rendered. |
| Public hygiene | `test_public_bundle_contains_no_local_or_secret_material` | Filesystem/static integration | Public README, manifests, provenance notes, dashboard source | None | No API keys, usernames, absolute local paths, exact replacement mapping, or exact reassignment formula; required disclosures remain. |
| Cross-artifact consistency | `test_readme_dashboard_and_architecture_references_are_synchronized` | Filesystem/static integration | README, app constants, SVG, fixture notes | None | README references the SVG; dashboard identifiers point to existing artifacts; attribution identifiers and DOI agree. |
| Smoke | `test_streamlit_application_runs_without_mutation_or_external_calls` | Streamlit integration | Real application and committed bundle; artifact metadata recorded before run | None; statically reject network and pipeline-entrypoint imports from `dashboard/` | AppTest has no uncaught exception; forbidden imports are absent; artifact paths, sizes, and modification times are unchanged. |

Do not create a shared fixture module initially. Add one only if explicit small
page-data builders are genuinely reused by more than one test module.

## 11. Notebook Implementation Notes

`notebooks/11_presentation_prep.ipynb` was used to:

- choose one coherent assessment request before selecting source runs;
- identify happy and successful-empty runs for both sources;
- confirm the target-plus-lag and OMNI lookback requirements;
- design a compact public example; and
- inspect the artifacts before the dashboard implementation.

The notebook remains a selection and provenance scratchpad. Production
presentation behavior belongs in `dashboard/`, and persisted example artifacts
belong under `examples/dashboard/`. The notebook is not a runtime dependency
of the application or future tests.

## 12. Acceptance Criteria

The public presentation MVP is complete when:

- the five pages appear in the specified order and the overview is default;
- the architecture SVG renders from the repository-relative path;
- K-index and OMNI pages independently explain their source, audit, and
  canonical stories;
- all fixed source, derived, and assessment artifacts pass loader validation;
- the assessment page presents Summary, Issues, and Full evidence from the one
  authoritative ready bundle;
- modification, synthetic-fixture, attribution, and use limitations are
  visible and mutually consistent;
- no exact masking transformation, secret, username, or absolute local path is
  exposed;
- no dashboard action writes artifacts, invokes a pipeline, or calls an
  external API;
- the README and architecture asset reflect the same implemented-versus-future
  boundary; and
- the deferred hybrid test matrix is implemented and passing before this spec
  advances from `Draft` to an accepted/completed status.

## 13. Open Questions

Questions deferred beyond this specification pass:

1. Is the current modification, attribution, and limitation approach
   sufficient if BoM does not provide explicit API-response redistribution
   guidance?
2. Should the month-scale OMNI fixtures remain committed verbatim, or should a
   later release use smaller reproducible extracts?
3. Should a blocked assessment example be added after the single ready story is
   stable?
4. Should Streamlit remain in `requirements-dev.txt` or move to a dedicated
   dashboard requirements file, and what minimum version should be supported?
5. Should the read-only application be deployed to Streamlit Community Cloud?
6. Should later dashboard versions expose additional display-only filtering or
   summaries without duplicating pipeline policy?
