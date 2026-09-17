# 2026-07-29 — K-index Preprocessing — Attempt 1

## Repository State

- Revision: `18a0276` on `main`
- Working tree: `uncommitted changes present`

## Scope

- Spec: `specs/spec-02-k-index-preproc.md`
- Sources:
  - `src/preprocess/space_weather_k_index_preproc.py`
  - `src/preprocess/space_weather_k_index_transform.py`
- Entrypoints:
  - `entrypoint/preproc_T1_k_index.py`
  - `entrypoint/transform_T1_k_index.py`
- Tests:
  - `tests/test_space_weather_k_index_preproc.py`
  - `tests/test_space_weather_k_index_transform.py`
- ADRs:
  - `docs/adr/adr-003-config-vs-cli-boundary.md`
  - `docs/adr/adr-009-raw-data-lake-manifest.md`
  - `docs/adr/adr-022-preprocessing-stages.md`
  - `docs/adr/adr-024-duckdb-for-preprocessing.md`
  - `docs/adr/adr-025-spec-driven-preprocessing.md`
  - `docs/adr/adr-026-contract-driven-tests.md`
  - `docs/adr/adr-027-config-driven-preprocessing-cli.md`
  - `docs/adr/adr-028-entrypoint-logging.md`

## Conditions

- Mode: Primarily closed-book; source consulted for Question 4
- Questions delivered: All at once

## Question 1 — Architecture and data flow

**Question**

> Trace a successful K-index observation from its raw ingestion run through the audit table and into the canonical observation table.
>
> Describe:
>
> - The responsibility of each preprocessing stage.
> - The main source modules and entrypoints involved.
> - What information each output retains or removes.
> - How incremental processing differs from a complete rebuild.

**Response**

> The first preprocessing stage takes considers all successful runs (read from the manifest, although we have 2 sources of status) from all run directories `data/01-raw/space_weather/k_index/<run_id>` and consolidates them in a parquet directory i.e. `data/02-processed/space_weather/k_index/T1`.
> -> I think `*preprocessing.py` is the src module that implements this stage?
> -> Each row stores the run-id, the site location, the valid time and recorded time, and the k-index observation.
> -> A run with empty data only gets one "sentinel" row so that all runs get audited, regardless whether kindex observation exists.
> -> Two modes are available for this: incremental processing and complete rebuild. The former only takes one run (I think the oldest run) and append it to T1 (if it exists). The latter rebuilds T1 from scratch by considering all runs in the raw data lake.
> The second preprocessing stage considers T1 and transforms into another intermediate canonical table T2 to record kindex observation for each location and time. When multiple kindex observations exists across different runs, the kindex observation associated with the latest run is picked. An additional flag is added to indicate whether distinct kindex observations are detected across different runs for each location and time.
> -> I think `*transform.py` is the src module that implements this stage?

**Grade:** Partially correct

**Assessment**

You correctly recalled the central two-stage architecture. The audit table contains successful-run observations and one sentinel for a successful empty run, while the canonical table chooses the latest-run value for each observation key and records cross-run disagreement. You also correctly distinguished one-run-at-a-time incremental processing from a rebuild over all successful runs.

Important details were missing or uncertain:

- Manifest status is authoritative for preprocessing; the marker is supplementary rather than an equal second source of success.
- The source modules are `space_weather_k_index_preproc.py` and `space_weather_k_index_transform.py`, and the corresponding entrypoints are `preproc_T1_k_index.py` and `transform_T1_k_index.py`.
- The raw run directory is shaped as `run_id=<run_id>`.
- The audit timestamp you called "recorded time" is named `analysis_time`.
- The canonical table removes both `analysis_time` and `run_id`; its exact fields are `location`, `valid_time`, `kindex`, and `flag`.
- Incremental processing selects exactly one oldest successful run not already represented in the audit table. A rebuild overwrites the audit table from all successful runs.

**Evidence**

- Contract: `specs/spec-02-k-index-preproc.md:23-39`, `specs/spec-02-k-index-preproc.md:52-60`, `specs/spec-02-k-index-preproc.md:76-93`, `specs/spec-02-k-index-preproc.md:97-112`, `specs/spec-02-k-index-preproc.md:127-136`
- Implementation: `src/preprocess/space_weather_k_index_preproc.py:146-171`, `src/preprocess/space_weather_k_index_preproc.py:353-431`, `src/preprocess/space_weather_k_index_transform.py:94-155`
- Entrypoints: `entrypoint/preproc_T1_k_index.py:70-136`, `entrypoint/transform_T1_k_index.py:46-90`
- Test evidence: `tests/test_space_weather_k_index_preproc.py:495-595`, `tests/test_space_weather_k_index_transform.py:287-315`
- Inference: None; these responsibilities and schemas are explicit repository contracts.

## Question 2 — Execution trace

**Question**

> Assume the raw lake contains these runs in chronological order:
>
> - Run A: `SUCCESS`, Darwin observation at time X with K-index `3`.
> - Run B: `FAILED`, Darwin observation at time X with K-index `8`.
> - Run C: `SUCCESS`, Melbourne, but with no observations in its chunk files.
> - Run D: `SUCCESS`, Darwin observation at time X with K-index `6`.
>
> Trace a complete rebuild followed by canonicalization.
>
> Describe the rows expected in the audit table and canonical table, including how Run B, Run C, duplicate Darwin observations, and the consistency flag are handled.

**Response**

> A complete rebuild would have created the following audit table (T1): [NOTE: I recall 2 timestamps that come with each observation: the valid time, which is the 3-hour period in which the kindex is asscoiated, and the record time, which is the time the observation is recorded but i think it is not referred as record time]
> -> Run A will be considered; will contain observation similar to `(run-id-A, Darwin, X, record time, 3)`
> -> Run B will not be considered in the audit T1 table (FAILED status),
> -> and one sentinel row is reserved for run C (run id C with location melbourne but null kindex observation)
> -> Run D will be considered; will contain observation similar to `(run-id-A, Darwin, X, record time, 6)`
> In the canonical table we have: (`Darwin, X, record time, 6, True`) where the last entry corresponds to the value consistency flag because kindex for Darwin at time X are inconsistent across runs.

**Grade:** Partially correct

**Assessment**

You correctly excluded failed Run B, retained successful empty Run C as an audit sentinel, chose Run D's later value of `6`, and set the Darwin consistency flag to `True`.

The expected audit rows are:

- Run A: Darwin, X, its `analysis_time`, `3`, and Run A's ID.
- Run C: Melbourne and Run C's ID, with `valid_time`, `analysis_time`, and `kindex` all null.
- Run D: Darwin, X, its `analysis_time`, `6`, and Run D's ID.

Your Run D tuple says `run-id-A`, apparently as a transcription mistake. More importantly, the **canonical Darwin row must not include the recorded/analysis timestamp**. Its fields are only Darwin, X, `6`, and `True`. The Melbourne sentinel is excluded entirely because its `valid_time` is null.

**Evidence**

- Contract: `specs/spec-02-k-index-preproc.md:97-120`, `specs/spec-02-k-index-preproc.md:127-143`, `specs/spec-02-k-index-preproc.md:242-248`
- Implementation: `src/preprocess/space_weather_k_index_preproc.py:242-268`, `src/preprocess/space_weather_k_index_transform.py:94-155`
- Test evidence: `tests/test_space_weather_k_index_preproc.py:204-252`, `tests/test_space_weather_k_index_transform.py:252-274`, `tests/test_space_weather_k_index_transform.py:330-367`
- Inference: Treating `run-id-A` in the Run D tuple as a transcription error is an inference; the canonical-schema correction is documented fact.

## Question 3 — Failure and edge-case handling

**Question**

> For each situation below, explain whether preprocessing should raise an exception, issue a warning and continue, or exit cleanly without writing output:
>
> - A manifest contains malformed JSON.
> - A manifest’s creation timestamp disagrees with its enclosing run directory.
> - A complete rebuild discovers no successful manifests.
> - Canonicalization is requested before the audit table exists.
> - The audit table contains a suspiciously formatted run ID.
> - Incremental processing finds that every successful run has already been processed.
>
> Also explain what output artifacts, if any, should remain in each case.

**Response**

> - A manifest contains malformed JSON.
>   -> raise an exception?
> - A manifest’s creation timestamp disagrees with its enclosing run directory.
>   ->
> - A complete rebuild discovers no successful manifests.
>   -> exit cleanly without writing output
> - Canonicalization is requested before the audit table exists.
>   ->exit cleanly without writing output
> - The audit table contains a suspiciously formatted run ID.
>   -> issue a warning and continue? as preprocesing should not concern about how runid is formatted
> - Incremental processing finds that every successful run has already been processed.
>   -> exit cleanly without writing output

**Grade:** Partially correct

**Assessment**

You correctly identified three behaviors:

- Missing audit input during canonicalization is a logged clean no-op with no canonical output.
- A suspicious run ID produces a warning and canonicalization continues.
- Fully up-to-date incremental processing returns without another audit-table write.

Malformed manifest JSON does raise `PreprocessSpecError`, but the answer was tentative. A creation timestamp that disagrees with its `run_id=...` directory also raises `PreprocessSpecError`; this case was unanswered. A rebuild with no successful manifests does not exit cleanly: it raises `PreprocessSpecError` before writing a rebuilt audit table.

For these validation failures, the preprocessing operation does not intentionally create its target output. Existing raw data and any pre-existing processed table are not deleted by the validation paths. When a suspicious run ID merely warns, canonicalization continues and can write its normal output.

**Evidence**

- Contract: `specs/spec-02-k-index-preproc.md:145-165`, `specs/spec-02-k-index-preproc.md:167-188`
- Implementation: `src/preprocess/space_weather_k_index_preproc.py:40-60`, `src/preprocess/space_weather_k_index_preproc.py:408-410`, `src/preprocess/space_weather_k_index_preproc.py:364-390`, `src/preprocess/space_weather_k_index_transform.py:257-304`
- Test evidence: `tests/test_space_weather_k_index_preproc.py:583-595`, `tests/test_space_weather_k_index_transform.py:404-430`
- Inference: Preservation of a pre-existing processed table follows from these failures occurring before the shown write calls; the spec explicitly requires no new output for missing audit input but does not define rollback of older artifacts.

## Question 4 — Spec, source, and test consistency

**Question**

> Review how the repository handles a successful ingestion run containing no observations.
>
> Compare the specification, preprocessing source, and tests. Explain:
>
> - How the run remains auditable.
> - Why it must not become a canonical observation.
> - What prevents it from producing multiple placeholder rows.
> - Whether the spec, source, and tests agree; identify any inconsistency you find.

**Response**

> - the successful empty run will still show up in the audit table with null kindex observation.
> - 
> - (I sps this prevention logic is handled at `build_t1_select_sql`) This might be because we do a left join from all successful runs to their chunks. If the corresponding chunks is empty (for chunked requests but turns out all of them are empty) then they will not appear in RHS of the LEFT JOIN, which means then empty successful runs will yield exactly one observation due to the left join. SIde note: though I had to view the source code for this -- if it is not already listed on the spec I think we can list this.
> - 

**Grade:** Partially correct

**Assessment**

You correctly identified the manifest-driven left join. One successful manifest contributes one left-side row; zero matching observation rows therefore produce exactly one audit sentinel with null observation fields. This implementation explanation is already represented at the contract level in the spec, although the spec intentionally does not prescribe the exact SQL technique.

Two requested parts were omitted:

- The sentinel **must not become a canonical observation because it has null `valid_time`**, and canonicalization explicitly filters such rows before grouping.
- You did not compare all three surfaces or state whether they agree.

The spec and source agree on one sentinel per successful empty run and its later exclusion. The tests verify a successful run with one empty chunk and verify sentinel exclusion from the canonical table. There is no observed behavioral disagreement. There is, however, a coverage gap: the fixture deliberately creates only one chunk per run, so the stronger edge case of multiple empty chunk files is not directly exercised even though the source logic is designed to handle it.

**Evidence**

- Contract: `specs/spec-02-k-index-preproc.md:97-120`, `specs/spec-02-k-index-preproc.md:127-135`, `specs/spec-02-k-index-preproc.md:157-165`, `specs/spec-02-k-index-preproc.md:416-432`
- Implementation: `src/preprocess/space_weather_k_index_preproc.py:174-269`, `src/preprocess/space_weather_k_index_transform.py:117-155`
- Test evidence: `tests/test_space_weather_k_index_preproc.py:92-94`, `tests/test_space_weather_k_index_preproc.py:204-252`, `tests/test_space_weather_k_index_preproc.py:386-445`, `tests/test_space_weather_k_index_transform.py:330-367`
- Inference: Calling the absent multi-empty-chunk case a test-coverage gap is an examiner assessment; it is not a documented source/spec disagreement.

## Question 5 — Design rationale and OMNI transfer

**Question**

> Why does the project preserve a separate audit-oriented table instead of immediately deduplicating raw runs into canonical observations?
>
> Then consider the planned OMNI preprocessing pipeline:
>
> - Which K-index preprocessing principles should carry over?
> - Which parts cannot be copied directly?
> - At what stage should OMNI fill-value placeholders become null values, and why?
> - What provenance would you retain before constructing the canonical OMNI observation table?

**Response**

> To check just in case kindex reuqests for the same location & time are inconsistent
>
> - Perhaps we can also incorporate those two stages (consolidate all observations including duplicates across runs before developing a canonical table.
> - i can think of some things:
>   -> We cannot read the data directly as we need information from /info. First things first, we might need to match observations returned from /data (json chunks) to the parameters first from _manifest.json
>   -> if each row of the canonical table represents observations of omni data parameters ingested so far, then we may have a jagged canonical table: you might have a request on a certain time frame with a larger parameter set and a request  on another time frame with a different, perhaps smaller parameter set. One crude workaround might be to create a placeholder stating that data for a time corresponding to a omni parameter variable has not been ingested yet (which is different from a missing value placeholder).
> - We haven't even decided yet on what the canonoical table should look like due to the previous (potential) issue ive identified ahead of time, but i think i might replace missing value placeholders right after we have made the canonical table, because i am afraid that we may have intermediate null values and so if we replace in advance then we cannot distinguish those null values from those arisen from say joins.

**Grade:** Partially correct

**Assessment**

You recognized the most important transferable shape: first preserve observations and duplicates across runs, then construct a canonical table. You also identified a genuine OMNI design problem: separate runs may request different parameter sets, so absence because a variable was not requested must not be confused with a source fill value. Your concern about preserving different reasons for missingness is strong.

The K-index rationale is broader than checking inconsistent values. The audit layer preserves raw-run provenance and successful-empty runs before deduplication removes information. Raw artifacts remain immutable, while canonicalization can be regenerated under explicit latest-run and consistency rules.

One factual correction is needed: positional OMNI parameter definitions come from the saved `hapi_info.json` and are also repeated in each complete chunk payload. The manifest records requested parameter names and artifact metadata, but it is not the sole parameter dictionary used to interpret each array position.

The exact OMNI preprocessing schema and fill-replacement stage are not yet specified, so that part of your answer is design inference rather than a documented error. A strong future contract would likely replace source fill codes as part of canonical-table construction, while retaining explicit provenance or missingness-reason fields so source-missing, not-requested, and join-generated nulls remain distinguishable. Your answer did not yet specify the provenance fields to preserve, such as run ID, chunk/request boundaries, dataset ID, retrieval metadata, requested parameter set, and source fill definitions.

**Evidence**

- Documented K-index contract: `specs/spec-02-k-index-preproc.md:23-28`, `specs/spec-02-k-index-preproc.md:52-59`, `specs/spec-02-k-index-preproc.md:125-136`
- Documented OMNI ingestion boundary: `specs/spec-05-ingest-omni-dataset.md:22-36`, `specs/spec-05-ingest-omni-dataset.md:64-67`, `specs/spec-05-ingest-omni-dataset.md:183-190`, `specs/spec-05-ingest-omni-dataset.md:209-223`
- Documented OMNI payload interpretation: `specs/spec-05-ingest-omni-dataset.md:313-345`, `specs/spec-05-ingest-omni-dataset.md:347-360`
- Inference: The proposed canonicalization-time replacement and missingness-reason fields are recommendations. The current OMNI ingestion spec only requires preserving fill placeholders unchanged for later preprocessing.

## Attempt Summary

- Correct: 0
- Partially correct: 5
- Incorrect: 0

## Concepts for Later Review

1. **Exact audit and canonical schemas** — Recall `analysis_time` in the audit layer and its removal, together with `run_id`, from the canonical layer.
2. **Failure taxonomy and artifact effects** — Distinguish fatal manifest/rebuild violations from warning-only and clean no-op paths.
3. **Empty-run and missingness semantics** — Connect manifest-driven sentinels, null-`valid_time` exclusion, and OMNI's distinct source-missing versus not-requested cases.
