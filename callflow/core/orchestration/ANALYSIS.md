# CallFlow performance refactor — code-verified dependency analysis

> Scope: application-level orchestration only. No Trino SQL, Smartcare SQL,
> indexes, schemas, or execution plans are changed (see request §12).
> This document is the "analyze the real code before implementing" step (§11).

## 1. How the code executes today (verified, not assumed)

There are **two** real execution flows in the product, plus one unrelated
public API (`NCI`). Both product flows are effectively **Smartcare-first**.

### Flow A — main page (`user-input-form` → `summary`)

Frontend `user-input-form.component.ts::onStartAnalyzing()`:

```
runSmartcareJob(payload)                     # Smartcare retrieval (slow)
  └─ on DONE: read result.suspected_cells_raw
       └─ runNppMinutesJob({...payload, suspected_cells_raw})   # NPP
            └─ on DONE: router.navigate('/summary')
```

So NPP is started **only after** Smartcare finishes, purely because the
frontend takes `suspected_cells_raw` out of the Smartcare job result
(`handleSmartcareUpdate`). This is a literal `Smartcare → NPP` serialization.

Backend building blocks (each is a `JobBackedAPIView`, retrieval **and**
analysis run in the same background thread — `views/job_base.py::_run_safely`):

- `views/smartcare.py` — input `msisdn_list`. `fetch_smartcare_nodes_data`
  → `smartcareNodesQueries` → `execute_smartcare_queries`
  (**currently sequential**; the parallel implementation is commented out in
  `services/smartcare_query_runner.py`). Also runs `remedy_ticket()` for the
  "data" category. This is the slow node.
- `views/npp.py` — input `suspected_cells_raw` (cell names). `run_job` does
  retrieval (`nppIntegration`) + analysis (`KPIAnalyzer.analyze_npp`) and,
  on the daily path, a hub/TAC/LAC enrichment step that reads **cached
  Smartcare** (`make_smartcare_cache_key` + `cache_get`, then
  `calculate_affected_hub_taclac_metrics`).
- `views/splunk.py` — input `node`. Retrieval (`splunkIntegration`) +
  analysis (`analyze_core_node`). **No Smartcare dependency at all.**
- `views/viavi.py` — input `suspected_sites`. Retrieval only.
- `views/remedy.py` — input `suspected_sites`. Retrieval only.

### Flow B — summary page abnormal-KPI panels

Frontend `summary.component.ts::Fetch_Abnormal_KPI()` fires three jobs in
parallel: `runAbnormalNppJob`, `runAbnormalSplunkJob`, `runAbnormalViaviJob`.

Each backend job is `abnormal_kpis.py::BaseAbnormalKPIsAPIView.run_job`, which
**always** does, in order:

```
fetch_smartcare_nodes_data(...)              # Smartcare retrieval FIRST
  → derive cells / sites from smartcare processed_data
      → _process_npp / _process_viavi / _process_splunk   # retrieval + analysis
```

- `_process_npp`: `cells_raw = smartcare_data['processed_data']['cell_name']`
  — **cells come from Smartcare, not Trino.**
- `_process_viavi`: `suspected_sites` from
  `process_sitename(smartcare processed_data)` — **sites from Smartcare.**
- `_process_splunk`: fixed node list `["SGSN","MME","GGSN","DSC","DNS"]`
  (runs nodes concurrently), but the `impacted_subs` mapping
  (`map_splunk_kpi_to_smartcare`) needs Smartcare.

So within every abnormal job, **retrieval is gated on Smartcare** even though
only the *analysis/enrichment* actually needs Smartcare.

### Trino is built but not wired into either product flow

`views/smartcare_trino.py` + `utils/trino_client.py::run_trino_query` +
`utils/mapping_cell_name.py::map_cell_ids` implement **Trino → cell_name**.
This endpoint exists and works standalone, but neither Flow A nor Flow B calls
it — both still source cell names from Smartcare. This is exactly the mismatch
§11 warns about: the intended "Trino → cell names" split is **not yet** how the
wired code behaves.

### Site names are derivable from cell names alone

`utils/data_utils.py::process_sitename(df, 'cell_name', log)` computes
`modified_site_name` purely by string-parsing `cell_name`. It does **not**
need any Smartcare-specific column, so **sites can be derived from Trino cell
names** without waiting for Smartcare.

## 2. Per-system documentation (required by §11)

### NPP
- Input for raw retrieval: **cell names** (`nppIntegration(cells, ...)`).
- Input for analysis: NPP data + Smartcare (impacted customers per cell, and
  hub/TAC/LAC via `calculate_affected_hub_taclac_metrics`).
- Current function(s): `views/npp.py::run_job`, `abnormal_kpis.py::_process_npp`.
- Current execution dependency: cells sourced from Smartcare
  (`suspected_cells_raw` / `processed_data['cell_name']`) → retrieval waits for
  Smartcare.
- Retrieval independent of Smartcare? **Yes**, if cells come from Trino.
- Analysis independent of Smartcare? **No** (enrichment needs Smartcare).
- Refactor: source cells from Trino; start NPP retrieval immediately; defer the
  hub/TAC/LAC + impacted-customer enrichment until Smartcare is ready.

### Splunk
- Input for raw retrieval: **node name** (fixed core-node list).
- Input for analysis: Splunk data; **+ Smartcare only in the abnormal flow**
  for `impacted_subs` mapping. The standalone `views/splunk.py` needs no
  Smartcare at all.
- Current function(s): `views/splunk.py::run_job`, `abnormal_kpis.py::_process_splunk`.
- Current execution dependency: in abnormal flow, sits behind the up-front
  Smartcare fetch even though retrieval needs only node names.
- Retrieval independent of Smartcare? **Yes**.
- Analysis independent of Smartcare? Standalone **Yes**; abnormal **No**.
- Refactor: start Splunk retrieval immediately (node names); do the
  Smartcare-based `impacted_subs` mapping only in the analysis stage.

### Viavi
- Input for raw retrieval: **site names** (`viaviQueries(conn, ..., suspected_sites)`).
- Input for analysis: Viavi data + Smartcare (impacted customers per site).
- Current function(s): `views/viavi.py::run_job`, `abnormal_kpis.py::_process_viavi`.
- Current execution dependency: sites derived from Smartcare `processed_data`.
- Retrieval independent of Smartcare? **Yes** — sites derive from cells
  (Trino → cells → `process_sitename`).
- Analysis independent of Smartcare? **No**.
- Refactor: derive sites from Trino cells; start retrieval immediately; defer
  impacted-customer analysis until Smartcare is ready.

### Remedy
- Input for raw retrieval: **site names** (`remedyTicketsData(conn, ..., suspected_sites)`).
- Input for analysis: Remedy data + Smartcare (`subs_count` per site via
  `calculate_top_remedy_with_subs`).
- Current function(s): `views/remedy.py::run_job`; also inline inside
  `smartcare.py::run_job` (`remedy_ticket`) for the data category.
- Current execution dependency: sites from Smartcare; in Flow A, Remedy is run
  *inside* the Smartcare job.
- Retrieval independent of Smartcare? **Yes** — sites from Trino cells.
- Analysis independent of Smartcare? **No**.
- Refactor: derive sites from Trino cells; retrieve tickets immediately; attach
  `subs_count` in the analysis stage once Smartcare is ready.

## 3. Final dependency table (verified against code)

| System    | Raw Data Requires | Analysis Requires        | SC required for retrieval? | SC required for analysis? |
| --------- | ----------------- | ------------------------ | -------------------------- | ------------------------- |
| Trino     | msisdn + window   | N/A                      | No                         | No                        |
| Smartcare | msisdn + window   | Smartcare processing     | No                         | N/A                       |
| NPP       | Cell names (Trino)| NPP data + Smartcare     | No                         | Yes                       |
| Splunk    | Node name (fixed) | Splunk data (+ SC*)      | No                         | Yes* (abnormal only)      |
| Viavi     | Site name (←cells)| Viavi data + Smartcare   | No                         | Yes                       |
| Remedy    | Site name (←cells)| Remedy data + Smartcare  | No                         | Yes                       |

`*` The standalone Splunk endpoint needs no Smartcare even for analysis; only
the abnormal-KPI Splunk path uses Smartcare (for `impacted_subs`). This is the
one place the code is stricter/looser than the request's table, so it is called
out explicitly.

This matches the request's table (§13) except for the Splunk nuance above.

## 4. Target execution model (data-availability driven)

```
                         START
             ┌─────────────┼───────────────┐
             ▼             ▼               ▼
        trino_cells    smartcare       splunk_retrieval   (node names, immediate)
             │         (slow, but          │
        ┌────┴────┐     no longer          │
        ▼         ▼     a gate)            │
   npp_retrieval sites                     │
        │      ┌──┴───┐                    │
        │      ▼      ▼                    │
        │  viavi_ret remedy_ret            │
        └───────┴──────┴─────────┐         │
                                 ▼         ▼
                        (each system: retrieval done AND smartcare done)
                                 ▼
                 npp_analysis / viavi_analysis / remedy_analysis / splunk_analysis
                                 ▼
                          FINAL INSIGHTS → dashboard → RCA files
```

Principle (§9): **run each operation as soon as all of its inputs exist** —
retrievals depend only on Trino/cells/sites/node; analyses additionally depend
on Smartcare. Smartcare's slowness no longer blocks retrieval; it only bounds
the analysis stage.

## 5. What is implemented here

- `scheduler.py` — a small, dependency-driven task engine (stdlib only): runs
  each task as soon as its declared dependencies have completed, on a thread
  pool, with per-task timeout, error isolation, upstream-result passing,
  structured logging, and a progress callback for `job_manager`/frontend status.
- `pipeline.py` — declares the CallFlow DAG above using that engine and the
  **existing** service functions (Trino, Smartcare, NPP, Viavi, Remedy, Splunk,
  `KPIAnalyzer`). Data sources are injectable so the wiring is unit-testable
  without a live JVM/DB. `run_callflow_analysis(...)` is the drop-in
  orchestration entry point a view can call.
- `tests/` — prove the behavioral contract: independent tasks run
  concurrently, a task waits only for its declared inputs, **retrieval does not
  wait for slow Smartcare while analysis does**, and one failing system does
  not block the others.

Wiring notes for integrators (kept backward compatible; existing endpoints are
untouched): a job-backed view can call `run_callflow_analysis` and stream the
per-task progress through the existing `job_manager` contract. Cell names should
be sourced from `smartcare_trino` (Trino) rather than from the Smartcare job so
that NPP/Viavi/Remedy retrieval can start immediately.
