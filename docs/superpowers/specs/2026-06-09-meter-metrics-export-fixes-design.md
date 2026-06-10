# Design: meter metrics export fixes (name mangling, phantom zero, histogram OTLP)

- **Date:** 2026-06-09
- **Library:** `instrument`
- **Baseline:** `be7e75e` (1.1.3)
- **Status:** partially superseded — §5 (the meter fix) is replaced by `2026-06-10-meter-row-store-design.md`, which fixes the bugs in storage rather than at the render layer. §6 (PR 2, histogram OTLP) is final and shipped as upstream PR #10.
- **Supersedes / corrects:** `INVESTIGATION-meter-attributed-name-mangling.md`
- **Delivery:** two independent upstream bugfix PRs to `benoitc/instrument` (PR 1 and PR 2 below). Either can land first.

---

## 1. Summary

Two independent defects make the OTel-style **meter** API unusable for anyone exporting metrics:

- **Bug 1 — attributed meter writes don't land under the registered name, and a phantom zero series appears.** `create_counter(M, <<"requests_total">>)` + `add(C, 1, #{method=>…, status=>…})` exports the real data under a *derived* name and leaves the registered name as a constant-zero, no-attributes series.
- **Bug 2 — the histogram OTLP encoder crashes and (once un-crashed) emits wrong bucket data.** Any histogram present drops the entire OTLP metrics batch; fixing the crash alone would then emit cumulative bucket counts where OTLP requires per-bucket deltas.

This document specifies the fix for each as a separate PR. The two share no code change.

---

## 2. Corrections to the original investigation

The investigation was directionally right but two facts must be corrected before implementing:

1. **The derived name differs by instrument type** (the investigation claimed a uniform `<name>_vec_<labels>`):
   - **counter / up_down_counter / gauge / observable_\***: the underlying `#metric.name` is the tagged tuple `{otel, Name}` (`instrument_meter.erl:423`), so `make_vec_name/2` hits its **`{otel, Name}` clause** (`:652`) → `{otel_vec, <<Name/binary, LabelSuffix/binary>>}` → renders as **`requests_total_method_status`** — *no `_vec` infix*.
   - **histogram**: `instrument_histogram:new_histogram/3` stores `name = Name` as a **bare binary** (`instrument_histogram.erl:86`), so `make_vec_name/2` hits the bare-binary clause (`instrument_meter.erl:658`) → **`latency_vec_method`** — *with `_vec`*.
   - Any downstream regression that asserts "no mangled series" must match **both** `<name>_<labels>` (counter/gauge) and `<name>_vec_<labels>` (histogram).

2. **The histogram OTLP defect is two sub-bugs, not one** (the investigation described only the `badkey`):
   - (2a) wrong bucket key → crash;
   - (2b) cumulative vs per-bucket counts → wrong data even after (2a) is fixed. Confirmed against `instrument_histogram:collect/2` (`:240`, `cumulative_count(...)`) and the Prometheus path that correctly consumes cumulative (`instrument_prometheus.erl:112`).

---

## 3. Decisions (and why)

| Decision | Choice | Why |
|---|---|---|
| Scope | Fix **both** bugs | Both independently block OTLP metrics; both touch the exporter. |
| Packaging | **Two separate PRs** | Independent root causes, no shared change; easier upstream review. |
| Bug 1 storage strategy | **A1: reuse existing fixed-schema vec storage; reconcile at the export/render layer** | Keeps the proven NIF vec storage and the deliberate "delegate to vec storage" design from commit `3954a86`. The dynamic-attribute-set rewrite (A2) was considered and rejected as too large for a bugfix PR (see §7). |
| Heterogeneous attribute key-sets | **Supported** (one stream, one data point per observed set) | Matches both reference SDKs (opentelemetry-python, opentelemetry-erlang); OTel allows per-measurement attribute sets. |
| Phantom zero | **Lazy base registration** | Removes the eager-registration special case so the base follows the same "register on first write" rule the vecs already follow. Avoids the gauge-set-to-0 hazard of a value heuristic. "Leave it" (accept a benign empty-labels `0`) is the fallback if review objects. |
| Backward compatibility | **Bugfix posture**: the derived/mangled names disappear, no compat shim | The derived names were never an intended contract (they originate as an implementation artifact in `3954a86`, see git history), and the data under them is unusable today. Document the change in the CHANGELOG. |

### 3.1 Reference-SDK grounding

Both canonical SDKs were inspected:

- **opentelemetry-python (1.43.0.dev):** metric name = instrument/view name, never attribute-derived; aggregation keyed by `frozenset(attributes.items())`; no schema pre-declaration; series created lazily (no phantom); the Prometheus exporter unions label keys across data points and empty-fills absent ones (a *tested* fix). No cardinality cap in this version.
- **opentelemetry-erlang (experimental SDK 0.6.0):** metric name = stream/instrument name; ETS keyed `{StreamName, Attributes, ReaderId, Generation}`; no schema; lazy aggregate init (no phantom); OTLP carries each data point's attributes independently. No Prometheus exporter in that checkout; no cardinality cap.

Takeaways applied here: (a) name is fixed identity, attributes are data-point dimensions; (b) heterogeneous key-sets are normal; (c) no phantom empty series; (d) **the Prometheus union-with-empty-fill is an inherent export-time step**, not avoidable by storing differently — so A1 (reconcile at export) does not lose anything A2 would gain on the Prometheus side.

---

## 4. Background: the current storage model

This is needed to read PR 1. All references are against `be7e75e`.

### 4.1 The three substrates

- **NIF atomics — the actual numbers.** A counter/gauge value is a single mutable atomic float in C memory, behind an opaque `Ref` from `instrument_nif:new_gauge/0`. `inc_gauge(Ref, V)` is a lock-free atomic add. Histograms instead use an Erlang `atomics` array (sum slot + one slot per bucket).
- **ETS — the registry's working store.** `instrument_registry` (gen_server) holds `#metric{}` records in public `set` table(s) keyed by `#metric.name`. All *mutations* (register, unregister, growing a vector's `labels_map`) happen here.
- **persistent_term — the lock-free read path + index.** Mirrors of `#metric{}` under `{instrument_metric, Name}`, the flat index list `instrument_metrics` (every registered name), and per-label child caches `{instrument_label, Name, LabelValues}`. The hot paths (`lookup/1`, `collect_all/0`) read here; writes are globally expensive, so they happen at create time, not on the increment path.

### 4.2 The `#metric{}` wrapper

Every registered metric is `#metric{name, handle, collect = {Mod, Fun, Args}}`:
- `name` — the registry key (e.g. `{otel, <<"requests_total">>}`),
- `handle` — live storage (a NIF `Ref`, a `#vector{}`, or a `#histogram{}`),
- `collect` — the MFA the exporter calls to read the handle into an export map.

### 4.3 Trace: `create_counter` then attributed `add`

`create_counter(M, <<"requests_total">>)` (`instrument_meter.erl:126` → `create_instrument/4:361` → `create_underlying_metric/3:416`):
- builds a NIF gauge `Ref0` (=0),
- wraps it as `Base = #metric{name={otel,<<"requests_total">>}, handle={Ref0,T}, collect={instrument_counter,collect,…}}`,
- **registers it** (`instrument_metric:register/1` → ETS + persistent_term + `instrument_metrics` index), and
- stores `#otel_instrument{kind=counter, handle=Base}` under `{otel_instrument, <<"requests_total">>}`.

`add(C, 1, #{method=>GET, status=>200})` (`:194` → `do_add/4` attributed counter clause `:509`):
- `attrs_to_labels/1` (`:565`) sorts → `LabelNames=[method,status]`, `LabelValues=[<<"GET">>,<<"200">>]`,
- `ensure_vec_metric/4` (`:607`) → `make_vec_name/2` (`:652`) **bakes the label names into the name** → `{otel_vec, <<"requests_total_method_status">>}`, lazily creates/registers a `#metric{handle=#vector{labels=[method,status], labels_map=#{}}}`, and records ownership `{otel_instrument_vecs, {otel,<<"requests_total">>}} → [VecName]` (`track_vec_metric/2:643`),
- `inc_counter_vec/3` → `get_or_create_label/2` creates a per-series child counter (its own NIF gauge `Ref1`) under `labels_map[[GET,200]]`, caches it, then atomic-incs `Ref1` to 1.

Resulting `collect_all/0` (`instrument_registry.erl:367`) flat list:
```erlang
[ #{type=>counter, name=>{otel,"requests_total"}, val=>0},                              % base, untouched
  #{type=>counter, name=>{otel_vec,"requests_total_method_status"},
    labels=>[method,status], data=>[{[method,status],[<<"GET">>,<<"200">>],1}]} ]       % the real data
```

Both render paths consume this same flat list and format each entry independently:
- **Prometheus:** `instrument_prometheus:format/0` (`:20`) → strips tags via `format_name/1` (`:152-153`) → two text blocks.
- **OTLP/console:** `instrument_metrics_exporter:collect/0` (`:129` → `collect_metrics/0:260`) → `convert_metric/2` per entry → two `metric_data` → exporters.

That flat list **is** both bugs: `requests_total` exports `0` (phantom), and the real `1` hides under `requests_total_method_status` (mangled name). A different key-set (`#{method}`) produces a *third* registered name (`{otel_vec,"requests_total_method"}`) with its own `labels_map` — one vec per key-set.

---

## 5. PR 1 — meter attributed path: one stream under the registered name

### 5.1 Approach (A1)

Insert **one grouping step between `collect_all/0` and the formatters.** Everything below it — `create_counter`, `add`, `ensure_vec_metric`, `make_vec_name`, the per-key-set vecs, the `labels_map`, the NIF atomics — is unchanged. The internal `{otel_vec, …}` names still exist; the grouping step relabels them to the registered instrument name on the way out. Plus a storage-side change for the phantom (lazy base registration, §5.6).

```
            BEFORE                                   A1
  collect_all()  → flat list            collect_all()  → flat list
        │                                      │
        │                              ┌───────────────────────┐   NEW: resolve + group
        ▼                              └───────────────────────┘   (shared helper)
  convert_metric / prom format                 │
        │                                       ▼
        ▼                              convert_metric / prom format
   exporters                                 exporters
```

### 5.2 Piece 1 — vec → instrument resolution (read-only; no storage change)

A shared helper builds, from the existing ownership links (`{otel_instrument_vecs, BaseName}`) plus the `otel_instruments` list and each instrument's handle, a map from every registered OTel name to its user-facing instrument name:
```
{otel,"requests_total"}                     → <<"requests_total">>
{otel_vec,"requests_total_method_status">>  → <<"requests_total">>
```
**Implementation note:** the base-name form stored in `{otel_instrument_vecs, …}` differs by type (`{otel, Name}` for counter/gauge/observable; bare binary for histogram). The helper must normalize both to the user-facing `Name`. Iterate `otel_instruments`; for each, read its `#otel_instrument.handle` → `get_internal_metric_name/1` (`:326`) to recover the internal base name, then read its tracked vec list. Non-OTel (standalone) metrics resolve to themselves and are passed through untouched.

### 5.3 Piece 2 — merge / normalize (the grouping step)

Collapse the flat list into one logical metric per instrument, normalizing the base's unlabeled value into an empty-labels row and carrying **per-row** label names:
```
BEFORE (collect_all flat list — 2 entries):
  #{type=counter, name={otel,"requests_total"},                 val=0}
  #{type=counter, name={otel_vec,"requests_total_method_status"},
                  labels=[method,status], data=[{[method,status],[<<"GET">>,<<"200">>],1}]}

AFTER (1 grouped entry; each row keeps its own label names):
  #{name=<<"requests_total">>, type=counter,
    data=[ {[],              [],                       0},     % base, normalized (empty-labels row)
           {[method,status], [<<"GET">>,<<"200">>],   1} ]}
```
Rules:
- Group raw entries whose names resolve (Piece 1) to the same instrument; emit one entry under the resolved name.
- Normalize a base's unlabeled shape (`val` for counter/gauge, `#{count,sum,buckets}` for histogram) into a single `{[], [], Value}` data row.
- Standalone (non-OTel) entries pass through unchanged.
- Histograms follow the same grouping (their vec rows carry `#{count,sum,buckets}` values).

### 5.4 Piece 3 — OTLP / console render

Where: `instrument_metrics_exporter:collect_metrics/0` calls the grouping helper after `collect_all/0`; `convert_metric/2` is updated to use **per-row** label names instead of the single vec-level `labels`:
- `make_attributes(RowNames, RowVals)` per data row (`:375`), so `[{[],[],0}, {[method,status],[GET,200],1}]` → data points `[{attributes=#{},value=0}, {attributes=#{method=>GET,status=>200},value=1}]`.
- No union needed — OTLP carries per-point attributes independently.
```
BEFORE: two metric_data: requests_total → [{#{},0}];  requests_total_method_status → [{#{method,status},1}]
AFTER:  one metric_data:  requests_total → [{#{},0}, {#{method=>GET,status=>200},1}]
```
`get_instrument_unit/1` (`:381`) and `to_binary/1` (`:394`) already resolve `{otel,_}`/`{otel_vec,_}`; with grouping under the user name, unit lookup also starts resolving correctly (today it misses on the mangled binary and defaults to `<<"1">>`).

### 5.5 Piece 4 — Prometheus render

Where: `instrument_prometheus:format/0` calls the grouping helper after `collect_all/0`, then renders each grouped metric with the **union** of label names across its rows, empty-filling gaps (Python's exporter is the template):
```
BEFORE:
  requests_total 0
  requests_total_method_status{method="GET",status="200"} 1
AFTER (union columns = method,status; stable sort):
  requests_total{method="",status=""} 0          % base row (absent under lazy-register, §5.6)
  requests_total{method="GET",status="200"} 1
```
The union must be computed per metric family and the key order stabilized (sort), so exactly one `# HELP`/`# TYPE` block is emitted per name. Counter `_total` suffixing, gauge, and histogram bucket rendering are otherwise unchanged.

### 5.6 Piece 5 — phantom suppression via lazy base registration (storage-side)

This sits **below** the grouping step, in the write/storage path — it makes the base follow the same "register on first write" rule the vecs already follow.

- **`create_underlying_metric/3`** (`:416/:430/:443/:456`): drop the `instrument_metric:register(Metric)` call (`:427/:440/:453/:466`). Build and return the `#metric{}` record (NIF ref / histogram) as today, but do not register it. The record still lives inside `#otel_instrument.handle`, which the labeled path reads for the name.
- **Add `ensure_base_registered/1`** (idempotent, race-tolerant):
  ```erlang
  ensure_base_registered(#metric{name = Name} = Metric) ->
    case instrument_registry:lookup(Name) of
      undefined -> _ = catch instrument_metric:register(Metric), ok;
      _         -> ok
    end.
  ```
  (A registry lookup per unlabeled write matches the cost the labeled path already pays — `ensure_vec_metric` does `instrument_registry:lookup/1` on every labeled write, `:612`.)
- **Call it from the unlabeled write clauses**, binding the whole record in the head:
  - `do_add/4` empty-attrs clauses (`:499`, `:503`),
  - `do_set/4` empty-attrs clauses (`:543`, `:547`),
  - `do_record/4` empty-attrs clause (`:532`).
- **Observables:** no change. 0-arity callbacks already write via `do_set(Handle, Kind, Value, #{})` (`:266`) → covered by the above. 1-arity (attributed) callbacks only hit the labeled path → base stays unregistered → no phantom.

Effect: an only-attributed instrument's base never enters `instrument_metrics`, so Piece 2 has no base row to normalize → the empty-labels `0` never exists. A both-ways instrument registers its base on the first unlabeled write → its `{}` row merges into the same stream (correct OTel).

### 5.7 Files touched (PR 1)

- `src/instrument_meter.erl` — `create_underlying_metric/3` (drop eager register), new `ensure_base_registered/1`, unlabeled clauses of `do_add`/`do_set`/`do_record`; **export a Piece 1 resolution helper** (it owns the `otel_instruments` and `{otel_instrument_vecs, …}` data) that maps a registered OTel name → user-facing instrument name. Also verify `instrument_metric:unregister/1` is a harmless no-op for an unregistered base.
- **New shared helper module** (e.g. `instrument_otel_streams`) housing the Piece 2 grouping/normalize, built on the `instrument_meter` resolution helper. Called by both render entry points below.
- `src/exporters/instrument_metrics_exporter.erl` — call the grouping helper in `collect_metrics/0` (after `collect_all/0`); update `convert_metric/2` to use per-row label names.
- `src/instrument_prometheus.erl` — call the grouping helper in `format/0` (after `collect_all/0`); add union-with-empty-fill rendering.

`ensure_vec_metric`, `make_vec_name`, `instrument_vector`, the NIF, and the standalone `instrument_metric` vec API are **unchanged**.

### 5.8 Behavior changes / CHANGELOG (PR 1)

- The derived names disappear: `<name>_<sorted-labels>` (counter/gauge/observable) and `<name>_vec_<sorted-labels>` (histogram). Their data now appears under the registered instrument name, with the attributes as data-point attributes (OTLP) / labels (Prometheus).
- The phantom `<name> 0` no-attributes series is gone.
- An instrument created but **never written** no longer emits a `0` series; it appears on first write (matches the OTel SDKs).
- Heterogeneous attribute key-sets on one instrument now produce one stream with a data point per set; in Prometheus, label columns are the union across sets with empty-string fill for absent keys.

### 5.9 Testing (PR 1)

- Attributed counter, one key-set → exactly one stream named as registered, attributes as data-point attributes, value correct; assert **no** `requests_total_method_status`-style series and **no** `requests_total 0` phantom.
- Two distinct key-sets on one counter → one stream, two data points; OTLP has two data points with their own attributes; Prometheus shows union columns with empty fills, single `# TYPE`.
- Gauge, up_down_counter, histogram attributed paths → same single-stream result; histogram asserts no `_vec` series.
- 0-arity observable (unlabeled) → base registers, one clean series. 1-arity attributed observable → no phantom base.
- Mixed labeled + unlabeled on one instrument → `{}` data point + labeled data points merged under one name.
- Created-but-never-written instrument → absent from `collect/0` output (the documented behavior change).
- Update `metric_name_otel_with_attrs_test` / `metric_name_otel_test` comments and tighten assertions to the single-stream contract.
- Regression matching the downstream chalet-mcp assertion: no derived `<name>_<labels>` or `<name>_vec_<labels>` series in the OTLP payload.

---

## 6. PR 2 — histogram OTLP encoder

Both fixes live in `src/exporters/instrument_metrics_exporter_otlp.erl`, in `encode_histogram_data_point/1` (`:234`). Collection (`instrument_metrics_exporter:convert_metric/2`) and the Prometheus/console render paths are **not** touched — they are correct today: the Prometheus path reads the raw `upper_bound`/`cumulative_count` (`instrument_prometheus.erl:108`, `:112`), and the console exporter consumes the converted `bound` as a debug dump (not OTLP semantics).

### 6.1 Fix 1 — bucket key (the crash)

`encode_histogram_data_point/1` reads `maps:get(upper_bound, B)` (`:240-241`), but `convert_metric/2` emits each bucket keyed `bound` (`instrument_metrics_exporter.erl:338`, `:356`). This raises `{badkey, upper_bound}`; because the OTLP exporter sends the whole collection as one payload inside a `catch`, **any** histogram present silently drops the entire batch (counters and gauges included). Fix: read `bound`.

### 6.2 Fix 2 — cumulative → per-bucket counts (wrong data)

`BucketCounts = [maps:get(count, B, 0) || B <- Buckets]` (`:238`) passes the counts straight through, but those counts are **cumulative** — `convert_metric/2` sets `count => cumulative_count` (`:338/:356`), sourced from `instrument_histogram:collect/2`'s `cumulative_count(...)` (`:240`). OTLP `bucketCounts` must be **per-bucket deltas**. The Prometheus path correctly wants cumulative, so the de-cumulation belongs **only** in the OTLP encoder. De-cumulate in ascending bound order:
```
boundaries [1,5,10];  cumulative (le) = [2, 5, 7],  +Inf = 8 (total)
  → bucketCounts  = [2, 3, 2, 1]   (2, 5-2, 7-5, 8-7)
    explicitBounds = [1, 5, 10]    (excludes +Inf; one shorter than bucketCounts)
```
`delta[i] = cumulative[i] - cumulative[i-1]` with `cumulative[-1] = 0`; the `+Inf` bucket's delta = total − last finite cumulative. Preserve the existing structure: `explicitBounds` excludes `infinity`; `bucketCounts` length = `explicitBounds` length + 1.

### 6.3 CHANGELOG (PR 2)

- Histograms now export over OTLP at all (previously any histogram raised `{badkey, upper_bound}` and dropped the whole metrics batch).
- OTLP histogram `bucketCounts` are now correct per-bucket counts (previously cumulative, which over-counts on any backend that treats them per the spec).

### 6.4 Testing (PR 2)

- A histogram (via meter or standalone vec) → OTLP encode succeeds; assert `explicitBounds` excludes `+Inf`, `length(bucketCounts) == length(explicitBounds) + 1`, and per-bucket counts for a known set of observations (e.g. the `[1,5,10]` example).
- A batch with a histogram **and** a counter → the whole batch is emitted (regression for the swallow-the-batch crash).
- Prometheus/console histogram output unchanged (still cumulative `le` buckets) — regression guard.

---

## 7. Out of scope / considered-and-rejected

- **A2 — dynamic attribute-set-keyed storage** (a store keyed by the attribute set, no fixed schema, like the reference SDKs). Cleaner model, but a re-architecture of the meter's attributed storage touching the hot write path — too large for a bugfix PR, and the Prometheus union is required either way, so A2 gains nothing externally over A1. Possible future direction, not this work.
- **Cross-key-set cardinality cap.** The existing per-vec cardinality cap (`instrument_registry:label_count/1`, overflow sentinel) still applies per internal vec; A1 does not add a cap across an instrument's key-sets. Out of scope.
- **Changing the standalone `instrument_metric` vec API** — unchanged.
- **`make_vec_name` internal naming** — left as-is; the names just stop being exported.

---

## 8. Risks / open items

- **Resolution base-name normalization** (§5.2): must reconcile `{otel, Name}` vs bare-binary base names to the user-facing `Name`. Drive it from `otel_instruments` + `get_internal_metric_name/1`.
- **`unregister` no-op** for an unregistered base (only-attributed instrument) — verify `instrument_metric:unregister/1` tolerates an unknown name.
- **Prometheus union ordering** — sort union keys for stable, single `# TYPE` output.
- **Phantom decision** — lazy-register is the chosen approach; "leave it" (benign empty-labels `0`) remains the fallback if upstream review prefers a smaller diff.
- **Performance** — resolution + grouping run once per `collect`/scrape (cold path); acceptable. The hot write path is unchanged except the per-unlabeled-write registry lookup, which mirrors the existing labeled-path cost.
