# PR 2 — Histogram OTLP Encoder: Stop the Batch-Dropping Crash and Emit Per-Bucket Counts

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the OTLP metrics histogram encoder so that (1) a histogram no longer raises `{badkey, upper_bound}` — which today silently drops the entire OTLP metrics batch — and (2) the emitted `bucketCounts` are per-bucket counts (OTLP semantics) rather than the cumulative counts the library stores internally.

**Architecture:** Both fixes are confined to `encode_histogram_data_point/1` in `src/exporters/instrument_metrics_exporter_otlp.erl`. The metric collection step (`instrument_metrics_exporter:convert_metric/2`) and the Prometheus/console paths are **not** touched — they correctly use the bucket key `bound` and the cumulative form. Full design: `docs/superpowers/specs/2026-06-09-meter-metrics-export-fixes-design.md` §6.

**Tech Stack:** Erlang/OTP (27/28/29), rebar3, Common Test. `json:encode/1` / `json:decode/1` from the OTP `json` module. `warnings_as_errors` is on — compile clean. Run a single case with `rebar3 ct --suite=<MODULE> --case=<CASE>`.

**Independence:** This PR shares **no source file** with PR 1 (meter attributed path). It touches only `instrument_metrics_exporter_otlp.erl`, plus a brand-new test suite and `CHANGELOG.md`. Branch from `master`; it can be developed and merged independently of PR 1. The only cross-PR merge point is `CHANGELOG.md` (both PRs add an entry under `## [Unreleased]` — a trivial both-added resolution).

**Background — why a histogram drops the whole batch today:** `convert_metric/2` emits each bucket as `#{bound => Boundary, count => CumulativeCount}` (`instrument_metrics_exporter.erl:338`,`:356`), but `encode_histogram_data_point/1` reads `maps:get(upper_bound, B)` (`instrument_metrics_exporter_otlp.erl:240`). That raises `{badkey, upper_bound}`; because `encode_metrics/1` encodes the whole metrics list in one shot and `instrument_metrics_exporter:do_export/1` wraps the export in a `catch` (`:253`), one histogram takes down the entire batch — counters and gauges included.

---

## File Structure

- **Modify** `src/exporters/instrument_metrics_exporter_otlp.erl`:
  - export `encode_metrics/1` (already the internal list→JSON entry point; exporting it gives a pure, HTTP-free surface for tests and is useful on its own).
  - `encode_histogram_data_point/1`: read `bound` (not `upper_bound`); compute per-bucket counts via a new `decumulative_counts/1` helper.
- **Create** `test/instrument_otlp_histogram_SUITE.erl` — a dedicated suite that encodes synthetic histogram metric-data through `encode_metrics/1` and inspects the decoded JSON. Kept separate from `instrument_metrics_exporter_SUITE.erl` so PR 1 and PR 2 never edit the same test file.
- **Modify** `CHANGELOG.md`.

The "converted" histogram data point shape the encoder receives (produced by `convert_metric/2`) is:
```
#{attributes => #{...}, timestamp => Ts, start_time => T | undefined,
  value => #{count => Total, sum => Sum,
             buckets => [#{bound => B, count => CumulativeCount}, ..., #{bound => infinity, count => Total}]}}
```
Buckets are in ascending `bound` order; the last one is the `+Inf` bucket (`bound => infinity`), and its `count` is the grand total.

---

## Task 1: Stop the crash (read `bound`) + a batch-encode regression test

**Files:**
- Modify: `src/exporters/instrument_metrics_exporter_otlp.erl` (`-export`; `encode_histogram_data_point/1` `:234`)
- Create: `test/instrument_otlp_histogram_SUITE.erl`

- [ ] **Step 1: Write the failing test**

Create `test/instrument_otlp_histogram_SUITE.erl`:

```erlang
-module(instrument_otlp_histogram_SUITE).
-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([histogram_batch_encodes_test/1]).
-include_lib("stdlib/include/assert.hrl").

all() -> [histogram_batch_encodes_test].

init_per_suite(Config) ->
  _ = application:ensure_all_started(crypto),
  ok = application:start(instrument),
  Config.

end_per_suite(Config) ->
  ok = application:stop(instrument),
  Config.

%% A synthetic converted histogram data point: cumulative counts under `bound`,
%% with the +Inf bucket last (this is what convert_metric/2 produces).
hist_metric(Name) ->
  DP = #{attributes => #{}, timestamp => 123,
         value => #{count => 8, sum => 42.0,
                    buckets => [#{bound => 1, count => 2},
                                #{bound => 5, count => 5},
                                #{bound => 10, count => 7},
                                #{bound => infinity, count => 8}]}},
  #{name => Name, type => histogram, data_points => [DP]}.

%% Pull every metric object out of a decoded OTLP payload.
all_metrics(Decoded) ->
  RMs = maps:get(<<"resourceMetrics">>, Decoded),
  lists:append([maps:get(<<"metrics">>, SM)
                || RM <- RMs, SM <- maps:get(<<"scopeMetrics">>, RM)]).

decode(Json) ->
  json:decode(iolist_to_binary(Json)).

%% Crash regression: a batch with a histogram AND a counter must encode.
%% Today the histogram raises {badkey, upper_bound} and the whole batch is lost.
histogram_batch_encodes_test(_Config) ->
  Counter = #{name => <<"c_otlp">>, type => counter,
              data_points => [#{attributes => #{}, value => 5, timestamp => 1}]},
  Json = instrument_metrics_exporter_otlp:encode_metrics([Counter, hist_metric(<<"h_otlp">>)]),
  Names = [maps:get(<<"name">>, M) || M <- all_metrics(decode(Json))],
  ?assert(lists:member(<<"c_otlp">>, Names)),
  ?assert(lists:member(<<"h_otlp">>, Names)),
  ok.
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `rebar3 ct --suite=instrument_otlp_histogram_SUITE --case=histogram_batch_encodes_test`
Expected: FAIL — two possible failure modes, both red:
- `undefined function instrument_metrics_exporter_otlp:encode_metrics/1` (not yet exported), or
- once exported, an `error:{badkey, upper_bound}` raised from `encode_histogram_data_point/1`.

- [ ] **Step 3: Export `encode_metrics/1`**

In `src/exporters/instrument_metrics_exporter_otlp.erl`, add `encode_metrics/1` to the public exports. Change:

```erlang
%% Public API
-export([new/1]).
```
to:
```erlang
%% Public API
-export([new/1, encode_metrics/1]).
```

(`encode_metrics/1` already exists as an internal function at `:120` — this just exposes it.)

- [ ] **Step 4: Read `bound` instead of `upper_bound`**

In `encode_histogram_data_point/1` (`:234`), change the `ExplicitBounds` line (`:240-241`) from:

```erlang
  ExplicitBounds = [maps:get(upper_bound, B) || B <- Buckets,
                    maps:get(upper_bound, B) =/= infinity],
```
to:
```erlang
  ExplicitBounds = [maps:get(bound, B) || B <- Buckets,
                    maps:get(bound, B) =/= infinity],
```

Leave the rest of the function unchanged for now (the `BucketCounts` line still reads cumulative counts — Task 2 fixes that).

- [ ] **Step 5: Run the test, verify it passes**

Run: `rebar3 ct --suite=instrument_otlp_histogram_SUITE --case=histogram_batch_encodes_test`
Expected: PASS — the batch now encodes; both `c_otlp` and `h_otlp` appear.

- [ ] **Step 6: Commit**

```bash
git add src/exporters/instrument_metrics_exporter_otlp.erl test/instrument_otlp_histogram_SUITE.erl
git commit -m "stop a histogram from dropping the whole OTLP batch by reading the correct bucket-bound key"
```

---

## Task 2: Emit per-bucket counts (de-cumulate)

**Files:**
- Modify: `src/exporters/instrument_metrics_exporter_otlp.erl` (`encode_histogram_data_point/1` `:238`; add `decumulative_counts/1`)
- Modify: `test/instrument_otlp_histogram_SUITE.erl` (add the per-bucket assertion test)

- [ ] **Step 1: Write the failing test**

Add `histogram_bucket_counts_per_bucket_test/1` to the `-export` and `all/0` of `test/instrument_otlp_histogram_SUITE.erl`, then add:

```erlang
%% bucketCounts must be PER-BUCKET deltas, not cumulative. explicitBounds
%% excludes +Inf and is exactly one shorter than bucketCounts.
histogram_bucket_counts_per_bucket_test(_Config) ->
  Json = instrument_metrics_exporter_otlp:encode_metrics([hist_metric(<<"h_otlp">>)]),
  [Hist] = [M || M <- all_metrics(decode(Json)), maps:get(<<"name">>, M) =:= <<"h_otlp">>],
  [DP] = maps:get(<<"dataPoints">>, maps:get(<<"histogram">>, Hist)),
  %% cumulative [2,5,7,8] -> per-bucket [2,3,2,1]; counts are encoded as strings
  ?assertEqual([<<"2">>, <<"3">>, <<"2">>, <<"1">>], maps:get(<<"bucketCounts">>, DP)),
  ?assertEqual([1, 5, 10], maps:get(<<"explicitBounds">>, DP)),
  ?assertEqual(4, length(maps:get(<<"bucketCounts">>, DP))),
  ?assertEqual(3, length(maps:get(<<"explicitBounds">>, DP))),
  ok.
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `rebar3 ct --suite=instrument_otlp_histogram_SUITE --case=histogram_bucket_counts_per_bucket_test`
Expected: FAIL — after Task 1 the encoder runs, but `bucketCounts` is still the cumulative `[<<"2">>,<<"5">>,<<"7">>,<<"8">>]`, so the first assertion fails.

- [ ] **Step 3: De-cumulate the bucket counts**

In `encode_histogram_data_point/1`, change the `BucketCounts` line (`:238`) from:

```erlang
  BucketCounts = [maps:get(count, B, 0) || B <- Buckets],
```
to:
```erlang
  %% Buckets carry cumulative counts in ascending bound order; OTLP wants
  %% per-bucket counts, so take the difference from the previous bound.
  BucketCounts = decumulative_counts(Buckets),
```

Then add the helper (next to `encode_histogram_data_point/1`):

```erlang
%% Convert cumulative bucket counts to per-bucket counts. Buckets are in
%% ascending bound order with the +Inf bucket last; each `count' is the
%% cumulative count up to and including that bound.
decumulative_counts(Buckets) ->
  {Counts, _Last} =
    lists:mapfoldl(fun(B, Prev) ->
      Cum = maps:get(count, B, 0),
      {Cum - Prev, Cum}
    end, 0, Buckets),
  Counts.
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `rebar3 ct --suite=instrument_otlp_histogram_SUITE --case=histogram_bucket_counts_per_bucket_test`
Expected: PASS — `bucketCounts` is now `[<<"2">>,<<"3">>,<<"2">>,<<"1">>]`.

- [ ] **Step 5: Run the whole suite (both tests)**

Run: `rebar3 ct --suite=instrument_otlp_histogram_SUITE`
Expected: PASS for both `histogram_batch_encodes_test` and `histogram_bucket_counts_per_bucket_test`.

- [ ] **Step 6: Commit**

```bash
git add src/exporters/instrument_metrics_exporter_otlp.erl test/instrument_otlp_histogram_SUITE.erl
git commit -m "emit per-bucket OTLP histogram counts instead of cumulative ones"
```

---

## Task 3: CHANGELOG + full regression run

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add the CHANGELOG entry**

Add under a `## [Unreleased]` heading at the top of `CHANGELOG.md` (create the heading if it is not already there — if PR 1 has already added it on this branch, append to its `### Fixed` list):

```markdown
## [Unreleased]

### Fixed
- OTLP metrics: a histogram no longer raises `{badkey, upper_bound}` during
  encoding. Previously any histogram present caused the encoder to crash and,
  because the whole batch is encoded under one `catch`, silently dropped every
  metric in that export (counters and gauges included).
- OTLP histogram `bucketCounts` are now per-bucket counts as the OTLP spec
  requires, instead of the cumulative counts stored internally (which made
  every bucket over-count on spec-compliant backends).
```

- [ ] **Step 2: Full test-suite regression**

Run: `rebar3 ct`
Expected: PASS across all suites. Pay attention to `instrument_histogram_SUITE`, `instrument_metrics_exporter_SUITE`, and the new `instrument_otlp_histogram_SUITE` — none of the Prometheus/console/collect paths changed, so they must stay green.

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "note the OTLP histogram encoder fixes in the changelog"
```

---

## Self-review (run before handing off / executing)

- **Spec coverage (spec §6):** Fix 1 (`upper_bound` → `bound`, the crash) → Task 1; Fix 2 (cumulative → per-bucket) → Task 2; CHANGELOG → Task 3. Covered.
- **Placeholder scan:** none — every step has concrete code and exact commands.
- **Type/name consistency:** the test helpers `hist_metric/1`, `all_metrics/1`, `decode/1` are defined in Task 1's suite and reused in Task 2. `decumulative_counts/1` is defined and used in Task 2. `encode_metrics/1` is exported in Task 1 and called by both tests.
- **Out of scope (do not touch here):** `convert_metric/2` and `instrument_histogram:collect/2` keep emitting `bound`/cumulative (the Prometheus and console paths depend on that). The meter attributed-path / phantom / grouping work is PR 1, a separate plan and branch.

## Risk notes for the executor

- `decumulative_counts/1` assumes ascending `bound` order with the `+Inf` bucket last — guaranteed by `instrument_histogram:collect/2` (boundaries are validated/sorted) and preserved by `convert_metric/2`. Do not sort or reorder.
- `encode_metrics/1` returns iodata; tests wrap it with `iolist_to_binary/1` before `json:decode/1` (the `decode/1` helper does this).
- Exporting `encode_metrics/1` is the intended public surface for this fix's tests; it does not change any existing behavior.
