# PR 1 — Meter Attributed Path: One Stream Under the Registered Name

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `instrument_meter` attributed writes (`add/3`, `record/3`, `set/3`, attributed observables) export as a single metric stream under the registered instrument name — attributes as data-point attributes — with no derived/mangled name and no phantom zero series.

**Architecture:** Approach **A1** (render-layer reconciliation). The NIF/vec storage, `ensure_vec_metric`, and `make_vec_name` are left untouched — they still create one internal vec per attribute key-set. A new grouping step inserted between `instrument_registry:collect_all/0` and the two formatters folds an instrument's base + vecs back under its registered name. Separately, the base instrument is registered **lazily** (on first unlabeled write) instead of eagerly at `create_*`, which removes the phantom zero at the source. Full design: `docs/superpowers/specs/2026-06-09-meter-metrics-export-fixes-design.md` §5.

**Tech Stack:** Erlang/OTP (27/28/29), rebar3, Common Test (`*_SUITE.erl`). Build compiles a NIF via cmake hooks; `warnings_as_errors` is on, so **every change must compile with zero warnings** (no unused vars/functions). Run a single case with `rebar3 ct --suite=<MODULE> --case=<CASE>`.

**Baseline:** branch `meter-metrics-export-fixes`, commit `8b1496a` (1.1.3 + the committed spec).

---

## File Structure

- **Create** `src/instrument_otel_streams.erl` — pure grouping/normalize step. `group/1` (reads the live OTel index) and `group/2` (index injected, for tests). Folds raw `collect_all/0` entries belonging to one OTel instrument into a single merged raw entry whose `data` rows each carry their own label names; passes non-OTel entries through unchanged. One responsibility: reshape the flat collected list into per-instrument streams.
- **Modify** `src/instrument_meter.erl` — (a) lazy base registration: stop registering in `create_underlying_metric/3`, add `ensure_base_registered/1`, call it from the unlabeled `do_add`/`do_set`/`do_record` clauses; (b) export `otel_name_index/0` (maps every registered OTel name → user-facing instrument binary).
- **Modify** `src/exporters/instrument_metrics_exporter.erl` — call `instrument_otel_streams:group/1` in `collect_metrics/0`; change the three labeled `convert_metric/2` clauses to use **per-row** label names.
- **Modify** `src/instrument_prometheus.erl` — call `instrument_otel_streams:group/1` in `format/0`; render labeled metrics over the **union** of label names across rows, empty-filling absent keys.
- **Modify tests** `test/instrument_meter_SUITE.erl`, `test/instrument_metrics_exporter_SUITE.erl`, `test/instrument_prometheus_SUITE.erl` — new behavior tests + tighten existing loose OTel tests + a no-mangled-name regression.
- **Modify** `CHANGELOG.md` — document the behavior change.

Label names on the meter path are **atoms** (attribute-map keys, e.g. `method`), label values are **binaries**. The grouping merges only meter-path (OTel) instruments, so within a merged entry all row names are atoms.

---

## Task 1: Lazy base registration (kill the phantom at the source)

**Files:**
- Modify: `src/instrument_meter.erl` (`create_underlying_metric/3` `:416-467`; `do_add/4` `:499-506`; `do_record/4` `:532-534`; `do_set/4` `:543-550`; add `ensure_base_registered/1`)
- Test: `test/instrument_meter_SUITE.erl`

- [ ] **Step 1: Write the failing test**

Add to the `-export` list and `all/0` in `test/instrument_meter_SUITE.erl`: `lazy_base_registration_test/1`. Then add:

```erlang
lazy_base_registration_test(_Config) ->
  Meter = instrument_meter:get_meter(<<"lazy_test">>),
  C = instrument_meter:create_counter(Meter, <<"lazy_counter">>),

  %% Only attributed writes so far: the base must NOT be registered.
  ok = instrument_meter:add(C, 1, #{method => <<"GET">>}),
  ?assertEqual(undefined,
               instrument_registry:lookup({otel, <<"lazy_counter">>})),

  %% First unlabeled write registers the base.
  ok = instrument_meter:add(C, 1),
  ?assertNotEqual(undefined,
                  instrument_registry:lookup({otel, <<"lazy_counter">>})),

  %% An instrument created but never written stays unregistered.
  _ = instrument_meter:create_counter(Meter, <<"never_written">>),
  ?assertEqual(undefined,
               instrument_registry:lookup({otel, <<"never_written">>})),
  ok.
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `rebar3 ct --suite=instrument_meter_SUITE --case=lazy_base_registration_test`
Expected: FAIL — today the base is registered eagerly at `create_counter`, so the first `?assertEqual(undefined, ...)` fails (lookup returns a `#metric{}`).

- [ ] **Step 3: Stop eager registration in `create_underlying_metric/3`**

In `src/instrument_meter.erl`, remove the `ok = instrument_metric:register(Metric),` line from **all four** clauses of `create_underlying_metric/3` (counter `:427`, up_down_counter `:440`, histogram `:453`, gauge `:466`). Each clause must still build and return the record. Example — the counter clause becomes:

```erlang
create_underlying_metric(Name, counter, Opts) ->
  %% Use gauge NIF for counter (monotonic increments only)
  {ok, Ref} = instrument_nif:new_gauge(),
  StartTime = erlang:system_time(nanosecond),
  Description = maps:get(description, Opts, <<>>),
  Info = instrument_lib:mk_info(Name, Description),
  #metric{
    name = {otel, Name},
    handle = {Ref, StartTime},
    collect = {instrument_counter, collect, [Info, {Ref, StartTime}]}
  }.
```

Do the same for the `up_down_counter`, `gauge`, and `histogram` clauses (the histogram clause's last expression becomes just `Metric` instead of `ok = instrument_metric:register(Metric), Metric`). Remove the now-unneeded trailing `, Metric;`/`Metric` bindings consistently — each clause's final expression is the record itself.

- [ ] **Step 4: Add `ensure_base_registered/1`**

Add this private function near the other internal functions in `src/instrument_meter.erl`:

```erlang
%% Register the base instrument's underlying metric the first time it is
%% written via the unlabeled path. Attributed-only instruments never call
%% this, so no phantom zero-valued base series is ever exported.
ensure_base_registered(#metric{name = Name} = Metric) ->
  case instrument_registry:lookup(Name) of
    undefined ->
      _ = catch instrument_metric:register(Metric),
      ok;
    _ ->
      ok
  end;
ensure_base_registered(_) ->
  ok.
```

- [ ] **Step 5: Call it from the unlabeled write clauses**

Bind the whole record in each unlabeled clause head and call `ensure_base_registered/1` before the NIF/histogram write.

`do_add/4` unlabeled clauses (`:499-506`):
```erlang
do_add(#metric{handle = {Ref, _StartTime}} = Metric, _Kind, Value, Attrs)
        when is_number(Value), map_size(Attrs) =:= 0 ->
  ensure_base_registered(Metric),
  instrument_nif:inc_gauge(Ref, float(Value));
do_add(#metric{handle = Ref} = Metric, _Kind, Value, Attrs)
        when is_number(Value), map_size(Attrs) =:= 0, is_reference(Ref) ->
  ensure_base_registered(Metric),
  instrument_nif:inc_gauge(Ref, float(Value));
```

`do_record/4` unlabeled clause (`:532`):
```erlang
do_record(#metric{} = Metric, _Kind, Value, Attrs)
        when is_number(Value), map_size(Attrs) =:= 0 ->
  ensure_base_registered(Metric),
  instrument_histogram:observe_histogram(Metric, Value);
```

`do_set/4` unlabeled clauses (`:543-550`):
```erlang
do_set(#metric{handle = {Ref, _StartTime}} = Metric, _Kind, Value, Attrs)
        when is_number(Value), map_size(Attrs) =:= 0 ->
  ensure_base_registered(Metric),
  instrument_nif:set_gauge(Ref, float(Value));
do_set(#metric{handle = Ref} = Metric, _Kind, Value, Attrs)
        when is_number(Value), map_size(Attrs) =:= 0, is_reference(Ref) ->
  ensure_base_registered(Metric),
  instrument_nif:set_gauge(Ref, float(Value));
```

- [ ] **Step 6: Run the test, verify it passes**

Run: `rebar3 ct --suite=instrument_meter_SUITE --case=lazy_base_registration_test`
Expected: PASS.

- [ ] **Step 7: Run the whole meter suite (no regressions)**

Run: `rebar3 ct --suite=instrument_meter_SUITE`
Expected: PASS. (`counter_add`, `gauge_set`, `histogram_record`, etc. all do at least one unlabeled write, so their bases still register.)

- [ ] **Step 8: Commit**

```bash
git add src/instrument_meter.erl test/instrument_meter_SUITE.erl
git commit -m "register meter base instruments lazily so attributed-only metrics emit no phantom zero"
```

---

## Task 2: OTel name index (vec → instrument resolution)

**Files:**
- Modify: `src/instrument_meter.erl` (add + export `otel_name_index/0`)
- Test: `test/instrument_meter_SUITE.erl`

- [ ] **Step 1: Write the failing test**

Add `otel_name_index_test/1` to `-export`/`all/0`, then:

```erlang
otel_name_index_test(_Config) ->
  Meter = instrument_meter:get_meter(<<"idx_test">>),
  C = instrument_meter:create_counter(Meter, <<"idx_counter">>),
  ok = instrument_meter:add(C, 1, #{method => <<"GET">>, status => 200}),
  ok = instrument_meter:add(C, 1),

  Index = instrument_meter:otel_name_index(),

  %% The base (registered on the unlabeled add) maps to the user name.
  ?assertEqual(<<"idx_counter">>,
               maps:get({otel, <<"idx_counter">>}, Index)),

  %% Every tracked vec maps to the same user name.
  VecNames = persistent_term:get({otel_instrument_vecs, {otel, <<"idx_counter">>}}, []),
  ?assert(length(VecNames) >= 1),
  lists:foreach(fun(V) ->
    ?assertEqual(<<"idx_counter">>, maps:get(V, Index))
  end, VecNames),
  ok.
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `rebar3 ct --suite=instrument_meter_SUITE --case=otel_name_index_test`
Expected: FAIL with `undefined function instrument_meter:otel_name_index/0`.

- [ ] **Step 3: Add and export `otel_name_index/0`**

Add `otel_name_index/0` to the `%% Utility` `-export([...])` block, then add:

```erlang
%% @doc Map every registered OTel metric name (the base `{otel, Name}` /
%% bare-binary histogram name, and each tracked `{otel_vec, _}` / bare vec
%% name) to its user-facing instrument name. Used by the export grouping
%% step to fold an instrument's base + vecs into one stream.
-spec otel_name_index() -> #{term() => binary()}.
otel_name_index() ->
  Names = persistent_term:get(otel_instruments, []),
  lists:foldl(fun(Name, Acc) ->
    case get_instrument(Name) of
      undefined ->
        Acc;
      #otel_instrument{handle = Handle} ->
        Base = get_internal_metric_name(Handle),
        VecNames = persistent_term:get({otel_instrument_vecs, Base}, []),
        Acc1 = case Base of
                 undefined -> Acc;
                 _ -> Acc#{Base => Name}
               end,
        lists:foldl(fun(V, A) -> A#{V => Name} end, Acc1, VecNames)
    end
  end, #{}, Names).
```

(`get_instrument/1` and `get_internal_metric_name/1` already exist in this module.)

- [ ] **Step 4: Run the test, verify it passes**

Run: `rebar3 ct --suite=instrument_meter_SUITE --case=otel_name_index_test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/instrument_meter.erl test/instrument_meter_SUITE.erl
git commit -m "expose an OTel name index so the exporter can group an instrument's vecs under its registered name"
```

---

## Task 3: Grouping/normalize module (`instrument_otel_streams`)

**Files:**
- Create: `src/instrument_otel_streams.erl`
- Test: `test/instrument_otel_streams_SUITE.erl`

An instrument with a single registry entry (a base-only instrument, or a single-key-set attributed instrument) passes through with **only its name rewritten** — preserving its shape and fields like `start_time`. Only instruments with several entries (base + vecs, or multiple vecs) are merged into one labeled entry whose `data` rows each carry their own label names, with the base's unlabeled value normalized into a `{[], [], Value}` row:

```
input raw entries for one counter:
  #{type=>counter, name=>{otel,<<"req">>}, help=>H, val=>0}                              % base
  #{type=>counter, name=>{otel_vec,<<"req_method">>}, help=>H, labels=>[method],
    data=>[{[method],[<<"GET">>],3}]}
output merged entry:
  #{type=>counter, name=><<"req">>, help=>H, labels=>[method],
    data=>[{[],[],0}, {[method],[<<"GET">>],3}]}
```

- [ ] **Step 1: Write the failing test**

Create `test/instrument_otel_streams_SUITE.erl`:

```erlang
-module(instrument_otel_streams_SUITE).
-export([all/0]).
-export([groups_base_and_vecs/1, renames_single_vec/1,
         passes_through_single_base/1, passes_through_non_otel/1]).
-include_lib("stdlib/include/assert.hrl").

all() -> [groups_base_and_vecs, renames_single_vec,
          passes_through_single_base, passes_through_non_otel].

%% base + two distinct key-set vecs for one instrument -> one merged stream
groups_base_and_vecs(_Config) ->
  Index = #{{otel, <<"req">>} => <<"req">>,
           {otel_vec, <<"req_method">>} => <<"req">>,
           {otel_vec, <<"req_method_status">>} => <<"req">>},
  Raw = [
    #{type => counter, name => {otel, <<"req">>}, help => <<"h">>, val => 0},
    #{type => counter, name => {otel_vec, <<"req_method">>}, help => <<"h">>,
      labels => [method], data => [{[method], [<<"GET">>], 3}]},
    #{type => counter, name => {otel_vec, <<"req_method_status">>}, help => <<"h">>,
      labels => [method, status], data => [{[method, status], [<<"GET">>, <<"200">>], 1}]}
  ],
  [Merged] = instrument_otel_streams:group(Raw, Index),
  ?assertEqual(<<"req">>, maps:get(name, Merged)),
  ?assertEqual(counter, maps:get(type, Merged)),
  Data = maps:get(data, Merged),
  ?assertEqual(3, length(Data)),
  ?assert(lists:member({[], [], 0}, Data)),
  ?assert(lists:member({[method], [<<"GET">>], 3}, Data)),
  ?assert(lists:member({[method, status], [<<"GET">>, <<"200">>], 1}, Data)),
  ok.

%% only-attributed instrument (single vec, no base) -> renamed, shape preserved
renames_single_vec(_Config) ->
  Index = #{{otel_vec, <<"g_region">>} => <<"g">>},
  Raw = [#{type => gauge, name => {otel_vec, <<"g_region">>}, help => <<"h">>,
           labels => [region], data => [{[region], [<<"us">>], 5}]}],
  [Out] = instrument_otel_streams:group(Raw, Index),
  ?assertEqual(<<"g">>, maps:get(name, Out)),
  ?assertEqual([{[region], [<<"us">>], 5}], maps:get(data, Out)),
  ok.

%% base-only instrument (no vecs) -> passed through unchanged except the name,
%% preserving its unlabeled shape (and fields like start_time on real input)
passes_through_single_base(_Config) ->
  Index = #{{otel, <<"c">>} => <<"c">>},
  Raw = [#{type => counter, name => {otel, <<"c">>}, help => <<"h">>, val => 7}],
  [Out] = instrument_otel_streams:group(Raw, Index),
  ?assertEqual(#{type => counter, name => <<"c">>, help => <<"h">>, val => 7}, Out),
  ok.

passes_through_non_otel(_Config) ->
  Index = #{},
  Raw = [#{type => counter, name => <<"plain">>, help => <<"h">>, val => 7}],
  ?assertEqual(Raw, instrument_otel_streams:group(Raw, Index)),
  ok.
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `rebar3 ct --suite=instrument_otel_streams_SUITE`
Expected: FAIL with `undefined function instrument_otel_streams:group/2`.

- [ ] **Step 3: Implement the module**

Create `src/instrument_otel_streams.erl`:

```erlang
%% @doc Render-layer grouping for OTel meter instruments.
%%
%% The meter stores one internal vec per attribute key-set (plus an optional
%% base for unlabeled writes), each under its own registry name. This module
%% rewrites those registry names to the registered instrument name and folds
%% the entries that now share a name into a single stream. An instrument with
%% just one entry passes through with only its name rewritten (shape and
%% fields like start_time preserved); instruments with several entries are
%% merged into one labeled entry whose `data' rows carry per-row label names.
%% Non-OTel (standalone) metrics are untouched. Pure given the name index.
-module(instrument_otel_streams).

-export([group/1, group/2]).

-spec group([map()]) -> [map()].
group(RawMetrics) ->
  group(RawMetrics, instrument_meter:otel_name_index()).

-spec group([map()], #{term() => binary()}) -> [map()].
group(RawMetrics, Index) ->
  Renamed = [rename(M, Index) || M <- RawMetrics],
  merge_by_name(Renamed).

%% Rewrite an OTel entry's registry name to the user-facing instrument name so
%% base + vecs collapse onto one key. Non-OTel entries are untouched.
rename(#{name := N} = M, Index) ->
  case Index of
    #{N := User} -> M#{name => User};
    _ -> M
  end.

%% Group entries by (now user-facing) name, preserving first-seen order. A name
%% with a single entry passes through unchanged; names with several entries are
%% merged into one labeled stream.
merge_by_name(Entries) ->
  {Order, Groups} =
    lists:foldl(fun(#{name := N} = M, {Ord, Acc}) ->
      case Acc of
        #{N := Ms} -> {Ord, Acc#{N => Ms ++ [M]}};
        _ -> {Ord ++ [N], Acc#{N => [M]}}
      end
    end, {[], #{}}, Entries),
  [merge_group(maps:get(N, Groups)) || N <- Order].

merge_group([Single]) ->
  Single;
merge_group([First | _] = Ms) ->
  Rows = lists:append([rows(M) || M <- Ms]),
  Union = lists:usort(lists:append([Names || {Names, _, _} <- Rows])),
  #{type => maps:get(type, First),
    name => maps:get(name, First),
    help => maps:get(help, First, <<>>),
    labels => Union,
    data => Rows}.

%% Normalize one raw entry into a list of {LabelNames, LabelValues, Value} rows.
rows(#{data := Data}) ->
  Data;
rows(#{type := histogram, count := Count, sum := Sum, buckets := Buckets}) ->
  [{[], [], #{count => Count, sum => Sum, buckets => Buckets}}];
rows(#{val := Val}) ->
  [{[], [], Val}].
```

- [ ] **Step 4: Run the test, verify it passes**

Run: `rebar3 ct --suite=instrument_otel_streams_SUITE`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/instrument_otel_streams.erl test/instrument_otel_streams_SUITE.erl
git commit -m "add render-layer grouping that folds a meter instrument's vecs into one stream"
```

---

## Task 4: Wire grouping + per-row attributes into the OTLP/console pipeline

**Files:**
- Modify: `src/exporters/instrument_metrics_exporter.erl` (`collect_metrics/0` `:260`; labeled `convert_metric/2` clauses: counter `:287`, gauge `:313`, histogram `:345`)
- Test: `test/instrument_metrics_exporter_SUITE.erl`

- [ ] **Step 1: Write the failing test**

Add `otel_attributed_single_stream_test/1` to `-export`/`all/0` in `test/instrument_metrics_exporter_SUITE.erl`:

```erlang
otel_attributed_single_stream_test(_Config) ->
  _ = instrument_meter:unregister_all_instruments(),
  Meter = instrument_meter:get_meter(<<"single_stream">>),
  C = instrument_meter:create_counter(Meter, <<"sreq_total">>, #{}),
  ok = instrument_meter:add(C, 1),                                   %% unlabeled
  ok = instrument_meter:add(C, 2, #{method => <<"GET">>}),
  ok = instrument_meter:add(C, 3, #{method => <<"GET">>, status => 200}),

  Metrics = instrument_metrics_exporter:collect(),

  %% Exactly one stream, named as registered; no derived name.
  Named = [M || #{name := N} = M <- Metrics, N =:= <<"sreq_total">>],
  ?assertEqual(1, length(Named)),
  ?assertEqual([], [M || #{name := N} = M <- Metrics,
                         binary:match(N, <<"sreq_total_">>) =/= nomatch]),

  [#{data_points := DPs}] = Named,
  AllAttrs = [maps:get(attributes, DP) || DP <- DPs],
  %% includes the {} from the unlabeled add and the two attributed sets
  ?assert(lists:member(#{}, AllAttrs)),
  ?assert(lists:any(fun(A) -> maps:get(<<"method">>, A, undefined) =:= <<"GET">>
                              andalso maps:get(<<"status">>, A, undefined) =:= <<"200">>
                    end, AllAttrs)),
  ok.
```

- [ ] **Step 2: Run the test, verify it fails**

Run: `rebar3 ct --suite=instrument_metrics_exporter_SUITE --case=otel_attributed_single_stream_test`
Expected: FAIL — today there are separate `sreq_total` and `sreq_total_method`/`sreq_total_method_status` entries, so `length(Named)` is 1 but the `sreq_total_` match list is non-empty (assertion fails), and the data points are split across streams.

- [ ] **Step 3: Group before converting**

In `src/exporters/instrument_metrics_exporter.erl`, change `collect_metrics/0` (`:260`) to group first:

```erlang
collect_metrics() ->
  %% First, invoke all observable callbacks to update their values
  instrument_meter:collect_observables(),
  RawMetrics = instrument_otel_streams:group(instrument_registry:collect_all()),
  Timestamp = erlang:system_time(nanosecond),
  lists:filtermap(fun(Metric) ->
    case convert_metric(Metric, Timestamp) of
      undefined -> false;
      Converted -> {true, Converted}
    end
  end, RawMetrics).
```

- [ ] **Step 4: Use per-row label names in the labeled `convert_metric/2` clauses**

Each labeled clause currently binds `labels := Labels` (now unused) and ignores the per-row names with `{_, LabelVals, Val}`. Switch to per-row names. With `warnings_as_errors` on, rename the unused `Labels` to `_Labels`.

Counter labeled clause (`:287`):
```erlang
convert_metric(#{type := counter, name := Name, help := Help, labels := _Labels, data := Data}, Timestamp) ->
  #{
    name => to_binary(Name),
    description => extract_help(Help),
    unit => get_instrument_unit(Name),
    type => counter,
    data_points => [#{
      attributes => make_attributes(RowNames, LabelVals),
      value => Val,
      timestamp => Timestamp
    } || {RowNames, LabelVals, Val} <- Data]
  };
```

Gauge labeled clause (`:313`) — identical change: `labels := _Labels`, and `make_attributes(RowNames, LabelVals)` over `{RowNames, LabelVals, Val} <- Data`.

Histogram labeled clause (`:345`):
```erlang
convert_metric(#{type := histogram, name := Name, help := Help, labels := _Labels, data := Data}, Timestamp) ->
  #{
    name => to_binary(Name),
    description => extract_help(Help),
    unit => get_instrument_unit(Name),
    type => histogram,
    data_points => [#{
      attributes => make_attributes(RowNames, LabelVals),
      value => #{
        count => maps:get(count, Val),
        sum => maps:get(sum, Val),
        buckets => [#{bound => maps:get(upper_bound, B), count => maps:get(cumulative_count, B)}
                    || B <- maps:get(buckets, Val)]
      },
      timestamp => Timestamp
    } || {RowNames, LabelVals, Val} <- Data]
  };
```

(`make_attributes/2` already does `lists:zip(Labels, LabelVals)` then `to_binary` on both — atom row-names like `method` become `<<"method">>`, matching today's behavior.)

- [ ] **Step 5: Run the new test, verify it passes**

Run: `rebar3 ct --suite=instrument_metrics_exporter_SUITE --case=otel_attributed_single_stream_test`
Expected: PASS.

- [ ] **Step 6: Run the whole exporter suite**

Run: `rebar3 ct --suite=instrument_metrics_exporter_SUITE`
Expected: PASS for non-OTel tests. The OTel tests `metric_name_otel_with_attrs_test`, `metric_attrs_otel_single_test`, `metric_attrs_otel_multiple_test` may now behave differently (single stream instead of derived names) — if any fail, **do not weaken the new behavior**; note them and fix their expectations in Task 6. If they were written loosely (`>= 1`, substring) they will still pass.

- [ ] **Step 7: Commit**

```bash
git add src/exporters/instrument_metrics_exporter.erl test/instrument_metrics_exporter_SUITE.erl
git commit -m "fold meter attributed writes into one OTLP/console stream under the registered name"
```

---

## Task 5: Wire grouping + label union into the Prometheus formatter

**Files:**
- Modify: `src/instrument_prometheus.erl` (`format/0` `:20`; `format_counter/1` `:45`; `format_gauge/1` `:65`; `format_histogram/1` `:87`; add `union_labels/1`, `pad_row/3`)
- Test: `test/instrument_prometheus_SUITE.erl`

- [ ] **Step 1: Write the failing test**

Add `otel_attributed_union_test/1` to `-export`/`all/0` in `test/instrument_prometheus_SUITE.erl` (match that suite's existing setup idiom for starting the app / resetting state):

```erlang
otel_attributed_union_test(_Config) ->
  _ = instrument_meter:unregister_all_instruments(),
  Meter = instrument_meter:get_meter(<<"prom_union">>),
  C = instrument_meter:create_counter(Meter, <<"preq_total">>, #{}),
  ok = instrument_meter:add(C, 2, #{method => <<"GET">>}),
  ok = instrument_meter:add(C, 3, #{method => <<"GET">>, status => 200}),

  Text = instrument_prometheus:format(),

  %% No derived/mangled series name.
  ?assertEqual(nomatch, binary:match(Text, <<"preq_total_method">>)),
  %% Exactly one TYPE line for the metric.
  ?assertEqual(1, count_substr(Text, <<"# TYPE preq_total_total counter">>)),
  %% The single-key row is empty-filled on `status` (union of {method},{method,status}).
  ?assertNotEqual(nomatch, binary:match(Text, <<"preq_total_total{method=\"GET\",status=\"\"} 2">>)),
  ?assertNotEqual(nomatch, binary:match(Text, <<"preq_total_total{method=\"GET\",status=\"200\"} 3">>)),
  ok.

%% count non-overlapping occurrences of Needle in Hay
count_substr(Hay, Needle) ->
  length(binary:matches(Hay, Needle)).
```

(If `instrument_prometheus_SUITE` lacks a per-testcase reset, add `instrument_meter:unregister_all_instruments()` / `instrument_metric:unregister_all()` to its `init_per_testcase`, mirroring `instrument_meter_SUITE`.)

- [ ] **Step 2: Run the test, verify it fails**

Run: `rebar3 ct --suite=instrument_prometheus_SUITE --case=otel_attributed_union_test`
Expected: FAIL — today the output contains `preq_total_method ...` derived series and no union row `preq_total_total{method="GET",status=""} 2`.

- [ ] **Step 3: Group before formatting**

In `src/instrument_prometheus.erl`, change `format/0` (`:20`):

```erlang
format() ->
  Metrics = instrument_otel_streams:group(instrument_registry:collect_all()),
  iolist_to_binary([format_metric(M) || M <- Metrics]).
```

- [ ] **Step 4: Add union helpers**

Add to `src/instrument_prometheus.erl`:

```erlang
%% Union of label names across all rows of a grouped metric, sorted for
%% stable output. Rows that lack a label render it as an empty string.
union_labels(Data) ->
  lists:usort(lists:append([Names || {Names, _Vals, _V} <- Data])).

%% Pad one row's values to the union label set, empty-filling absent keys.
pad_row(Union, RowNames, RowVals) ->
  RowMap = maps:from_list(lists:zip(RowNames, RowVals)),
  [maps:get(L, RowMap, <<"">>) || L <- Union].
```

- [ ] **Step 5: Render labeled metrics over the union**

Replace the **labeled** clauses of `format_counter/1`, `format_gauge/1`, and `format_histogram/1` so they compute the union once and pad each row to it. The unlabeled clauses (`#{name, help, val}` / `#{name, help, count, sum, buckets}`) are unchanged.

Counter labeled clause (`:45`):
```erlang
format_counter(#{name := Name, help := Help, data := Data}) ->
  NameBin = format_name(Name),
  TotalName = <<NameBin/binary, "_total">>,
  Union = union_labels(Data),
  [
    <<"# HELP ">>, TotalName, <<" ">>, escape_help(Help), <<"\n">>,
    <<"# TYPE ">>, TotalName, <<" counter\n">>,
    [format_labeled_value(TotalName, Union, pad_row(Union, RowNames, RowVals), Val)
     || {RowNames, RowVals, Val} <- Data]
  ].
```

Gauge labeled clause (`:65`):
```erlang
format_gauge(#{name := Name, help := Help, data := Data}) ->
  NameBin = format_name(Name),
  Union = union_labels(Data),
  [
    <<"# HELP ">>, NameBin, <<" ">>, escape_help(Help), <<"\n">>,
    <<"# TYPE ">>, NameBin, <<" gauge\n">>,
    [format_labeled_value(NameBin, Union, pad_row(Union, RowNames, RowVals), Val)
     || {RowNames, RowVals, Val} <- Data]
  ].
```

Histogram labeled clause (`:87`):
```erlang
format_histogram(#{name := Name, help := Help, data := Data}) ->
  NameBin = format_name(Name),
  Union = union_labels(Data),
  [
    <<"# HELP ">>, NameBin, <<" ">>, escape_help(Help), <<"\n">>,
    <<"# TYPE ">>, NameBin, <<" histogram\n">>,
    [format_histogram_data(NameBin, Union, pad_row(Union, RowNames, RowVals), Val)
     || {RowNames, RowVals, Val} <- Data]
  ].
```

> Note: the original labeled clauses matched `labels := Labels` and destructured rows as `{_LabelNames, LabelVals, Val}`. After grouping, `data` rows carry per-row names, so we read `{RowNames, RowVals, Val}` and pad to the union. `format_labeled_value/4` and `format_histogram_data/4` already accept a `(Name, LabelNames, LabelVals, Val)` shape, so passing `(…, Union, PaddedVals, …)` needs no change to those helpers.

- [ ] **Step 6: Run the new test, verify it passes**

Run: `rebar3 ct --suite=instrument_prometheus_SUITE --case=otel_attributed_union_test`
Expected: PASS.

- [ ] **Step 7: Run the whole Prometheus suite**

Run: `rebar3 ct --suite=instrument_prometheus_SUITE`
Expected: PASS. Standalone vec metrics still have a single consistent label set per metric, so their union == their labels and padding is a no-op.

- [ ] **Step 8: Commit**

```bash
git add src/instrument_prometheus.erl test/instrument_prometheus_SUITE.erl
git commit -m "render meter attributed metrics as one Prometheus family with unioned, empty-filled labels"
```

---

## Task 6: Update existing OTel tests, add the regression, and the CHANGELOG

**Files:**
- Modify: `test/instrument_metrics_exporter_SUITE.erl` (`metric_name_otel_with_attrs_test`, `metric_attrs_otel_single_test`, `metric_attrs_otel_multiple_test`)
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Confirm the existing OTel tests still pass (they are loose)**

Run: `rebar3 ct --suite=instrument_metrics_exporter_SUITE --case=metric_name_otel_with_attrs_test,metric_attrs_otel_single_test,metric_attrs_otel_multiple_test`
Expected: PASS. These three were written before the fix with loose assertions (substring name match, `length(...) >= 1`, flat-map over *all* matching metrics), so they keep passing after grouping — but their comments describe the old "one metric per attribute schema" behavior and their assertions no longer guard against regressions. This task **tightens** them to the single-stream contract; it is hardening, not a breakage fix. (`metric_attrs_type_conversion_test` is also loose and correct as-is — leave it.)

Note: `instrument_metrics_exporter_SUITE`'s `init_per_testcase` resets only exporters, not instruments, so each tightened test below starts with `instrument_meter:unregister_all_instruments()` and filters by the **exact** registered name, making the "exactly one stream" assertions robust against instruments left by other tests.

- [ ] **Step 2: Tighten `metric_name_otel_with_attrs_test` to the single-stream contract**

Replace its body so it asserts the fixed behavior (one stream, no derived name). Keep the same instrument name (`otel_attr_counter`) the test already uses:

```erlang
metric_name_otel_with_attrs_test(_Config) ->
  _ = instrument_meter:unregister_all_instruments(),
  Meter = instrument_meter:get_meter(<<"attr_service">>),
  Counter = instrument_meter:create_counter(Meter, <<"otel_attr_counter">>, #{
    description => <<"OTel counter with attributes">>
  }),
  ok = instrument_meter:add(Counter, 1, #{method => <<"GET">>}),
  ok = instrument_meter:add(Counter, 2, #{method => <<"POST">>}),
  ok = instrument_meter:add(Counter, 3, #{method => <<"GET">>, status => 200}),

  Metrics = instrument_metrics_exporter:collect(),

  %% Exactly one stream, named as registered; no derived `_method`/`_method_status` names.
  Named = [M || #{name := N} = M <- Metrics, N =:= <<"otel_attr_counter">>],
  ?assertEqual(1, length(Named)),
  ?assertEqual([], [M || #{name := N} = M <- Metrics,
                         binary:match(N, <<"otel_attr_counter_">>) =/= nomatch]),
  ok.
```

- [ ] **Step 3: Tighten `metric_attrs_otel_single_test` and `metric_attrs_otel_multiple_test`**

Replace both bodies (both currently pass loosely; these versions assert the single-stream contract). `metric_attrs_otel_single_test` (gauge, one key-set `[host]`, two values):

```erlang
metric_attrs_otel_single_test(_Config) ->
  _ = instrument_meter:unregister_all_instruments(),
  Meter = instrument_meter:get_meter(<<"single_attr_svc">>),
  Gauge = instrument_meter:create_gauge(Meter, <<"otel_single_attr_gauge">>, #{}),

  ok = instrument_meter:set(Gauge, 42.5, #{host => <<"server1">>}),
  ok = instrument_meter:set(Gauge, 38.2, #{host => <<"server2">>}),

  Metrics = instrument_metrics_exporter:collect(),

  %% One stream under the registered name; no derived name.
  Named = [M || #{name := N} = M <- Metrics, N =:= <<"otel_single_attr_gauge">>],
  ?assertEqual(1, length(Named)),
  ?assertEqual([], [M || #{name := N} = M <- Metrics,
                         binary:match(N, <<"otel_single_attr_gauge_">>) =/= nomatch]),

  [#{data_points := DPs}] = Named,
  AllAttrs = [maps:get(attributes, DP) || DP <- DPs],
  ?assert(lists:any(fun(A) -> maps:get(<<"host">>, A, undefined) =:= <<"server1">> end, AllAttrs)),
  ?assert(lists:any(fun(A) -> maps:get(<<"host">>, A, undefined) =:= <<"server2">> end, AllAttrs)),
  ok.
```

`metric_attrs_otel_multiple_test` (histogram, one key-set `[endpoint, method]`, two values):

```erlang
metric_attrs_otel_multiple_test(_Config) ->
  _ = instrument_meter:unregister_all_instruments(),
  Meter = instrument_meter:get_meter(<<"multi_attr_svc">>),
  Histogram = instrument_meter:create_histogram(Meter, <<"otel_multi_attr_hist">>, #{
    boundaries => [0.1, 0.5, 1.0, 5.0]
  }),

  ok = instrument_meter:record(Histogram, 0.25, #{method => <<"GET">>, endpoint => <<"/api">>}),
  ok = instrument_meter:record(Histogram, 0.8, #{method => <<"POST">>, endpoint => <<"/api">>}),

  Metrics = instrument_metrics_exporter:collect(),

  %% One histogram stream under the registered name; no derived `_vec` name.
  Named = [M || #{name := N} = M <- Metrics, N =:= <<"otel_multi_attr_hist">>],
  ?assertEqual(1, length(Named)),
  ?assertEqual([], [M || #{name := N} = M <- Metrics,
                         binary:match(N, <<"otel_multi_attr_hist_vec">>) =/= nomatch]),

  [#{data_points := DPs}] = Named,
  AllAttrs = [maps:get(attributes, DP) || DP <- DPs],
  ?assert(lists:any(fun(A) -> maps:get(<<"method">>, A, undefined) =:= <<"GET">> end, AllAttrs)),
  ?assert(lists:any(fun(A) -> maps:get(<<"endpoint">>, A, undefined) =:= <<"/api">> end, AllAttrs)),
  ok.
```

- [ ] **Step 4: Add a no-mangled-name regression test**

Add `no_mangled_otel_series_test/1` to `-export`/`all/0` in `test/instrument_metrics_exporter_SUITE.erl`:

```erlang
no_mangled_otel_series_test(_Config) ->
  _ = instrument_meter:unregister_all_instruments(),
  M = instrument_meter:get_meter(<<"nomangle">>),
  Ctr = instrument_meter:create_counter(M, <<"nm_counter">>, #{}),
  ok = instrument_meter:add(Ctr, 1, #{method => <<"GET">>, status => 200}),
  H = instrument_meter:create_histogram(M, <<"nm_latency">>, #{boundaries => [1, 5, 10]}),
  ok = instrument_meter:record(H, 2.0, #{endpoint => <<"/a">>}),

  Names = [N || #{name := N} <- instrument_metrics_exporter:collect()],

  %% No counter/gauge style `<name>_<labels>` and no histogram `<name>_vec_<labels>`.
  ?assertEqual([], [N || N <- Names, binary:match(N, <<"nm_counter_">>) =/= nomatch]),
  ?assertEqual([], [N || N <- Names, binary:match(N, <<"nm_latency_vec">>) =/= nomatch]),
  %% The registered names are present.
  ?assert(lists:member(<<"nm_counter">>, Names)),
  ?assert(lists:member(<<"nm_latency">>, Names)),
  ok.
```

- [ ] **Step 5: Run the full exporter suite**

Run: `rebar3 ct --suite=instrument_metrics_exporter_SUITE`
Expected: PASS (including the updated and new tests).

- [ ] **Step 6: Update the CHANGELOG**

Add an entry under a new unreleased heading at the top of `CHANGELOG.md`:

```markdown
## [Unreleased]

### Fixed
- Meter attributed writes (`add/3`, `record/3`, `set/3`, and attributed
  observable callbacks) now export under the **registered instrument name**
  with the attributes as data-point attributes, instead of under a
  label-derived name (`<name>_<labels>` for counters/gauges, `<name>_vec_<labels>`
  for histograms). The data was previously unreachable by the documented name.
- The registered instrument no longer appears as a constant-zero, no-attributes
  series when it is only ever written with attributes. The base instrument is
  now registered lazily on its first unlabeled write.

### Changed
- An instrument that is created but never written is no longer exported as a
  zero series; it appears on its first write (matching the OpenTelemetry SDKs).
- In the Prometheus exposition, when one instrument is written with different
  attribute key-sets, the metric family's label columns are the union across
  those key-sets, with empty-string values for absent keys.
```

- [ ] **Step 7: Run the full test suite (final regression gate)**

Run: `rebar3 ct`
Expected: PASS across all suites (notably `instrument_meter_SUITE`, `instrument_metrics_exporter_SUITE`, `instrument_prometheus_SUITE`, `instrument_observable_SUITE`, `instrument_cardinality_SUITE`, `instrument_e2e_SUITE`).

- [ ] **Step 8: Commit**

```bash
git add test/instrument_metrics_exporter_SUITE.erl CHANGELOG.md
git commit -m "tighten OTel exporter tests to the single-stream contract and add a no-mangled-name regression"
```

---

## Self-review (run before handing off / executing)

- **Spec coverage (spec §5):** Piece 1 resolution → Task 2; Piece 2 merge/normalize → Task 3; Piece 3 OTLP/console per-row attrs → Task 4; Piece 4 Prometheus union → Task 5; Piece 5 lazy base registration → Task 1; existing-test updates + CHANGELOG + regression → Task 6. All covered.
- **Type/name consistency:** the grouping output (`#{type, name, help, labels, data=[{Names,Vals,Val}]}`) is consumed identically by `convert_metric/2` (per-row `{RowNames, LabelVals, Val}`) and `instrument_prometheus` (per-row + union). `otel_name_index/0` is defined in Task 2 and used in Task 3. `union_labels/1`/`pad_row/3` defined and used in Task 5.
- **warnings_as_errors:** Task 4 explicitly renames the now-unused `labels := Labels` → `labels := _Labels`; Task 5's labeled clauses drop the `labels` binding entirely. New functions are all exported or used.
- **Out of scope (do not touch here):** histogram OTLP encoder (`upper_bound`/de-cumulation) — that is PR 2, a separate plan. `make_vec_name`, `ensure_vec_metric`, `instrument_vector`, and the standalone `instrument_metric` vec API are unchanged.

## Risk notes for the executor

- If `instrument_prometheus_SUITE` has no per-testcase reset, add one (Step 5.1 note) so state from other suites/tests doesn't leak into the union assertions.
- Standalone (non-OTel) metrics must remain byte-identical in both outputs — they pass through `group/1` untouched. The full-suite run in Task 6 Step 7 is the guard.
- Atom vs binary label names: meter-path row names are atoms; `make_attributes/2` and `format_labels` already `to_binary` them, so unioning/padding on atoms is consistent. Do not pre-convert names to binaries in the grouping.
