# Meter Row Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the meter's per-key-set vec storage with a per-instrument flat row store so attributed metrics export under their registered name with no per-scrape reconciliation and no phantom zero series.

**Architecture:** One registry entry per meter instrument (`{otel, Name}`) whose `#metric.handle` is a new `#otel_rows{}` container holding every row (`{Names, Values} → row #metric{}`), a precomputed label union, and stream metadata. Writes resolve rows via one persistent_term get; row creation is serialized through the existing `instrument_registry` gen_server; collect reads the container and emits the final stream shape directly. Spec: `docs/superpowers/specs/2026-06-10-meter-row-store-design.md` (read it first — §3–§9 define every structure and path; Appendix A has sequence diagrams per use case).

**Tech Stack:** Erlang/OTP 27–29, rebar3, Common Test, NIF-backed atomics (pre-existing), `warnings_as_errors` is ON (an unused unexported function fails the build — deletions must land in the same commit as their last caller's removal).

---

## Context for the implementer

- **Baseline:** branch from `master` (`be7e75e`, release 1.1.3). The spec and this plan live on the `meter-metrics-export-fixes` docs branch; Task 0 copies them onto the implementation branch.
- **The bugs being fixed** (visible on master): `instrument_meter:add(C, 1, #{method => <<"GET">>})` stores data in a vec registered under a *derived* name (`{otel_vec, <<"name_method">>}` — `make_vec_name`, `src/instrument_meter.erl:652`), so exports show a mangled series plus a constant-zero phantom under the registered name (the base registered eagerly at create, `src/instrument_meter.erl:427`).
- **Build/test commands:**
  - `rebar3 compile` (first build compiles the NIF via cmake hooks — slow once, then cached)
  - `rebar3 ct --suite=test/instrument_meter_SUITE` (one suite)
  - `rebar3 ct --suite=test/instrument_meter_SUITE --case=row_store_single_entry_test` (one case)
  - `rebar3 ct` (everything; `instrument_e2e_SUITE` needs Docker — if it fails locally without Docker, verify every other suite is green; CI gates e2e separately)
  - `rebar3 dialyzer` (CI runs it; run before the final commit)
- **Suites that must stay green UNMODIFIED** (proof the public standalone vec API is untouched): `instrument_vector_SUITE`, `instrument_cardinality_SUITE`, `instrument_leaks_SUITE`, `instrument_race_SUITE`, `instrument_stress_SUITE`.

### File map

| File | Change |
|---|---|
| `include/instrument.hrl` | Add `#otel_rows{}` record + overflow-canon macro |
| `src/instrument_registry.erl` | Add `create_otel_row/2` API + handle_call + helpers; `note_cardinality_dropped/1`; `#otel_rows` clauses in `erase_cached_labels/2` and `release_exemplar_reservoirs/1` |
| `src/instrument_meter.erl` | Rewrite create/write/collect/observable/unregister; delete the vec-borrowing machinery |
| `src/instrument_prometheus.erl` | Empty-data skip; pad rows to the family union; drop `{otel_vec,_}` clause |
| `src/exporters/instrument_metrics_exporter.erl` | Empty-data skip; per-row label names; `start_time` on labeled counter/histogram data points; drop `{otel_vec,_}` clauses |
| `src/instrument_test.erl` | Drop the `{otel_vec,_}` `name_matches/2` clause |
| `test/instrument_meter_SUITE.erl` | New row-store tests; updated mangled-name assertions |
| `test/instrument_prometheus_SUITE.erl` | Union/empty-fill test; never-written test |
| `test/instrument_metrics_exporter_SUITE.erl` | Single-stream contract tests; start_time test |
| `test/instrument_observable_SUITE.erl` | Updated derived-name assertions |
| `CHANGELOG.md` | Unreleased entries |

---

### Task 0: Worktree and baseline

**Files:** none (setup only)

- [ ] **Step 0.1: Create the worktree from master**

```bash
cd /Users/gstarinkin/src/instrument
git worktree add ../instrument-meter-row-store -b meter-row-store be7e75e
cd ../instrument-meter-row-store
```

- [ ] **Step 0.2: Copy the spec and this plan onto the branch** (they get dropped before the PR, as with previous branches)

```bash
mkdir -p docs/superpowers/specs docs/superpowers/plans
git -C ../instrument show meter-metrics-export-fixes:docs/superpowers/specs/2026-06-10-meter-row-store-design.md > docs/superpowers/specs/2026-06-10-meter-row-store-design.md
git -C ../instrument show meter-metrics-export-fixes:docs/superpowers/plans/2026-06-10-meter-row-store.md > docs/superpowers/plans/2026-06-10-meter-row-store.md
git add docs && git commit -m "carry the row-store design spec and plan for reference during implementation"
```

- [ ] **Step 0.3: Baseline build + meter suite green**

Run: `rebar3 compile && rebar3 ct --suite=test/instrument_meter_SUITE`
Expected: PASS (this is master — everything green before we start)

---

### Task 1: `#otel_rows` container + `instrument_registry:create_otel_row/2`

The registry learns to create rows inside an instrument container, serialized in its gen_server exactly like `create_vector_metric` is today. Nothing calls it yet (it's exported, so `warnings_as_errors` is satisfied).

**Files:**
- Modify: `include/instrument.hrl` (append after the `#vector{}` record)
- Modify: `src/instrument_registry.erl`
- Test: `test/instrument_meter_SUITE.erl`

- [ ] **Step 1.1: Add the record and overflow macro to `include/instrument.hrl`**

Append after the `#vector{}` record:

```erlang
%% Flat per-instrument row store for the OTel meter. One registered
%% #metric{handle = #otel_rows{}} per instrument; rows are unregistered
%% per-attribute-set storage handles keyed by the canonical attribute form
%% {SortedNames, Values}. The unlabeled series is the row keyed {[], []}.
-record(otel_rows, {
  kind                :: counter | up_down_counter | histogram | gauge
                       | observable_counter | observable_gauge
                       | observable_up_down_counter,
  help = <<>>         :: binary(),
  start_time          :: integer() | undefined,  %% instrument creation, ns
  boundaries          :: [number()] | undefined, %% histograms only
  union = []          :: [atom() | binary()],    %% sorted union of row label names
  rows = #{}          :: #{{list(), list()} => #metric{}}
}).

%% The OTel spec's cardinality-overflow attribute set, as a canonical row key.
-define(OTEL_OVERFLOW_CANON, {[<<"otel.metric.overflow">>], [<<"true">>]}).
```

- [ ] **Step 1.2: Write the failing tests**

In `test/instrument_meter_SUITE.erl`, add to the `-export` list and to `all()`:

```erlang
  create_otel_row_test,
  create_otel_row_race_test,
  create_otel_row_overflow_test,
  create_otel_row_not_found_test
```

Add the test bodies and helper at the end of the file:

```erlang
%% ============================================================================
%% Registry row-store machinery (instrument_registry:create_otel_row/2)
%% ============================================================================

mk_container(RegName, Kind) ->
  mk_container(RegName, Kind, undefined).

mk_container(RegName, Kind, Boundaries) ->
  #metric{
    name = RegName,
    handle = #otel_rows{
      kind = Kind,
      help = <<>>,
      start_time = erlang:system_time(nanosecond),
      boundaries = Boundaries,
      union = [],
      rows = #{}
    },
    collect = {instrument_meter, collect_instrument, [RegName]}
  }.

create_otel_row_test(_Config) ->
  RegName = {otel, <<"rowstore_basic">>},
  ok = instrument_metric:register(mk_container(RegName, counter)),

  Canon = {[method, status], [<<"GET">>, <<"200">>]},
  {ok, Row} = instrument_registry:create_otel_row(RegName, Canon),
  ?assertMatch(#metric{}, Row),

  %% The parent record grew: row stored under Canon, union merged + sorted.
  #metric{handle = #otel_rows{union = Union, rows = Rows}} =
    instrument_registry:lookup(RegName),
  ?assertEqual([method, status], Union),
  ?assertMatch(#{Canon := #metric{}}, Rows),

  %% The row is cached for the write fast path.
  ?assertMatch(#metric{},
               persistent_term:get({instrument_label, RegName, Canon}, undefined)),

  %% A second key-set merges into the union.
  Canon2 = {[region], [<<"eu">>]},
  {ok, _} = instrument_registry:create_otel_row(RegName, Canon2),
  #metric{handle = #otel_rows{union = Union2, rows = Rows2}} =
    instrument_registry:lookup(RegName),
  ?assertEqual([method, region, status], Union2),
  ?assertEqual(2, map_size(Rows2)),
  ok.

create_otel_row_race_test(_Config) ->
  %% A second create for the same Canon returns the SAME row (race loser path).
  RegName = {otel, <<"rowstore_race">>},
  ok = instrument_metric:register(mk_container(RegName, counter)),
  Canon = {[a], [<<"x">>]},
  {ok, Row1} = instrument_registry:create_otel_row(RegName, Canon),
  {ok, Row2} = instrument_registry:create_otel_row(RegName, Canon),
  ?assertEqual(Row1, Row2),
  #metric{handle = #otel_rows{rows = Rows}} = instrument_registry:lookup(RegName),
  ?assertEqual(1, map_size(Rows)),
  ok.

create_otel_row_overflow_test(_Config) ->
  RegName = {otel, <<"rowstore_overflow">>},
  ok = instrument_metric:register(mk_container(RegName, counter)),
  os:putenv("OTEL_METRIC_CARDINALITY_LIMIT", "2"),
  try
    {ok, _} = instrument_registry:create_otel_row(RegName, {[a], [<<"1">>]}),
    {ok, _} = instrument_registry:create_otel_row(RegName, {[a], [<<"2">>]}),
    %% At the limit: the third distinct set lands on the overflow row.
    {ok, Row3} = instrument_registry:create_otel_row(RegName, {[a], [<<"3">>]}),
    #metric{handle = #otel_rows{rows = Rows, union = Union}} =
      instrument_registry:lookup(RegName),
    ?assertEqual(3, map_size(Rows)),            %% 2 real + 1 overflow
    OverflowCanon = {[<<"otel.metric.overflow">>], [<<"true">>]},
    ?assertMatch(#{OverflowCanon := _}, Rows),
    ?assert(lists:member(<<"otel.metric.overflow">>, Union)),
    %% Returned row IS the overflow row, and it is cached under the overflow key.
    ?assertEqual(Row3,
                 persistent_term:get({instrument_label, RegName, OverflowCanon},
                                     undefined)),
    %% The requested (dropped) Canon is NOT cached — no unbounded pt growth.
    ?assertEqual(undefined,
                 persistent_term:get({instrument_label, RegName, {[a], [<<"3">>]}},
                                     undefined))
  after
    os:unsetenv("OTEL_METRIC_CARDINALITY_LIMIT")
  end,
  ok.

create_otel_row_not_found_test(_Config) ->
  ?assertEqual({error, not_found},
               instrument_registry:create_otel_row({otel, <<"rowstore_missing">>},
                                                   {[a], [<<"x">>]})),
  ok.
```

`test/instrument_meter_SUITE.erl` already has `-include("instrument.hrl").` and `-include_lib("stdlib/include/assert.hrl").` near the top — check, and add whichever is missing.

- [ ] **Step 1.3: Run to verify failure**

Run: `rebar3 ct --suite=test/instrument_meter_SUITE --case=create_otel_row_test`
Expected: FAIL — `undef instrument_registry:create_otel_row/2` (and a compile error first if `#otel_rows` was not added — Step 1.1 fixes that).

- [ ] **Step 1.4: Implement in `src/instrument_registry.erl`**

Add to the second export list (next to `create_vector_metric/2`):

```erlang
  create_otel_row/2,
  note_cardinality_dropped/1
```

Add the API functions next to `create_vector_metric/2` (`src/instrument_registry.erl:90`):

```erlang
%% Create (or fetch) the row for one canonical attribute set of an OTel
%% meter instrument. Serialized through the gen_server so concurrent first
%% writers cannot mint duplicate rows. Returns the overflow row when the
%% instrument is at its cardinality limit.
-spec create_otel_row(term(), {list(), list()}) -> {ok, #metric{}} | {error, term()}.
create_otel_row(RegName, Canon) ->
  gen_server:call(?MODULE, {create_otel_row, RegName, Canon}).

%% Count one write dropped into the overflow series (feeds cardinality_dropped/1).
-spec note_cardinality_dropped(term()) -> ok.
note_cardinality_dropped(Name) ->
  _ = incr_dropped_count(Name),
  ok.
```

Add the handle_call clause after the `{create_vector_metric, Name, Label}` clause (`src/instrument_registry.erl:143-151`):

```erlang
handle_call({create_otel_row, RegName, Canon}, _From, State) ->
  {reply, do_create_otel_row(RegName, Canon), State};
```

Add the internals after `mk_metric/1` (`src/instrument_registry.erl:401-408`):

```erlang
%% @private Row creation for the OTel meter row store. Runs in the
%% gen_server. Re-checks existence (race losers get the winner's row) and
%% the cardinality limit (the overflow canon itself is exempt). The
%% replacing pt put inside do_reg_metric/1 is the one literal-GC sweep per
%% new row; the row cache put is a fresh key and only-if-absent, so it
%% never sweeps.
do_create_otel_row(RegName, Canon) ->
  case ets:lookup(instrument_lib:table(), RegName) of
    [#metric{handle = #otel_rows{rows = Rows} = Container} = Parent] ->
      case maps:find(Canon, Rows) of
        {ok, Row} ->
          ok = cache_otel_row(RegName, Canon, Row),
          {ok, Row};
        error ->
          Limit = instrument_config:get_metric_cardinality_limit(),
          AtLimit = map_size(Rows) >= Limit andalso Canon =/= ?OTEL_OVERFLOW_CANON,
          case AtLimit of
            true ->
              do_create_otel_row(RegName, ?OTEL_OVERFLOW_CANON);
            false ->
              Row = mk_otel_row(RegName, Canon, Container),
              insert_otel_row(RegName, Canon, Row, Parent)
          end
      end;
    _ ->
      {error, not_found}
  end.

%% @private Mint the storage handle for one row. Counter rows carry a
%% per-row {Ref, StartTime}; everything non-counter/non-histogram (gauge,
%% up_down_counter, observable_*) is gauge storage — set / inc / dec
%% semantics at the NIF layer. Row names are decorative (never registered).
mk_otel_row(RegName, Canon, #otel_rows{kind = Kind, boundaries = Boundaries}) ->
  RowName = {otel_row, RegName, Canon},
  case Kind of
    counter   -> instrument_counter:new_counter(RowName, <<>>);
    histogram -> instrument_histogram:new_histogram(RowName, <<>>, Boundaries);
    _         -> instrument_gauge:new_gauge(RowName, <<>>)
  end.

%% @private Store the row in the container, merge the union, re-register
%% the parent (ETS tables + replacing pt put), cache the row.
insert_otel_row(RegName, {Names, _Values} = Canon, Row, #metric{handle = Container} = Parent) ->
  #otel_rows{union = Union, rows = Rows} = Container,
  Container2 = Container#otel_rows{
    union = lists:umerge(Names, Union),   %% both sides sorted
    rows = maps:put(Canon, Row, Rows)
  },
  do_reg_metric(Parent#metric{handle = Container2}),
  ok = cache_otel_row(RegName, Canon, Row),
  {ok, Row}.

%% @private Cache only-if-absent: putting over an existing pt key is a
%% REPLACING put, which schedules a needless literal-GC sweep on the
%% race-loser path. (cache_label/3 puts unconditionally — don't use it
%% blindly here.)
cache_otel_row(RegName, Canon, Row) ->
  case persistent_term:get({instrument_label, RegName, Canon}, undefined) of
    undefined -> cache_label(RegName, Canon, Row);
    _ -> ok
  end.
```

Note `Names` from the meter side is always sorted (it comes out of `lists:sort/1`); `?OTEL_OVERFLOW_CANON`'s single name is trivially sorted; `lists:umerge/2` requires both inputs sorted, which holds.

- [ ] **Step 1.5: Add the cleanup clauses** so unregistering a container erases its row caches and exemplar reservoirs.

In `erase_cached_labels/2` (`src/instrument_registry.erl:232-244`), insert a clause between the `undefined` clause and the `#vector` clause:

```erlang
erase_cached_labels(Name, #metric{handle = #otel_rows{rows = Rows}}) ->
  maps:fold(fun(Canon, _Row, Acc) ->
    case persistent_term:get({instrument_label, Name, Canon}, undefined) of
      undefined -> Acc;
      _ ->
        persistent_term:erase({instrument_label, Name, Canon}),
        Acc + 1
    end
  end, 0, Rows);
```

In `release_exemplar_reservoirs/1` (`src/instrument_registry.erl:249-256`), insert a clause between the `undefined` clause and the `#vector` clause:

```erlang
release_exemplar_reservoirs(#metric{handle = #otel_rows{rows = Rows}}) ->
  maps:foreach(fun(_Canon, Row) ->
    instrument_histogram:cleanup(Row)
  end, Rows);
```

- [ ] **Step 1.6: Run the four tests**

Run: `rebar3 ct --suite=test/instrument_meter_SUITE --case=create_otel_row_test,create_otel_row_race_test,create_otel_row_overflow_test,create_otel_row_not_found_test`
Expected: PASS. (`collect_instrument` does not exist yet — these tests never collect, so the dangling MFA is harmless.)

- [ ] **Step 1.7: Full meter suite + standalone-vec suites still green**

Run: `rebar3 ct --suite=test/instrument_meter_SUITE,test/instrument_vector_SUITE,test/instrument_cardinality_SUITE`
Expected: PASS — nothing existing changed behavior.

- [ ] **Step 1.8: Commit**

```bash
git add include/instrument.hrl src/instrument_registry.erl test/instrument_meter_SUITE.erl
git commit -m "add a per-instrument row store primitive to the registry so the meter can stop deriving vec names"
```

---

### Task 2: The switch — meter writes/collect/observables on the row store, formatters consume it

This task is one atomic commit by necessity: changing `#otel_instrument.handle` flips creation, writes, collection, and observables together, and the emitted heterogeneous rows would crash master's `lists:zip(Labels, LabelVals)` formatters — so meter, prometheus, exporter, and the suites asserting old (mangled) behavior all move in the same commit. The steps inside stay small.

**Files:**
- Modify: `src/instrument_meter.erl` (most of the module)
- Modify: `src/instrument_prometheus.erl:20-103, 151-159`
- Modify: `src/exporters/instrument_metrics_exporter.erl:272-400`
- Modify: `src/instrument_test.erl:594-597`
- Test: `test/instrument_meter_SUITE.erl`, `test/instrument_prometheus_SUITE.erl`, `test/instrument_metrics_exporter_SUITE.erl`, `test/instrument_observable_SUITE.erl`

- [ ] **Step 2.1: Write the new failing behavioral tests (meter suite)**

In `test/instrument_meter_SUITE.erl`, add to `-export` and `all()`:

```erlang
  row_store_single_entry_test,
  unlabeled_write_is_a_row_test,
  never_written_emits_nothing_test,
  attributed_only_no_phantom_test
```

Bodies:

```erlang
row_store_single_entry_test(_Config) ->
  Meter = instrument_meter:get_meter(<<"rs_meter">>),
  C = instrument_meter:create_counter(Meter, <<"rs_requests">>, #{description => <<"reqs">>}),
  ok = instrument_meter:add(C, 1, #{method => <<"GET">>}),
  ok = instrument_meter:add(C, 2, #{method => <<"GET">>, status => 200}),
  ok = instrument_meter:add(C, 3),

  %% ONE registry entry, under the real name; no derived names anywhere.
  Names = persistent_term:get(instrument_metrics, []),
  Mine = [N || N <- Names,
               case N of
                 {otel, <<"rs_requests", _/binary>>} -> true;
                 {otel_vec, <<"rs_requests", _/binary>>} -> true;
                 _ -> false
               end],
  ?assertEqual([{otel, <<"rs_requests">>}], Mine),

  %% The container holds all three rows, including {[],[]} for the unlabeled add.
  #metric{handle = #otel_rows{union = Union, rows = Rows}} =
    instrument_registry:lookup({otel, <<"rs_requests">>}),
  ?assertEqual([method, status], Union),
  ?assertEqual(3, map_size(Rows)),
  ?assertMatch(#{{[], []} := _}, Rows),

  %% The collect callback emits one final-shape stream.
  Stream = instrument_meter:collect_instrument({otel, <<"rs_requests">>}),
  ?assertMatch(#{name := {otel, <<"rs_requests">>},
                 type := counter,
                 help := <<"reqs">>,
                 labels := [method, status]}, Stream),
  ?assert(is_integer(maps:get(start_time, Stream))),
  Data = maps:get(data, Stream),
  ?assertEqual(3, length(Data)),
  ?assert(lists:member({[], [], 3.0}, Data)),
  ?assert(lists:member({[method], [<<"GET">>], 1.0}, Data)),
  ?assert(lists:member({[method, status], [<<"GET">>, <<"200">>], 2.0}, Data)),
  ok.

unlabeled_write_is_a_row_test(_Config) ->
  Meter = instrument_meter:get_meter(<<"rs_meter">>),
  G = instrument_meter:create_gauge(Meter, <<"rs_gauge">>),
  ok = instrument_meter:set(G, 42),
  #metric{handle = #otel_rows{rows = Rows}} =
    instrument_registry:lookup({otel, <<"rs_gauge">>}),
  ?assertEqual(1, map_size(Rows)),
  ?assertMatch(#{{[], []} := _}, Rows),
  %% Second write hits the cached row — same row count, updated value.
  ok = instrument_meter:set(G, 43),
  ?assertMatch(#{data := [{[], [], 43.0}]},
               instrument_meter:collect_instrument({otel, <<"rs_gauge">>})),
  ok.

never_written_emits_nothing_test(_Config) ->
  Meter = instrument_meter:get_meter(<<"rs_meter">>),
  _ = instrument_meter:create_counter(Meter, <<"rs_silent">>),
  %% Registered (collect_all sees it) but empty — both formats emit nothing.
  ?assertMatch(#{data := []},
               instrument_meter:collect_instrument({otel, <<"rs_silent">>})),
  ?assertEqual(nomatch,
               binary:match(instrument_prometheus:format(), <<"rs_silent">>)),
  ?assertEqual([], [M || #{name := <<"rs_silent">>} = M
                         <- instrument_metrics_exporter:collect()]),
  ok.

attributed_only_no_phantom_test(_Config) ->
  Meter = instrument_meter:get_meter(<<"rs_meter">>),
  C = instrument_meter:create_counter(Meter, <<"rs_attr_only">>),
  ok = instrument_meter:add(C, 5, #{path => <<"/x">>}),
  #metric{handle = #otel_rows{rows = Rows}} =
    instrument_registry:lookup({otel, <<"rs_attr_only">>}),
  %% No {[],[]} row — nothing was written unlabeled, so no zero series.
  ?assertEqual([{[path], [<<"/x">>]}], maps:keys(Rows)),
  Text = instrument_prometheus:format(),
  ?assertEqual(nomatch, binary:match(Text, <<"rs_attr_only_total 0">>)),
  ?assertNotEqual(nomatch,
                  binary:match(Text, <<"rs_attr_only_total{path=\"/x\"} 5">>)),
  ok.
```

- [ ] **Step 2.2: Run to verify they fail**

Run: `rebar3 ct --suite=test/instrument_meter_SUITE --case=row_store_single_entry_test`
Expected: FAIL — on master the lookup finds the eagerly-registered base `#metric` (not `#otel_rows`), and `collect_instrument/1` is undefined.

- [ ] **Step 2.3: Rewrite `src/instrument_meter.erl`**

**(a) Exports.** In the Utility export list (`:56-63`), add `collect_instrument/1` (it is invoked as an MFA from `instrument_registry:collect_all/0`, so it must be exported):

```erlang
%% Utility
-export([
  get_instrument/1,
  list_instruments/0,
  collect_observables/0,
  collect_instrument/1,
  unregister_instrument/1,
  unregister_all_instruments/0
]).
```

**(b) Creation.** Replace `create_instrument/4`'s binary clause body (`:363-387`) — only the `undefined` branch changes:

```erlang
create_instrument(#meter{} = Meter, Name, Kind, Opts) when is_binary(Name), is_map(Opts) ->
  Description = maps:get(description, Opts, undefined),
  Unit = maps:get(unit, Opts, undefined),
  Temporality = maps:get(temporality, Opts, cumulative),

  case get_instrument(Name) of
    undefined ->
      RegName = {otel, Name},
      ok = register_container(RegName, Name, Kind, Opts),
      Instrument = #otel_instrument{
        name = Name,
        kind = Kind,
        description = Description,
        unit = Unit,
        meter = Meter,
        handle = RegName,
        temporality = Temporality
      },
      register_instrument(Name, Instrument),
      Instrument;
    Existing ->
      Existing
  end.
```

Replace `create_observable_instrument/4`'s binary clause `undefined` branch (`:391-414`) the same way:

```erlang
  case get_instrument(Name) of
    undefined ->
      RegName = {otel, Name},
      ok = register_container(RegName, Name, Kind, #{}),
      Instrument = #otel_instrument{
        name = Name,
        kind = Kind,
        description = undefined,
        unit = undefined,
        meter = Meter,
        handle = {observable, RegName, Callback}
      },
      register_instrument(Name, Instrument),
      Instrument;
    Existing ->
      Existing
  end.
```

**Delete** `create_underlying_metric/3` (all 4 clauses, `:416-467`) and `create_observable_underlying/2` (`:474-479`) and `storage_type/1` (`:485-487`). **Add** in their place:

```erlang
%% Register the instrument's single registry entry: an #otel_rows container
%% under the real (tagged) name. No storage is allocated here — rows are
%% minted on first write per attribute set. An empty container collects as
%% data == [] and the formatters emit nothing, so a created-but-never-written
%% instrument produces no output (and no phantom zero).
register_container(RegName, Name, Kind, Opts) ->
  Boundaries =
    case Kind of
      histogram ->
        case find_view_boundaries(Name) of
          undefined -> maps:get(boundaries, Opts, default_boundaries());
          B -> B
        end;
      _ ->
        undefined
    end,
  Container = #metric{
    name = RegName,
    handle = #otel_rows{
      kind = Kind,
      help = maps:get(description, Opts, <<>>),
      start_time = erlang:system_time(nanosecond),
      boundaries = Boundaries,
      union = [],
      rows = #{}
    },
    collect = {?MODULE, collect_instrument, [RegName]}
  },
  ok = instrument_metric:register(Container).
```

**(c) Writes.** Replace the `add/3`, `record/3`, `set/3` API clauses (`:194-223`):

```erlang
%% @doc Adds a value to a Counter or UpDownCounter with attributes.
-spec add(instrument(), number(), map()) -> ok | {error, term()}.
add(#otel_instrument{kind = counter, handle = RegName}, Value, Attrs)
    when is_number(Value), Value >= 0, is_map(Attrs) ->
  do_write(RegName, Attrs, fun(Row) -> instrument_counter:inc_counter(Row, Value) end);
add(#otel_instrument{kind = up_down_counter, handle = RegName}, Value, Attrs)
    when is_number(Value), Value >= 0, is_map(Attrs) ->
  do_write(RegName, Attrs, fun(Row) -> instrument_gauge:inc_gauge(Row, Value) end);
add(#otel_instrument{kind = up_down_counter, handle = RegName}, Value, Attrs)
    when is_number(Value), Value < 0, is_map(Attrs) ->
  do_write(RegName, Attrs, fun(Row) -> instrument_gauge:dec_gauge(Row, -Value) end);
add(_, _, _) ->
  {error, invalid_operation}.
```

```erlang
%% @doc Records a value in a Histogram with attributes.
-spec record(instrument(), number(), map()) -> ok | {error, term()}.
record(#otel_instrument{kind = histogram, handle = RegName}, Value, Attrs)
    when is_number(Value), is_map(Attrs) ->
  do_write(RegName, Attrs, fun(Row) -> instrument_histogram:observe_histogram(Row, Value) end);
record(_, _, _) ->
  {error, invalid_operation}.
```

```erlang
%% @doc Sets a value on a Gauge with attributes.
-spec set(instrument(), number(), map()) -> ok | {error, term()}.
set(#otel_instrument{kind = gauge, handle = RegName}, Value, Attrs)
    when is_number(Value), is_map(Attrs) ->
  do_write(RegName, Attrs, fun(Row) -> instrument_gauge:set_gauge(Row, Value) end);
set(_, _, _) ->
  {error, invalid_operation}.
```

**Delete** the whole `do_add`/`do_record`/`do_set` clause families (`:498-557`). **Add** the unified path:

```erlang
%% ============================================================================
%% Row-store write path
%% ============================================================================

%% Fast path: canonicalize → one persistent_term get → one NIF op.
%% Identical for labeled and unlabeled writes (Canon = {[], []} for no attrs).
do_write(RegName, Attrs, WriteFun) ->
  Canon = attrs_to_labels(Attrs),
  case persistent_term:get({instrument_label, RegName, Canon}, undefined) of
    #metric{} = Row -> WriteFun(Row);
    undefined -> slow_write(RegName, Canon, WriteFun)
  end.

%% Slow path — once per distinct attribute set over the instrument's life.
%% Pre-checks (membership, exact cardinality) read the parent record outside
%% the gen_server; only genuinely-new rows pay the serialized call.
slow_write(RegName, Canon, WriteFun) ->
  case instrument_registry:lookup(RegName) of
    #metric{handle = #otel_rows{rows = Rows}} ->
      case maps:find(Canon, Rows) of
        {ok, Row} ->
          %% Cache raced or was wiped — re-cache (only-if-absent) and write.
          ok = recache_row(RegName, Canon, Row),
          WriteFun(Row);
        error ->
          Limit = instrument_config:get_metric_cardinality_limit(),
          case map_size(Rows) >= Limit of
            true -> overflow_write(RegName, WriteFun);
            false -> create_and_write(RegName, Canon, WriteFun)
          end
      end;
    _ ->
      {error, not_found}
  end.

create_and_write(RegName, Canon, WriteFun) ->
  case instrument_registry:create_otel_row(RegName, Canon) of
    {ok, Row} -> WriteFun(Row);
    {error, _} = Err -> Err
  end.

%% Writes past the cardinality limit land on the spec's overflow series —
%% an ordinary row keyed ?OTEL_OVERFLOW_CANON. Once it exists this path is
%% pt gets + NIF only; the gen_server is involved just to create it.
overflow_write(RegName, WriteFun) ->
  ok = instrument_registry:note_cardinality_dropped(RegName),
  Canon = ?OTEL_OVERFLOW_CANON,
  case persistent_term:get({instrument_label, RegName, Canon}, undefined) of
    #metric{} = Row -> WriteFun(Row);
    undefined -> create_and_write(RegName, Canon, WriteFun)
  end.

%% Only-if-absent: putting over an existing pt key is a replacing put,
%% which schedules a needless literal-GC sweep.
recache_row(RegName, Canon, Row) ->
  case instrument_registry:lookup_label(RegName, Canon) of
    undefined -> instrument_registry:cache_label(RegName, Canon, Row);
    _ -> ok
  end.
```

**(d) Collect.** Add (near the write path):

```erlang
%% ============================================================================
%% Collection
%% ============================================================================

%% @doc Collect callback for one meter instrument (invoked via MFA from
%% instrument_registry:collect_all/0). One pt get + one NIF read per row;
%% the emitted map is final — name, wire type, stored union, rows.
-spec collect_instrument(term()) -> map().
collect_instrument(RegName) ->
  #metric{handle = #otel_rows{kind = Kind, help = Help, start_time = StartTime,
                              union = Union, rows = Rows}} =
    instrument_registry:lookup(RegName),
  Data = maps:fold(
    fun({Names, Values}, Row, Acc) ->
      [{Names, Values, read_row(Kind, Row)} | Acc]
    end, [], Rows),
  #{name => RegName,
    help => Help,
    type => wire_type(Kind),
    start_time => StartTime,
    labels => Union,
    data => Data}.

%% Counter rows are counter storage; histogram rows histogram storage;
%% everything else (gauge / up_down_counter / observable_*) is gauge storage.
read_row(counter, Row) -> instrument_counter:get_counter(Row);
read_row(histogram, Row) -> instrument_histogram:get_histogram(Row);
read_row(_, Row) -> instrument_gauge:get_gauge(Row).

%% observable_counter is gauge-shaped in storage (callbacks report absolute
%% values, set semantics) but renders as a counter.
wire_type(counter) -> counter;
wire_type(observable_counter) -> counter;
wire_type(histogram) -> histogram;
wire_type(_) -> gauge.
```

**(e) Observables.** Replace `collect_observable/1` + `store_observable_observation/5` (`:256-296`):

```erlang
collect_observable(#otel_instrument{kind = Kind, handle = {observable, RegName, Callback}})
    when Kind =:= observable_counter;
         Kind =:= observable_gauge;
         Kind =:= observable_up_down_counter ->
  try
    case erlang:fun_info(Callback, arity) of
      {arity, 0} ->
        %% Legacy 0-arity callback — one unlabeled observation.
        observe(RegName, Callback(), #{});
      {arity, 1} ->
        %% Observer-pattern callback — multiple observations with attributes.
        Observer = fun(Value, Attrs) -> observe(RegName, Value, Attrs) end,
        Callback(Observer)
    end
  catch
    _:_ -> ok
  end;
collect_observable(_) ->
  ok.

%% Every observable observation is a standard fast-path write with set
%% semantics (callbacks report absolute values).
observe(RegName, Value, Attrs) when is_number(Value), is_map(Attrs) ->
  do_write(RegName, Attrs, fun(Row) -> instrument_gauge:set_gauge(Row, Value) end);
observe(_, _, _) ->
  ok.
```

**(f) Unregister.** Replace `unregister_instrument/1`'s binary clause (`:304-323`), and **delete** `get_internal_metric_name/1` (`:325-328`), `unregister_associated_vec_metrics/1` (`:330-339`), `unregister_underlying_metric/1` (`:350-355`):

```erlang
unregister_instrument(Name) when is_binary(Name) ->
  Key = {otel_instrument, Name},
  case persistent_term:get(Key, undefined) of
    undefined ->
      {error, not_found};
    #otel_instrument{handle = Handle} ->
      %% The registry's do_unreg_metric drives all storage cleanup from the
      %% container record: row caches, exemplar reservoirs, overflow key.
      _ = instrument_metric:unregister(reg_name(Handle)),
      _ = persistent_term:erase(Key),
      Names = persistent_term:get(otel_instruments, []),
      persistent_term:put(otel_instruments, lists:delete(Name, Names)),
      ok
  end.

reg_name({observable, RegName, _Callback}) -> RegName;
reg_name(RegName) -> RegName.
```

**(g) Delete the vec-borrowing machinery** — these are now caller-less and `warnings_as_errors` would flag them: `ensure_vec_metric/4` (`:579-640`), `track_vec_metric/2` (`:643-649`), `make_vec_name/2` (`:652-660`), `label_suffix/1` (`:663-671`). Keep `attrs_to_labels/1`, `to_label_value/1`, `find_view_boundaries/1`, `default_boundaries/0`, `register_instrument/2` — all still used.

- [ ] **Step 2.4: Prometheus — pad to the stored union, skip empty families**

In `src/instrument_prometheus.erl`:

Add a first clause to `format_metric/1` (`:24-32`):

```erlang
format_metric(#{data := []}) ->
  %% A family with no rows (e.g. a created-but-never-written meter
  %% instrument) emits nothing — matching OTel SDKs' no-data-point behavior.
  [];
```

Replace the labeled `format_counter/1` clause (`:45-53`) — rows may carry fewer keys than the family union, so pad per row instead of zipping the family labels blindly:

```erlang
%% Counter vec with labels. Rows carry their own label names; absent union
%% keys render as empty strings (heterogeneous OTel attribute sets).
format_counter(#{name := Name, help := Help, labels := Labels, data := Data}) ->
  NameBin = format_name(Name),
  TotalName = <<NameBin/binary, "_total">>,
  [
    <<"# HELP ">>, TotalName, <<" ">>, escape_help(Help), <<"\n">>,
    <<"# TYPE ">>, TotalName, <<" counter\n">>,
    [format_labeled_value(TotalName, Labels, pad_row(Labels, RowNames, RowVals), Val)
     || {RowNames, RowVals, Val} <- Data]
  ].
```

Replace the labeled `format_gauge/1` clause (`:65-72`) the same way:

```erlang
%% Gauge vec with labels
format_gauge(#{name := Name, help := Help, labels := Labels, data := Data}) ->
  NameBin = format_name(Name),
  [
    <<"# HELP ">>, NameBin, <<" ">>, escape_help(Help), <<"\n">>,
    <<"# TYPE ">>, NameBin, <<" gauge\n">>,
    [format_labeled_value(NameBin, Labels, pad_row(Labels, RowNames, RowVals), Val)
     || {RowNames, RowVals, Val} <- Data]
  ].
```

Replace the labeled `format_histogram/1` clause (`:87-94`) the same way:

```erlang
%% Histogram vec with labels
format_histogram(#{name := Name, help := Help, labels := Labels, data := Data}) ->
  NameBin = format_name(Name),
  [
    <<"# HELP ">>, NameBin, <<" ">>, escape_help(Help), <<"\n">>,
    <<"# TYPE ">>, NameBin, <<" histogram\n">>,
    [format_histogram_data(NameBin, Labels, pad_row(Labels, RowNames, RowVals), Val)
     || {RowNames, RowVals, Val} <- Data]
  ].
```

Add `pad_row/3` next to `format_labeled_value/4` (`:133`):

```erlang
%% Pad one row's values to the family's label set, empty-filling absent
%% keys. First clause: the common case (row names == family labels — every
%% standalone vec, and meter instruments with one key-set) passes through.
pad_row(Union, Union, Vals) ->
  Vals;
pad_row(Union, RowNames, RowVals) ->
  RowMap = maps:from_list(lists:zip(RowNames, RowVals)),
  [maps:get(L, RowMap, <<"">>) || L <- Union].
```

Delete the `format_name({otel_vec, Name})` clause (`:153`) — the tag no longer exists.

- [ ] **Step 2.5: Exporter — per-row attributes, start_time, empty skip**

In `src/exporters/instrument_metrics_exporter.erl`:

Add a first `convert_metric/2` clause (before the scalar counter clause, `:272`):

```erlang
convert_metric(#{data := []}, _Timestamp) ->
  %% No rows → no data points → no metric (OTel no-data behavior).
  undefined;
```

Replace the labeled counter clause (`:287-298`) — per-row label names, stream start_time:

```erlang
convert_metric(#{type := counter, name := Name, help := Help, labels := _Labels, data := Data} = Metric, Timestamp) ->
  StartTime = maps:get(start_time, Metric, undefined),
  #{
    name => to_binary(Name),
    description => extract_help(Help),
    unit => get_instrument_unit(Name),
    type => counter,
    data_points => [data_point(make_attributes(RowNames, LabelVals), Val, Timestamp, StartTime)
                    || {RowNames, LabelVals, Val} <- Data]
  };
```

Replace the labeled gauge clause (`:313-324`) — per-row label names (gauges carry no start_time):

```erlang
convert_metric(#{type := gauge, name := Name, help := Help, labels := _Labels, data := Data}, Timestamp) ->
  #{
    name => to_binary(Name),
    description => extract_help(Help),
    unit => get_instrument_unit(Name),
    type => gauge,
    data_points => [#{
      attributes => make_attributes(RowNames, LabelVals),
      value => Val,
      timestamp => Timestamp
    } || {RowNames, LabelVals, Val} <- Data]
  };
```

Replace the labeled histogram clause (`:345-361`):

```erlang
convert_metric(#{type := histogram, name := Name, help := Help, labels := _Labels, data := Data} = Metric, Timestamp) ->
  StartTime = maps:get(start_time, Metric, undefined),
  #{
    name => to_binary(Name),
    description => extract_help(Help),
    unit => get_instrument_unit(Name),
    type => histogram,
    data_points => [data_point(make_attributes(RowNames, LabelVals),
                               #{
                                 count => maps:get(count, Val),
                                 sum => maps:get(sum, Val),
                                 buckets => [#{bound => maps:get(upper_bound, B),
                                               count => maps:get(cumulative_count, B)}
                                             || B <- maps:get(buckets, Val)]
                               },
                               Timestamp, StartTime)
                    || {RowNames, LabelVals, Val} <- Data]
  };
```

Add the helper next to `make_attributes/2` (`:375`):

```erlang
%% Build one data point; include start_time only when known (standalone
%% vecs carry none — same omission as the scalar gauge clause).
data_point(Attrs, Val, Timestamp, undefined) ->
  #{attributes => Attrs, value => Val, timestamp => Timestamp};
data_point(Attrs, Val, Timestamp, StartTime) ->
  #{attributes => Attrs, value => Val, timestamp => Timestamp, start_time => StartTime}.
```

Delete the `{otel_vec, Name}` clauses from `get_instrument_unit/1` (`:383-385`) and `to_binary/1` (`:395`).

In `src/instrument_test.erl`, delete the `name_matches({otel_vec, Name}, SearchName)` clause (`:596-597`).

- [ ] **Step 2.6: Run the new tests**

Run: `rebar3 ct --suite=test/instrument_meter_SUITE --case=row_store_single_entry_test,unlabeled_write_is_a_row_test,never_written_emits_nothing_test,attributed_only_no_phantom_test`
Expected: PASS

- [ ] **Step 2.7: Update the suites that assert the old (mangled / multi-entry) behavior**

Run the four affected suites first to see exactly what fails:

Run: `rebar3 ct --suite=test/instrument_meter_SUITE,test/instrument_observable_SUITE,test/instrument_metrics_exporter_SUITE,test/instrument_prometheus_SUITE`

Apply these updates — they replace assertions of master's buggy output (derived names, phantom zero) with the new export contract:

**`test/instrument_meter_SUITE.erl`** — in `add_negative_to_labeled_up_down_counter/1`, the two rendered-name assertions drop the derived `_a` suffix:

```erlang
  ?assertNotEqual(nomatch,
                  binary:match(Output1, <<"signed_active{a=\"x\"} 3.0">>)),
```
and
```erlang
  ?assertNotEqual(nomatch,
                  binary:match(Output2, <<"signed_active{a=\"y\"} -1.0">>)),
```

**`test/instrument_observable_SUITE.erl`** — in `observable_counter_renders_as_counter/1` replace the three assertions:

```erlang
  %% The labeled observation renders under the registered instrument name
  %% (attributes as labels), not a derived `_region` name. format_counter
  %% adds the `_total` suffix.
  ?assertNotEqual(nomatch,
                  binary:match(Output, <<"# TYPE obs_counter_labeled_total counter">>)),
  ?assertNotEqual(nomatch,
                  binary:match(Output,
                               <<"obs_counter_labeled_total{region=\"us-east\"} 7.0">>)),

  %% Must NOT be misrendered as gauge.
  ?assertEqual(nomatch,
               binary:match(Output, <<"# TYPE obs_counter_labeled gauge">>)),
```

In `observable_counter_with_multiple_label_schemas/1` replace the assertion block:

```erlang
  %% Both label-key schemas fold into one stream under the registered name,
  %% rendered as a counter. The label columns are the union (`a`, `b`) with
  %% empty-string fill for the schema that lacks `b`.
  ?assertNotEqual(nomatch,
                  binary:match(Output, <<"# TYPE obs_counter_multi_schema_total counter">>)),
  ?assertNotEqual(nomatch,
                  binary:match(Output, <<"obs_counter_multi_schema_total{a=\"x\",b=\"\"} 5.0">>)),
  ?assertNotEqual(nomatch,
                  binary:match(Output,
                               <<"obs_counter_multi_schema_total{a=\"x\",b=\"y\"} 7.0">>)),
```

**`test/instrument_prometheus_SUITE.erl`** — add `-include_lib("stdlib/include/assert.hrl").` after the ct include; add `otel_attributed_union_test` to the export list and to the group list that already contains `format_name_tuple_test` (this suite uses `groups/0`, not a flat `all/0`); append:

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

**`test/instrument_metrics_exporter_SUITE.erl`** — add `otel_attributed_single_stream_test`, `no_mangled_otel_series_test`, and `otlp_labeled_start_time_test` to the export list and `all()`; rewrite `metric_name_otel_with_attrs_test/1`'s assertion half:

```erlang
%% Test that OTel meter metrics with attributes export as one stream under
%% the registered name (no derived `_<labels>` series).
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

Rewrite the assertion halves of `metric_attrs_otel_single_test/1` (gauge) and `metric_attrs_otel_multiple_test/1` (histogram) on the same pattern (each begins with `_ = instrument_meter:unregister_all_instruments(),`):

```erlang
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

```erlang
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

Append the three new tests:

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

otlp_labeled_start_time_test(_Config) ->
  %% Attributed counter data points carry the stream's start_time
  %% (previously dropped — only scalar clauses had it).
  _ = instrument_meter:unregister_all_instruments(),
  Meter = instrument_meter:get_meter(<<"st_svc">>),
  C = instrument_meter:create_counter(Meter, <<"st_counter">>, #{}),
  ok = instrument_meter:add(C, 1, #{path => <<"/x">>}),

  [#{data_points := [DP]}] =
    [M || #{name := <<"st_counter">>} = M <- instrument_metrics_exporter:collect()],
  ?assert(is_integer(maps:get(start_time, DP))),
  ok.
```

- [ ] **Step 2.8: Run the four suites until green**

Run: `rebar3 ct --suite=test/instrument_meter_SUITE,test/instrument_observable_SUITE,test/instrument_metrics_exporter_SUITE,test/instrument_prometheus_SUITE`
Expected: PASS. If an existing test fails that this plan did not anticipate, the fix is almost always one of: (a) it asserted a derived name — update to the registered name; (b) it asserted the scalar emit shape (`val := ...`) for a meter instrument — meter entries are now always labeled-shape (`data`, `labels` keys), update the match; (c) it relied on the eager base zero series — assert absence instead. Do NOT change `instrument_vector_SUITE` / `instrument_cardinality_SUITE` / standalone-vec tests — if those fail, the implementation broke the public vec API; fix the implementation.

- [ ] **Step 2.9: Standalone-vec proof suites**

Run: `rebar3 ct --suite=test/instrument_vector_SUITE,test/instrument_cardinality_SUITE,test/instrument_race_SUITE,test/instrument_leaks_SUITE`
Expected: PASS, zero modifications to these files (`git status` must show them untouched).

- [ ] **Step 2.10: Commit**

```bash
git add src/instrument_meter.erl src/instrument_prometheus.erl \
        src/exporters/instrument_metrics_exporter.erl src/instrument_test.erl \
        test/instrument_meter_SUITE.erl test/instrument_observable_SUITE.erl \
        test/instrument_metrics_exporter_SUITE.erl test/instrument_prometheus_SUITE.erl
git commit -m "store meter rows on the instrument so attributed metrics export under their registered name"
```

---

### Task 3: Per-instrument cardinality behavior through the meter API

Task 1 implemented the mechanism; this task locks the user-visible behavior: the limit counts rows per instrument, overflowed writes aggregate into the `otel.metric.overflow` series, accounting APIs report it.

**Files:**
- Test: `test/instrument_meter_SUITE.erl`

- [ ] **Step 3.1: Write the failing test**

Add `meter_cardinality_overflow_test` to `-export` and `all()`, and:

```erlang
meter_cardinality_overflow_test(_Config) ->
  Meter = instrument_meter:get_meter(<<"card_meter">>),
  os:putenv("OTEL_METRIC_CARDINALITY_LIMIT", "2"),
  try
    C = instrument_meter:create_counter(Meter, <<"card_counter">>),
    ok = instrument_meter:add(C, 1, #{id => <<"1">>}),
    ok = instrument_meter:add(C, 1, #{id => <<"2">>}),
    %% Over the limit: both writes aggregate into the overflow series.
    ok = instrument_meter:add(C, 5, #{id => <<"3">>}),
    ok = instrument_meter:add(C, 7, #{id => <<"4">>}),
    %% Existing rows stay writable at the limit.
    ok = instrument_meter:add(C, 1, #{id => <<"1">>}),

    RegName = {otel, <<"card_counter">>},
    Stream = instrument_meter:collect_instrument(RegName),
    Data = maps:get(data, Stream),
    ?assertEqual(3, length(Data)),    %% 2 real rows + 1 overflow row
    ?assert(lists:member({[<<"otel.metric.overflow">>], [<<"true">>], 12.0}, Data)),
    ?assert(lists:member({[id], [<<"1">>], 2.0}, Data)),

    %% Accounting: 3 cached rows (2 real + overflow); 2 dropped writes.
    ?assertEqual(3, instrument_registry:label_count(RegName)),
    ?assertEqual(2, instrument_registry:cardinality_dropped(RegName)),

    %% Prometheus renders the overflow series under the union.
    Text = instrument_prometheus:format(),
    ?assertNotEqual(nomatch,
                    binary:match(Text, <<"otel.metric.overflow=\"true\"">>))
  after
    os:unsetenv("OTEL_METRIC_CARDINALITY_LIMIT")
  end,
  ok.
```

- [ ] **Step 3.2: Run to verify**

Run: `rebar3 ct --suite=test/instrument_meter_SUITE --case=meter_cardinality_overflow_test`
Expected: PASS already if Tasks 1–2 are correct (the mechanism exists; this is a behavioral lock). If it fails, fix the implementation — the most likely gaps: `overflow_write` not counting drops (`note_cardinality_dropped`), or the limit check reading the wrong store.

- [ ] **Step 3.3: Commit**

```bash
git add test/instrument_meter_SUITE.erl
git commit -m "lock per-instrument cardinality and the otel.metric.overflow series as the meter's overflow contract"
```

---

### Task 4: Cleanup and concurrency contracts

**Files:**
- Test: `test/instrument_meter_SUITE.erl`

- [ ] **Step 4.1: Write the failing/locking tests**

Add `unregister_cleans_row_store_test` and `concurrent_first_write_race_test` to `-export` and `all()`, and:

```erlang
unregister_cleans_row_store_test(_Config) ->
  Meter = instrument_meter:get_meter(<<"clean_meter">>),
  C = instrument_meter:create_counter(Meter, <<"clean_counter">>),
  ok = instrument_meter:add(C, 1, #{a => <<"x">>}),
  ok = instrument_meter:add(C, 1),

  RegName = {otel, <<"clean_counter">>},
  CanonA = {[a], [<<"x">>]},
  ?assertMatch(#metric{},
               persistent_term:get({instrument_label, RegName, CanonA}, undefined)),

  ok = instrument_meter:unregister_instrument(<<"clean_counter">>),

  %% Entry, descriptor, and every row cache are gone.
  ?assertEqual(undefined, instrument_registry:lookup(RegName)),
  ?assertEqual(undefined, instrument_meter:get_instrument(<<"clean_counter">>)),
  ?assertEqual(undefined,
               persistent_term:get({instrument_label, RegName, CanonA}, undefined)),
  ?assertEqual(undefined,
               persistent_term:get({instrument_label, RegName, {[], []}}, undefined)),

  %% Re-creating starts from a clean slate.
  C2 = instrument_meter:create_counter(Meter, <<"clean_counter">>),
  ok = instrument_meter:add(C2, 1, #{a => <<"x">>}),
  ?assertMatch(#{data := [{[a], [<<"x">>], 1.0}]},
               instrument_meter:collect_instrument(RegName)),
  ok.

concurrent_first_write_race_test(_Config) ->
  %% N processes racing the FIRST write of the same attribute set must
  %% produce exactly one row and lose no increments.
  Meter = instrument_meter:get_meter(<<"race_meter">>),
  C = instrument_meter:create_counter(Meter, <<"race_counter">>),
  Self = self(),
  N = 50,
  Pids = [spawn_link(fun() ->
            ok = instrument_meter:add(C, 1, #{shard => <<"s1">>}),
            Self ! {done, self()}
          end) || _ <- lists:seq(1, N)],
  [receive {done, P} -> ok after 5000 -> ct:fail(timeout) end || P <- Pids],

  #metric{handle = #otel_rows{rows = Rows}} =
    instrument_registry:lookup({otel, <<"race_counter">>}),
  ?assertEqual(1, map_size(Rows)),
  ?assertMatch(#{data := [{[shard], [<<"s1">>], V}]} when V == N * 1.0,
               instrument_meter:collect_instrument({otel, <<"race_counter">>})),
  ok.
```

Note on the last assertion: `?assertMatch` with a `when` guard on a bound-in-pattern variable is valid (`V` binds in the pattern, the guard compares). If the suite's Erlang version chokes on the guard form, fall back to:

```erlang
  #{data := [{[shard], [<<"s1">>], V}]} =
    instrument_meter:collect_instrument({otel, <<"race_counter">>}),
  ?assertEqual(float(N), V),
```

- [ ] **Step 4.2: Run them**

Run: `rebar3 ct --suite=test/instrument_meter_SUITE --case=unregister_cleans_row_store_test,concurrent_first_write_race_test`
Expected: PASS (mechanisms from Tasks 1–2; this locks them). A failure in the race test means lost increments — check that `create_otel_row` race losers receive the *winner's* row, and that `do_write`'s WriteFun runs on the returned row in every path.

- [ ] **Step 4.3: Full local gate**

Run: `rebar3 ct`
Expected: every suite PASS except possibly `instrument_e2e_SUITE` without Docker (see Context). Then:

Run: `rebar3 dialyzer`
Expected: clean. Likely nits if not: the `#otel_rows` type union in specs, or the now-narrower `add/3` return type — fix specs, not code.

- [ ] **Step 4.4: Commit**

```bash
git add test/instrument_meter_SUITE.erl
git commit -m "lock row-store cleanup and concurrent first-write semantics"
```

---

### Task 5: CHANGELOG

**Files:**
- Modify: `CHANGELOG.md` (insert after the header, before `## [1.1.3]`)

- [ ] **Step 5.1: Add the Unreleased section**

```markdown
## [Unreleased]

### Fixed
- OTel meter instruments written with attributes (`add/3`, `record/3`,
  `set/3`, attributed observable callbacks) now export under their
  registered name as a single stream, with the attributes as data-point
  attributes (OTLP/console) or labels (Prometheus). Previously the data
  was exported under a derived `<name>_<labels>` (counters/gauges) or
  `<name>_vec_<labels>` (histograms) series, while the registered name
  exported a constant zero.
- The constant-zero phantom series is gone: an instrument that has never
  been written exports nothing (matching OTel SDKs); the unlabeled series
  appears on the first unlabeled write.
- OTLP data points of attributed counters and histograms now carry
  `start_time` (previously only unlabeled instruments had it).

### Changed
- The meter stores attributed series in a per-instrument row store instead
  of one fixed-schema vector per attribute key-set. Export-time behavior
  changes: in Prometheus, an instrument written with several attribute
  key-sets renders as one family whose label columns are the union of the
  observed keys, with absent keys as empty strings.
- The metric cardinality limit (`OTEL_METRIC_CARDINALITY_LIMIT`) applies
  per meter instrument (previously per attribute key-set, so one
  instrument could hold a multiple of the limit). Writes past the limit
  aggregate into the OTel-specified `otel.metric.overflow` series
  (previously a per-vector sentinel label set).
- Metric families with no data rows are omitted from both export formats
  (previously an empty family could render `# HELP`/`# TYPE` headers with
  no samples).
- The standalone `instrument_metric:*_vec` API is unchanged.
```

- [ ] **Step 5.2: Final verification + commit**

Run: `rebar3 compile && rebar3 ct --suite=test/instrument_meter_SUITE && git status --short`
Expected: compile clean, suite PASS, only `CHANGELOG.md` modified.

```bash
git add CHANGELOG.md
git commit -m "document the meter row-store export fixes and behavior changes"
```

---

## Done criteria

- `rebar3 ct` green (e2e excepted without Docker) and `rebar3 dialyzer` clean on `meter-row-store`.
- `git diff master --stat` shows NO changes to `src/instrument_vector.erl`, `src/instrument_metric.erl`, or any standalone-vec suite.
- Grep proofs: `grep -rn "otel_vec\|make_vec_name\|otel_instrument_vecs" src/` returns nothing.
- The branch carries the spec+plan docs commit (Task 0) — drop it before opening the PR (`git rebase --onto` or interactive drop, as done for the histogram PR).
