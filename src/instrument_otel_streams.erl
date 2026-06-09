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
    help => first_non_empty([maps:get(help, M, <<>>) || M <- Ms]),
    labels => Union,
    data => Rows}.

%% Only the base entry carries the instrument's description; vec entries are
%% created with empty help. collect_all/0 order is unspecified, so the base may
%% not be first in the group. Pick the first non-empty help so the merged
%% stream keeps the description regardless of order.
first_non_empty([H | _]) when H =/= <<>> -> H;
first_non_empty([_ | Rest]) -> first_non_empty(Rest);
first_non_empty([]) -> <<>>.

%% Normalize one raw entry into a list of {LabelNames, LabelValues, Value} rows.
rows(#{data := Data}) ->
  Data;
rows(#{type := histogram, count := Count, sum := Sum, buckets := Buckets}) ->
  [{[], [], #{count => Count, sum => Sum, buckets => Buckets}}];
rows(#{val := Val}) ->
  [{[], [], Val}].
