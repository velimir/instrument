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
