%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is Flukso.
%%
%% The Initial Developer of the Original Code is bart@flukso.net.
%% Copyright (c) 2014 bart@flukso.net  All rights reserved.
%%

-module(rabbit_mqtt_flukso).

-export([pub_parse/2]).

-include("rabbit_mqtt_frame.hrl").
-include("rabbit_mqtt.hrl").

-define(TMPO_ROOT_DIR, <<"/var/run/flukso/tmpo/sensor/">>).

pub_parse(Topic, Payload) -> 
    tmpo_sink(re:split(Topic, "/"), Payload).

tmpo_sink([<<"">>, <<"sensor">>, Sid, <<"tmpo">>, Rid, Lvl, Cid, Ext], Payload) ->
    Path = tmpo_path(Sid, Rid, Lvl, Cid, Ext),
    filelib:ensure_dir(Path),
    {ok, Fd} = file:open(Path, [write]),
    ok = file:write(Fd, Payload),
    ok = file:datasync(Fd),
    ok = file:close(Fd),
    LvlInt = list_to_integer(binary_to_list(Lvl)),
    CidInt = list_to_integer(binary_to_list(Cid)),
    tmpo_clean(Sid, Rid, LvlInt, CidInt, Ext),
    {ok, tmpo_file_sunk};
tmpo_sink(_TopicList, _Payload) ->
    {ok, no_tmpo_topic_match}.

tmpo_clean(Sid, Rid, LvlInt, CidInt, Ext) when LvlInt > 8 ->
    Children = tmpo_children(LvlInt, CidInt),
    ChildLvl = integer_to_list(LvlInt - 4),
    [file:delete(tmpo_path(Sid, Rid, ChildLvl, Child, Ext)) || Child <- Children];
tmpo_clean(_, _, _, _, _) ->
    no_cleaning_up_to_do.

tmpo_children(LvlInt, CidInt) ->
    Delta = trunc(math:pow(2, LvlInt - 4)),
    [integer_to_list(CidInt + Pos * Delta) || Pos <- lists:seq(0, 15)].

tmpo_path(Sid, Rid, Lvl, Cid, Ext) ->
    iolist_to_binary([?TMPO_ROOT_DIR, Sid, "/", Rid, "/", Lvl, "/", Cid, ".", Ext]).

