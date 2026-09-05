-module(quasar_postgres_listener_ffi).

-export([start/3, stop/1]).

-include_lib("pog/include/pog_Config.hrl").

start(Config, Channel, Wake) ->
    Owner = self(),
    Listener = spawn(fun() -> init(Owner, Config, Channel, Wake) end),
    receive
        {Listener, started} -> {ok, Listener};
        {Listener, {error, Reason}} -> {error, Reason}
    after 1000 ->
        exit(Listener, kill),
        {error, timeout}
    end.

init(Owner, Config, Channel, Wake) ->
    OwnerMonitor = erlang:monitor(process, Owner),
    case start_notifications(Config) of
        {ok, Notifications} ->
            case pgo_notifications:listen(Notifications, Channel) of
                {ok, Ref} ->
                    Owner ! {self(), started},
                    loop(OwnerMonitor, Notifications, Ref, Channel, Wake);
                {eventually, Ref} ->
                    Owner ! {self(), started},
                    loop(OwnerMonitor, Notifications, Ref, Channel, Wake);
                Error ->
                    ignore_errors(fun() -> gen_statem:stop(Notifications) end),
                    Owner ! {self(), {error, Error}}
            end;
        Error ->
            Owner ! {self(), {error, Error}}
    end.

start_notifications(Config) ->
    #config{
        host = Host,
        port = Port,
        database = Database,
        user = User,
        password = Password,
        ssl = Ssl,
        connection_parameters = ConnectionParameters,
        trace = Trace,
        ip_version = IpVersion,
        rows_as_map = RowsAsMap
    } = Config,
    {SslActivated, SslOptions} = ssl_options(Host, Ssl),
    Options0 = #{
        host => binary_to_list(Host),
        port => Port,
        database => Database,
        user => User,
        ssl => SslActivated,
        ssl_options => SslOptions,
        connection_parameters => ConnectionParameters,
        trace => Trace,
        decode_opts => [{return_rows_as_maps, RowsAsMap}],
        socket_options => case IpVersion of
            ipv4 -> [];
            ipv6 -> [inet6]
        end
    },
    Options = case Password of
        {some, Value} -> maps:put(password, Value, Options0);
        none -> Options0
    end,
    pgo_notifications:start_link(Options).

stop(Pid) ->
    Ref = erlang:monitor(process, Pid),
    Pid ! {stop, self()},
    receive
        {Pid, stopped} -> erlang:demonitor(Ref, [flush]);
        {'DOWN', Ref, process, Pid, _} -> ok
    after 1000 -> ok
    end,
    nil.

loop(OwnerMonitor, Notifications, Ref, Channel, Wake) ->
    receive
        {notification, Notifications, Ref, Channel, Payload} ->
            _ = try Wake(Payload) catch _:_ -> nil end,
            loop(OwnerMonitor, Notifications, Ref, Channel, Wake);
        {notification, _, _, _, _} ->
            loop(OwnerMonitor, Notifications, Ref, Channel, Wake);
        {stop, From} ->
            ignore_errors(fun() -> pgo_notifications:unlisten(Notifications, Ref) end),
            ignore_errors(fun() -> gen_statem:stop(Notifications) end),
            From ! {self(), stopped};
        {'DOWN', OwnerMonitor, process, _, _} ->
            ignore_errors(fun() -> gen_statem:stop(Notifications) end)
    end.

ignore_errors(Run) ->
    try Run(), nil catch _:_ -> nil end.

ssl_options(_Host, ssl_disabled) ->
    {false, []};
ssl_options(_Host, ssl_unverified) ->
    {true, [{verify, verify_none}]};
ssl_options(Host, ssl_verified) ->
    {true, [
        {verify, verify_peer},
        {cacerts, public_key:cacerts_get()},
        {server_name_indication, binary_to_list(Host)},
        {customize_hostname_check, [
            {match_fun, public_key:pkix_verify_hostname_match_fun(https)}
        ]}
    ]}.
