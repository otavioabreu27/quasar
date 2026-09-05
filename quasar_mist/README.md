# quasar_mist

`quasar_mist` executes ordinary Mist request handlers inside bounded Quasar
local pools.

```gleam
import mist
import quasar_jobs as quasar
import quasar_mist

let assert Ok(runtime) =
  quasar.new()
  |> quasar.local_pool(
    name: "http",
    workers: 100,
    prefetch: 1,
    buffer_capacity: 500,
  )
  |> quasar.start

let handler =
  router.handle
  |> quasar_mist.manage(
    using: runtime,
    pool: "http",
    timeout: 5_000,
  )

handler
|> mist.new
|> mist.port(8000)
|> mist.start
```

Wisp composes without a second executor:

```gleam
router.handle
|> wisp_mist.handler(secret_key)
|> quasar_mist.manage(using: runtime, pool: "http", timeout: 5_000)
|> mist.new
|> mist.port(8000)
|> mist.start
```

Mist owns the connection, TLS, parsing, HTTP/1.1, HTTP/2, and transmission of
the response bytes. Quasar owns execution until the handler returns a response;
that does not mean all bytes have reached the client.

The managed MVP supports ordinary fixed responses. Route WebSockets, SSE,
request/response streaming, chunked bodies, and handlers that transfer
connection ownership around the wrapper:

```gleam
case request.path_segments(request) {
  ["ws"] -> websocket_handler(request)
  _ -> managed_handler(request)
}
```

Mist does not currently expose a public disconnect signal to this synchronous
wrapper. A timed-out handler may continue and its late response is ignored.
Requests are never persisted or retried automatically.
