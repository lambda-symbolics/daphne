# Daphne

Common Lisp Debug Adapter Protocol sessions: framing, correlation, bounded
requests and events, semantic operations, and adapter process ownership.
Dependencies: `argo`, `babel`, `bordeaux-threads`, `uiop`. License: ISC.
The provided process backend requires SBCL. Implement the `transport` protocol
for another process manager, a socket, or an existing connection.

## Values and ownership

Use string-keyed hash tables for JSON objects, vectors for arrays, `t` for
true, `(argo:json-false)` for false, and `nil` for null. Build objects with
`argo:json-object`. Keep submitted arguments immutable until the request returns.
Semantic requests return two values: response body and complete response object.
Events and responses are ordinary decoded JSON values.

A session exclusively owns its transport. Call `session-close` in
`unwind-protect`, even after failure. Closing is idempotent and waits for cleanup.
The process backend kills/reaps the adapter and closes its pipes. It drains
stderr separately from DAP stdout. Debuggee termination is requested through
DAP rather than by killing unrelated processes.

## Example

```lisp
(multiple-value-bind (session transport)
    (daphne:start-adapter "/path/to/adapter" '("--stdio"))
  (declare (ignore transport))
  (unwind-protect
       (progn
         (daphne:session-initialize session)
         (daphne:session-start
          session :launch (argo:json-object "program" "/path/to/program" "stopOnEntry" t)
          :configure
          (lambda (session)
            (daphne:session-set-breakpoints
             session (argo:json-object "path" "/path/to/source")
             (vector (argo:json-object "line" 12)))))
         (daphne:session-wait-event session "stopped" :timeout 30)
         (daphne:session-threads session)
         (daphne:session-stack-trace session 1 :levels 20)
         (daphne:session-scopes session 2)
         (daphne:session-variables session 4 :count 50)
         (daphne:session-evaluate session "answer" :frame-id 2)
         (daphne:session-continue session 1)
         (daphne:session-terminate session :disconnect t))
    (daphne:session-close session)))
```

Obtain thread/frame/variable references from actual responses; the example IDs
are illustrative. Launch/attach arguments depend on the adapter. Evaluation
can mutate the debuggee.

## Public API

All synchronous request/event-wait functions accept `:timeout` (seconds, default
10, positive and at most 86400) and `:cancel-p` (zero-argument predicate).
Timeout/cancellation closes the connection and fails pending requests, including
when the adapter stops reading stdin. Adapter rejection signals
`dap-request-error` but permits subsequent requests. Caller callbacks must be
bounded. Transport cleanup must also be bounded and interrupt blocked I/O.

- `(start-adapter program arguments &key directory environment max-header
  max-body stderr-limit max-events max-pending)` returns session and
  `process-transport`. Arguments are literal strings; no shell is involved.
  A non-NIL list of `NAME=VALUE` strings in `environment` replaces the inherited
  environment. `adapter-process` returns the SBCL process. `adapter-stderr`
  returns a fresh octet vector containing the first retained stderr octets
  (default limit 65536); subsequent stderr is drained without retention.
- `(make-session transport &key max-events max-pending)` starts its reader.
  Defaults are 1024 events and 64 pending requests. Event overflow fails the
  connection; pending-request admission rejects only the new request.
- `(session-initialize session &key arguments timeout cancel-p)` initializes
  once and returns capabilities. Default arguments specify one-based indices
  and path-style sources. Inspect `session-capabilities` for optional features.
- `(session-start session mode arguments &key configure timeout cancel-p)`
  accepts `:launch` or `:attach` after initialization. It waits for `initialized`,
  calls `configure` with the session in the waiting caller thread, and sends
  `configurationDone` when supported. Delayed launch/attach responses are
  handled; unrelated queued events are preserved. Bound the configure callback
  yourself; nested semantic requests have their own deadlines/cancellation.
- `(session-configuration-done session &key timeout cancel-p)` sends this request
  when supported. Normally use `session-start` for configuration sequencing.
- `(session-set-breakpoints session source breakpoints &key timeout cancel-p)`
  replaces the source's breakpoint set. Pass a source object and breakpoint
  vector, including `#()` to clear the set.
- `(session-continue session thread-id &key timeout cancel-p)` continues a stopped
  session; `(session-pause session thread-id &key timeout cancel-p)` pauses a
  running session.
- `(session-step session thread-id kind &key timeout cancel-p)` accepts `:in`,
  `:over`, or `:out` in a stopped session.
- `(session-threads session &key timeout cancel-p)` returns threads in running or
  stopped sessions.
- `(session-stack-trace session thread-id &key start-frame levels timeout cancel-p)`
  defaults to start 0, levels 100.
- `(session-scopes session frame-id &key timeout cancel-p)` returns frame scopes.
- `(session-variables session reference &key start count timeout cancel-p)`
  defaults to start 0, count 100. Stack/variable counts must be 1..10000.
  Stack, scope and variable helpers require a stopped session.
- `(session-evaluate session expression &key frame-id context timeout cancel-p)`
  takes string expression/context (`"repl"` by default), in running/stopped state.
- `(session-terminate session &key disconnect terminate-debuggee timeout cancel-p)`
  sends `terminate`, or `disconnect` for `:disconnect t`, then always closes.
  `terminate-debuggee` controls the disconnect argument (default t).
- `(session-request session command arguments &key timeout cancel-p on-event event-name)`
  is the extension boundary: arbitrary command and JSON object arguments,
  concurrent/out-of-order response correlation, body and complete response.
  `on-event` consumes queued events in the caller thread and may make nested
  requests; `event-name` limits consumption to one event kind. Use this boundary
  for adapter extensions or finer per-thread lifecycle logic.
- `(session-events session &key name)` drains events in arrival order, optionally
  only the named kind. `(session-wait-event session name &key timeout cancel-p)`
  removes the first matching event and preserves unrelated events.
- `(session-close session &optional failure)` closes and cleans up.
  `session-failure` returns the first connection failure.

`session-state` reports `:new`, `:initializing`, `:initialized`, `:configuring`,
`:running`, `:stopped`, `:terminated`, `:closed` or `:failed`. Running/stopped
reflect received events at connection level, not per-thread state. Terminated
sessions permit explicit disconnect/cleanup. Invalid helper state signals
`dap-state-error` before sending.

## Transport and framing

Subclass `transport`, implementing `transport-read` (decoded object or NIL at
EOF), `transport-write` (message object), and `transport-close` (idempotent,
bounded, unblocking both directions). Writes are serialized. A reader thread
receives events/responses; request writes run in supervised threads so deadlines
cover blocked writes. Reverse adapter requests receive correlated unsupported
responses; initialize advertises no reverse-request capability.

`(make-stream-transport input output &key max-header max-body)` owns binary
streams. For pipe/socket use, specialize close to interrupt blocked I/O before
closing streams. The provided process backend does this.

`(read-frame binary-stream &key max-header max-body)` returns one JSON object,
or NIL only at clean EOF before any header byte. It rejects duplicate/missing/
invalid Content-Length, truncated frames, non-CRLF headers, invalid UTF-8/JSON,
and non-object bodies. Unknown headers are accepted. Lengths are UTF-8 octets.
`(write-frame binary-stream message &key max-body)` writes and flushes framing.
Defaults: 8192 header octets and 16 MiB body. Encoding uses argo JSON limits.

## Conditions

All failures inherit `dap-error`, with `dap-error-message` and optional
`dap-error-cause`: `dap-protocol-error`, `dap-transport-error`, `dap-state-error`,
`dap-limit-error`, `dap-timeout`, `dap-cancelled`, and `dap-request-error`.
`dap-request-error-response` returns the rejected response. Protocol/correlation
failures close the connection.

## Tests

Register the checkout with ASDF and run `(asdf:test-system "daphne")`.
Tests spawn fresh SBCL processes running `fixture.lisp`, an actual DAP adapter
with delayed configuration, semantic responses, reordered replies, stderr
pressure, malformed frames/correlation and stalled operations. Set
`DAPHNE_TEST_SETUP` to the absolute dependency setup file (Qlot or Quicklisp)
that fixture subprocesses should load.

```sh
DAPHNE_TEST_SETUP="$PWD/.qlot/setup.lisp" sbcl --noinform --non-interactive \
  --load .qlot/setup.lisp \
  --eval '(asdf:load-asd (truename ".cache/daphne/daphne.asd"))' \
  --eval '(asdf:test-system "daphne")'
```
