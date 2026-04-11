# WendaoArrow

WendaoArrow is the Julia transport interface package for Wendao's Arrow-native
plugin boundary. The local package runtime is now Flight-only. WendaoArrow
ships package-local `Arrow.Flight.Service` composition plus an optional
`gRPCServer` bridge; it no longer exposes local HTTP or IPC server helpers.

The first analyzer package now lives alongside it at `.data/WendaoAnalyzer`.

In this workspace, `Arrow` stays pinned to the upstream `arrow-julia` main
revision, while `ArrowTypes` resolves from the sibling checkout under
`.data/arrow-julia/src/ArrowTypes` to avoid local Julia `Pkg` subdirectory
resolution failures during Flight development.

## What This Package Owns

WendaoArrow is not the analyzer itself. It owns the Julia-side transport and
contract layer that future analyzer packages can reuse.

- The Rust gateway and future host runtime own request shaping, transport
  selection, timeout policy, fallback, and response validation.
- WendaoArrow owns Arrow schema/metadata handling, package-local Flight
  service composition, and the shared request/response contract helpers.
- Julia analyzer packages such as `WendaoAnalyzer` implement domain logic on
  top of that interface.

The Rust gateway remains in
`packages/rust/crates/xiuxian-wendao/src/gateway/`.

## Layout

- `docs/`: package docs
- `src/`: WendaoArrow Julia package
- `test/`: package-local and cross-process Flight tests
- `examples/`: Flight startup examples
- `scripts/`: helper scripts for local Flight runs

## Documentation

Primary docs live under:

- `docs/01_core/wendao_arrow/SPEC.md`
- `docs/01_core/wendao_arrow/VECTOR_SUBSYSTEM.md`
- `docs/01_core/wendao_arrow/architecture/transport-profiles.md`
- `docs/01_core/wendao_arrow/architecture/arrow-schema-contract.md`
- `docs/01_core/wendao_arrow/roadmap.md`

## Transport Direction

- Preferred and packaged transport profile: `Arrow Flight`
- Local service composition:
  `build_flight_service` / `build_stream_flight_service`
- Optional network listener surface when `gRPCServer` is present:
  `flight_server` / `serve_flight` / `serve_stream_flight`
- Cross-profile contract invariant:
  `wendao.schema_version = v1`

WendaoArrow now assumes that any non-Flight fallback belongs to the host
runtime, not to this Julia package.

## Quick Start

Start the stream-first scoring Flight server:

```bash
.data/WendaoArrow.jl/scripts/run_stream_scoring_flight_server.sh --host 127.0.0.1 --port 18815
```

Start the metadata-aware Flight server:

```bash
.data/WendaoArrow.jl/scripts/run_stream_metadata_flight_server.sh --host 127.0.0.1 --port 18815
```

Start the schema-metadata preservation Flight server:

```bash
.data/WendaoArrow.jl/scripts/run_stream_schema_metadata_flight_server.sh --host 127.0.0.1 --port 18815
```

Start the response-`app_metadata` Flight server:

```bash
.data/WendaoArrow.jl/scripts/run_stream_app_metadata_flight_server.sh --host 127.0.0.1 --port 18815
```

Use TOML:

```bash
.data/WendaoArrow.jl/scripts/run_stream_scoring_flight_server.sh --config .data/WendaoArrow.jl/config/wendao_arrow.example.toml
```

Or override with flags:

```bash
.data/WendaoArrow.jl/scripts/run_stream_scoring_flight_server.sh --host 127.0.0.1 --port 18815
```

The launcher scripts prefer a vendored `.cache/vendor/gRPCServer.jl` checkout
when present. If no vendored checkout exists, they next reuse an installed
`gRPCServer` package from the active Julia depot. Set
`WENDAO_FLIGHT_GRPCSERVER_PATH` to an explicit local `gRPCServer.jl` checkout
when you need to override both defaults.

Start an optional Flight listener from Julia when `gRPCServer` is present:

```julia
using WendaoArrow
using gRPCServer
using Tables

server = WendaoArrow.serve_stream_flight(
    stream -> begin
        for batch in stream
            return Tables.columntable(batch)
        end
        return (doc_id = String[], analyzer_score = Float64[], final_score = Float64[])
    end;
    host = WendaoArrow.DEFAULT_HOST,
    port = WendaoArrow.DEFAULT_FLIGHT_PORT,
    block = false,
)

gRPCServer.stop!(server; force = true)
```

Config precedence is `defaults < TOML < flags`.

## Package Surface

WendaoArrow exposes:

- `schema_metadata(table)` as the stable wrapper over `Arrow.getmetadata(...)`
- `merge_schema_metadata(...)` for shared schema-metadata overlay with
  `wendao.schema_version` ownership kept upstream
- `schema_table(table_like; schema_version = ..., metadata = ..., colmetadata = ...)`
  for transport-ready Arrow table materialization over arbitrary downstream
  row contracts
- `normalize_metadata_values(metadata)` for shared additive request-metadata
  validation
- `normalize_scoring_response(table_like)` for packaged scoring-style response
  normalization while preserving schema and field metadata
- `flight_descriptor(path)` as the packaged default-path helper over upstream
  `Arrow.Flight.pathdescriptor(...)`
- `flight_route_descriptor(route)` for route-string normalization into an
  Arrow Flight path descriptor
- `flight_schema_headers(; schema_version = ..., headers = ...)` for shared
  `x-wendao-schema-version` request-header construction
- `flight_exchange_request(source; descriptor = ..., headers = ...)` for one
  prepared `DoExchange` request wrapper
- `flight_exchange_table(...)` for request-wrapper-driven local or client-side
  `DoExchange` response decoding
- `build_flight_service(processor)` for table-first local Flight `DoExchange`
  services
- `build_stream_flight_service(processor)` for stream-first local Flight
  `DoExchange` services
- `gateway_flight_client()` for the live Wendao gateway Flight client surface
- `gateway_repo_search(...)` for the runtime-owned repo-search Flight route
- `gateway_knowledge_search(...)` for the runtime-owned knowledge-search
  Flight route
- `flight_server(service)` for optional `gRPCServer`-backed listener
  composition
- `serve_flight(processor)` and `serve_stream_flight(processor)` for optional
  Flight listeners when `gRPCServer` is loaded

Downstream Julia packages with custom Arrow row contracts should prefer
`schema_table(...)` over direct `Arrow.write(...)` calls. That keeps
`wendao.schema_version` ownership and schema-metadata merging inside
`WendaoArrow.jl` while leaving route-specific metadata and column choices in
the downstream package.

Downstream packages that need draft or analyzer-specific Flight routes should
also prefer `flight_route_descriptor(...)` and `flight_schema_headers(...)`
instead of rebuilding the shared `x-wendao-schema-version` header contract on
their own.

Runtime-generated custom scoring helper scripts are not part of the stable
`WendaoArrow.jl` package surface. They now belong under project-cache
ownership rooted at `PRJ_CACHE_HOME`, while any cache-local namespace below
that root is expected to come from the calling lane's own config or descriptor
surface rather than from package-local numbered files. The canonical checked-in
example servers remain under `examples/` and `scripts/`.

When a downstream package is ready to actually emit a request, it should
prefer `flight_exchange_request(...)` and `flight_exchange_table(...)` instead
of manually threading source, descriptor, and headers at each call site.

The packaged `build_flight_service(...)` and `build_stream_flight_service(...)`
surfaces now also accept `expected_schema_version = ...`, so draft downstream
contracts can exercise the same upstream service builder without forking a
second decode path.

Use the table-first surface when the analyzer wants the entire decoded table at
once. Use the stream-first surface when the analyzer needs batch-wise Arrow
processing and wants to keep the request as an `Arrow.Flight.stream(...)`
consumer for as long as possible.

When a processor needs request-side Flight `app_metadata`, set
`include_request_app_metadata=true` on the packaged service builder. The
table-first processor then receives the same
`(table=..., app_metadata=...)` wrapper returned by upstream
`Arrow.Flight.table(...; include_app_metadata=true)`, while the stream-first
processor iterates the same wrapper shape returned by upstream
`Arrow.Flight.stream(...; include_app_metadata=true)`.

Those packaged service builders now sit on upstream
`Arrow.Flight.exchangeservice(...)`; WendaoArrow only keeps the domain-specific
request decoding, schema-version validation, and descriptor-aware logging.
Package-local local Flight proofs now also use upstream source-based in-process
invocation through `Arrow.Flight.table(service, context, source; ...)` instead
of hand-built request/response `Channel{FlightData}` plumbing.

Current Flight services treat incoming descriptors as routing inputs only.
Outbound responses preserve analyzer-provided schema metadata, preserve
analyzer-provided field metadata when packaged normalization runs through
`normalize_scoring_response(...)`, force `wendao.schema_version = v1`, and do
not echo the request descriptor back into outbound `FlightData`.

When a processor needs sideband batch diagnostics or provenance on the Flight
wire, wrap the response in upstream `Arrow.Flight.withappmetadata(...)`.
WendaoArrow no longer owns a separate runtime carrier for response
`app_metadata`.

The metadata overlay path now rides on upstream `Arrow.withmetadata(...)`
instead of package-local wrapper types.

The packaged gateway client surface is explicit and separate from the local
`v1` scoring-service contract. Gateway search helpers default to
`x-wendao-schema-version = v2`, target the live runtime-owned routes
`/search/repos/main` and `/search/knowledge`, and use upstream
`Arrow.Flight.getflightinfo(...) + doget(...) + table(...)` under the hood.

## Contract Notes

The stream scoring example returns the contract-shaped response columns
`doc_id`, `analyzer_score`, and `final_score`. Those scoring-style examples run
their outbound tuples through `normalize_scoring_response(...)`, which:

- coerces `doc_id` to `String`
- coerces `analyzer_score` / `final_score` to finite `Float64`
- preserves additive columns such as `trace_id`, `tenant_id`,
  `attempt_count`, `cache_hit`, `cache_score`, `cache_generated_at`,
  `cache_backend`, `cache_scope`, `ranking_strategy`, and `retrieval_mode`
- preserves processor-owned schema metadata and field metadata

The metadata Flight example shows how to read request schema metadata such as
`trace_id`, `tenant_id`, `attempt_count`, `cache_hit`, `cache_score`,
`cache_generated_at`, `cache_backend`, `cache_scope`, `ranking_strategy`, and
`retrieval_mode` from Flight batches and re-emit them as additive response
columns without changing the base contract. Those metadata values are validated
on ingress before row iteration.

## Validation

Julia package-local validation:

```bash
.data/WendaoArrow/scripts/test_wendao_arrow.sh
```

Optional Flight server extension validation:

```bash
.data/WendaoArrow/scripts/test_wendao_arrow_flight.sh
```

Live gateway benchmark:

```bash
direnv exec . julia --project=.data/WendaoArrow .data/WendaoArrow/scripts/benchmark_gateway_flight.jl --host 127.0.0.1 --port 9517 --query flight --limit 5 --samples 10
```

The package-local regression matrix is split under `test/runtests/` into
focused support, contract-helper, scoring/metadata-contract, local-Flight, and
config files. The cross-process Flight regression matrix is split under
`test/flight_grpcserver/` into focused support, bootstrap, roundtrip,
contract-error, and response-error files.

Current Flight verification covers:

- extension loading and non-blocking listener startup
- request-side invalid-argument diagnostics for missing columns, duplicate
  `doc_id`, empty `doc_id`, non-numeric and non-finite `vector_score`, and
  invalid schema version
- request-metadata invalid-argument diagnostics for bad typed metadata and bad
  enum metadata
- response-side invalid-argument diagnostics for scoring normalization failures
  and additive metadata response failures
- native Julia error-path regression coverage with a narrow retry only for
  transient `Deadline exceeded.` harness noise during cross-process proof
- schema metadata and field metadata preservation through local and remote
  Flight response paths
- response `app_metadata` preservation through local Flight plus cross-process
  `pyarrow.flight` and native Julia `DoExchange`

Current cross-process status is narrower than the package-local matrix:

- `test/runtests.jl` does not currently include `test/flight_grpcserver.jl`;
  run the cross-process matrix directly when debugging transport behavior
- `test/flight_grpcserver/support.jl` now auto-discovers the repo-local
  `.data/arrow-julia` checkout so the harness can develop both `Arrow` and
  `ArrowTypes` without relying on an extra environment variable
- the harness now accepts either
  `.cache/arrow-julia-flight-pyenv/bin/python` or
  `.cache/arrow-julia-flight-pyenv/.venv/bin/python` for the local
  `pyarrow.flight` environment
- in the current workspace snapshot, the direct
  `direnv exec . julia --project=.data/WendaoArrow.jl .data/WendaoArrow.jl/test/flight_grpcserver.jl`
  matrix now passes for both `pyarrow.flight` and native Julia across scoring,
  metadata, schema-metadata, and error-contract cases
- the native Julia transport closure came from two upstream fixes in the local
  Julia stack:
  - server-side HTTP/2 SETTINGS no longer emit `ENABLE_PUSH`
  - public invalid-argument gRPC responses unwrap `ArgumentError` into plain
    contract text
- the shared `WendaoArrow.jl` substrate now also has generic cross-process
  list-column proofs:
  - native Julia and `pyarrow.flight` pass table-first list request/response
    roundtrips
  - native Julia also passes the same list contract over the routed
    `/graph/structural/rerank` descriptor
  - native Julia also passes that routed list contract with Search-like
    additive request headers
- the remaining native Julia live blocker is therefore narrower than generic
  list transport: it now sits in the exact `WendaoSearch.jl` `v0-draft`
  structural contract shape or the current demo processor path
