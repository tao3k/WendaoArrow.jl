# WendaoArrow

WendaoArrow is the Julia transport interface package for Wendao's Arrow-native
plugin boundary. The local package runtime is now Flight-only. WendaoArrow
ships package-local `Arrow.Flight.Service` composition plus an optional
`gRPCServer` bridge; it no longer exposes local HTTP or IPC server helpers.

The first analyzer package now lives alongside it at `.data/WendaoAnalyzer`.

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
.data/WendaoArrow/scripts/run_stream_scoring_flight_server.sh --host 127.0.0.1 --port 18815
```

Start the metadata-aware Flight server:

```bash
.data/WendaoArrow/scripts/run_stream_metadata_flight_server.sh --host 127.0.0.1 --port 18815
```

Start the schema-metadata preservation Flight server:

```bash
.data/WendaoArrow/scripts/run_stream_schema_metadata_flight_server.sh --host 127.0.0.1 --port 18815
```

Start the response-`app_metadata` Flight server:

```bash
.data/WendaoArrow/scripts/run_stream_app_metadata_flight_server.sh --host 127.0.0.1 --port 18815
```

Use TOML:

```bash
.data/WendaoArrow/scripts/run_stream_scoring_flight_server.sh --config .data/WendaoArrow/config/wendao_arrow.example.toml
```

Or override with flags:

```bash
.data/WendaoArrow/scripts/run_stream_scoring_flight_server.sh --host 127.0.0.1 --port 18815
```

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
- `normalize_metadata_values(metadata)` for shared additive request-metadata
  validation
- `normalize_scoring_response(table_like)` for packaged scoring-style response
  normalization while preserving schema and field metadata
- `flight_descriptor(path)` as the packaged default-path helper over upstream
  `Arrow.Flight.pathdescriptor(...)`
- `build_flight_service(processor)` for table-first local Flight `DoExchange`
  services
- `build_stream_flight_service(processor)` for stream-first local Flight
  `DoExchange` services
- `flight_server(service)` for optional `gRPCServer`-backed listener
  composition
- `serve_flight(processor)` and `serve_stream_flight(processor)` for optional
  Flight listeners when `gRPCServer` is loaded

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

The package-local regression matrix is split under `test/runtests/` into
focused support, contract-helper, scoring/metadata-contract, local-Flight, and
config files. The cross-process Flight regression matrix is split under
`test/flight_grpcserver/` into focused support, bootstrap, roundtrip,
contract-error, and response-error files.

Current Flight verification covers:

- extension loading and non-blocking listener startup
- single-batch and multi-batch cross-process `pyarrow.flight` and native Julia
  `Arrow.Flight.Client` happy-path `DoExchange`
- native Julia client proof through upstream source-based
  `Arrow.Flight.doexchange(client, source; ...)` plus high-level
  `Arrow.Flight.table(...)` decoding instead of manual request-channel writes
  and response-message collection
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
