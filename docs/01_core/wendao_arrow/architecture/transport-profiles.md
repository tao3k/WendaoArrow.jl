# Transport Profiles

## Intent

This document aligns WendaoArrow with the current upstream `arrow-julia`
direction.

WendaoArrow now exposes one packaged local transport profile: Arrow Flight.
Any host-level fallback strategy remains outside this Julia package.

## Current Arrow.jl Input

The current `arrow-julia` mainline now provides the primitives WendaoArrow
needs to stay Flight-native:

- in-tree Flight protocol bindings
- Python-owned live interoperability proofs plus optional upstream extension
  backends
- transport-agnostic server composition
- packaged Flight listener helpers owned by `gRPCServer.jl`
- Arrow-owned metadata overlay helpers
- Arrow-owned logical and extension runtime support

That removes the original reason for carrying a package-local HTTP/IPC server
surface inside WendaoArrow.

## Packaged Profile: Arrow Flight

Arrow Flight is the packaged transport profile for WendaoArrow because it
matches the active blueprint and the upstream Julia Arrow direction:

- stream-oriented record batch exchange
- native Julia client and server support
- better transport semantics for remote execution and capability growth

WendaoArrow therefore treats the stream-first processor surface as the primary
package shape. The Flight binding reuses the existing stream-first contract
instead of introducing a second analyzer-facing contract.

The package exposes:

- `flight_descriptor(path)` as the packaged default-path helper over upstream
  `Arrow.Flight.pathdescriptor(...)`
- `build_flight_service(processor)` for table-first `DoExchange`
- `build_stream_flight_service(processor)` for stream-first `DoExchange`
- `flight_server`, `serve_flight`, and `serve_stream_flight` through the
  packaged `:grpcserver` listener path
- `flight_listener_backend_capabilities(...)` and
  `flight_listener_backend_supported(...)` for the packaged network-listener
  backend contract, delegated to upstream `Arrow.Flight`

The packaged listener wrappers keep three runtime bounds explicit:

- `max_active_requests` for concurrency admission control
- `request_capacity` for request-stream buffering
- `response_capacity` for response-stream buffering

Current response encoding treats the incoming `FlightDescriptor` as a routing
input, not as a response echo contract. Outbound `FlightData` preserves
analyzer-provided schema metadata, preserves analyzer-provided field metadata
when packaged normalization runs through `normalize_scoring_response(...)`,
forces `wendao.schema_version = v1`, and omits `flight_descriptor` so the
service surface stays closer to standard Flight server behavior.

The package Flight hot path now rides on upstream
`Arrow.Flight.exchangeservice(...)`, so request priming, fallback descriptor
resolution, response closing, and `putflightdata!` emission are no longer
package-owned transport glue. WendaoArrow keeps only domain decode,
schema-version validation, and descriptor-aware logging on top.
The package-local local-Flight test surface now uses upstream
`Arrow.Flight.table(service, context, source; ...)` for in-process invocation
instead of hand-built request/response `FlightData` channels.

When a packaged processor needs sideband per-batch diagnostics or provenance,
WendaoArrow now keeps that surface at the same abstraction level through
upstream `Arrow.Flight.withappmetadata(...)` instead of a package-owned
runtime carrier.

Current packaged backend support remains `:grpcserver` as the only supported
default. The requested `Nghttp2Wrapper.jl` backend is not accepted as a
packaged listener backend yet because upstream `Arrow.Flight` ships that
backend only behind the optional `Nghttp2Wrapper.jl` extension surface and it
still lacks full request-streaming and bidirectional parity. WendaoArrow also
treats the old `:purehttp2` selector as retired legacy now that the packaged
live backend is owned by `gRPCServer.jl`.

## Host Runtime Boundary

WendaoArrow no longer ships local HTTP or IPC server helpers.

If the host runtime wants a non-Flight fallback, that policy belongs above the
package boundary. The package contract that should survive any host-level
fallback is:

- Arrow request and response schema contract in `arrow-schema-contract.md`
- stream-first processor compatibility for analyzer packages
- schema contract version propagation through Arrow schema metadata
- analyzer ownership of domain logic and scoring semantics

Transport selection, fallback ordering, and diagnostics remain host/runtime
responsibilities.

## Package Implications

Near-term package work should follow these rules:

- keep the packaged `:grpcserver` listener wrappers aligned with the
  stream-first processor contract
- keep server-side Flight responses descriptor-free unless a concrete
  interoperability requirement proves otherwise
- keep new examples and analyzer guidance Flight-first and stream-first
- validate request-batch column contracts close to the analyzer seam so remote
  Flight failures preserve actionable domain diagnostics
- keep Flight server integration behind package-local wrappers instead of
  mixing selection logic into analyzer code
- preserve schema-version observability through Arrow schema metadata on every
  packaged response path
- keep optional Flight startup tooling isolated from the default package
  environment unless the runtime explicitly opts in

## Next Integration Checkpoints

- deepen remote-client `DoExchange` interoperability coverage beyond the
  landed `pyarrow` and native Julia matrix
- improve richer domain error mapping and larger exchange coverage
- replace the bootstrap-style startup helper with a steadier operator-facing
  runtime path once WendaoArrow has a dedicated Flight runtime environment
- keep the Rust side responsible for negotiation and any explicit fallback
