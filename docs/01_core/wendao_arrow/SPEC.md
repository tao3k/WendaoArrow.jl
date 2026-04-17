# WendaoArrow Specification

## Purpose

WendaoArrow is a Julia transport interface package for validating a pluggable
Arrow-native data path from the existing Wendao Rust gateway to Julia. The
current package implementation is Flight-only locally and exposes packaged
`PureHTTP2` Flight server helpers over the same analyzer-facing processor
contract.

## Scope

This MVP covers:

- the reusable Julia transport interface
- generic Arrow request and response transport over Flight
- package-local Flight `DoExchange` service composition over `Arrow.Flight`
- packaged `PureHTTP2` Flight server helpers over the same processor contracts
- transport-profile documentation for future Flight adoption
- documentation for Rust gateway integration

This MVP does not cover:

- a duplicated local Rust gateway
- analyzer-specific domain logic
- production Wendao fallback logic
- dynamic schema negotiation
- application-specific ranking semantics
- production-grade Flight operational hardening and remote interoperability
  coverage

## Design Principles

- The existing Rust gateway and future runtime layer own transport selection,
  timeout, and response validation.
- WendaoArrow owns the Arrow contract layer plus package-local Flight
  transport composition.
- WendaoArrow may expose Flight service and packaged listener adapters without
  owning host-level transport negotiation.
- Future analyzer packages own domain-specific scoring and schema semantics.
- Arrow-native columnar payloads are the canonical data plane across the
  boundary.
- Arrow Flight is the preferred transport when the runtime and plugin can
  negotiate it.
- The project remains focused on the interface layer until the contract
  stabilizes.

## Project Layout

- `docs/01_core/wendao_arrow/`: core project docs
- `src/`: WendaoArrow Julia package
- `test/`: package tests
- `examples/`: Flight startup examples
- `scripts/`: local helper commands

## Entry Points

- Julia package entry: `src/WendaoArrow.jl`
- Example Flight scoring server: `examples/stream_scoring_flight_server.jl`
- Local Flight service seam: `build_flight_service` /
  `build_stream_flight_service`
- Packaged Flight listener helpers: `flight_server` / `serve_flight` /
  `serve_stream_flight`
- Rust gateway integration anchor: `packages/rust/crates/xiuxian-wendao/src/gateway/`

## Contract References

- Transport profile direction:
  `docs/01_core/wendao_arrow/architecture/transport-profiles.md`
- Arrow request and response schema: `docs/01_core/wendao_arrow/architecture/arrow-schema-contract.md`
