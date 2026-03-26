# WendaoArrow Specification

## Purpose

WendaoArrow is a Julia interface package for validating a pluggable Arrow IPC data path from the existing Wendao Rust gateway to Julia over HTTP.

## Scope

This MVP covers:

- the reusable Julia HTTP interface
- generic Arrow request and response transport
- HTTP handler composition for future analyzers
- documentation for Rust gateway integration

This MVP does not cover:

- a duplicated local Rust gateway
- analyzer-specific domain logic
- production Wendao fallback logic
- dynamic schema negotiation
- application-specific ranking semantics

## Design Principles

- The existing Rust gateway owns transport, timeout, and response validation.
- WendaoArrow owns Arrow IPC encode and decode plus HTTP handler composition.
- Future analyzer packages own domain-specific scoring and schema semantics.
- Arrow IPC is the canonical payload format across the boundary.
- The project remains focused on the interface layer until the contract stabilizes.

## Project Layout

- `docs/01_core/wendao_arrow/`: core project docs
- `src/`: WendaoArrow Julia package
- `test/`: package tests
- `examples/`: passthrough server example
- `scripts/`: local helper commands

## Entry Points

- Julia package entry: `src/WendaoArrow.jl`
- Example passthrough server: `examples/passthrough_server.jl`
- Rust gateway integration anchor: `packages/rust/crates/xiuxian-wendao/src/gateway/`
