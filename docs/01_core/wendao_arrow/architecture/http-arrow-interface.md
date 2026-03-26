# HTTP Arrow Interface

## Intent

This document defines the transport and composition layer provided by the WendaoArrow Julia package.

The Rust gateway implementation remains in `packages/rust/crates/xiuxian-wendao/src/gateway/` and is not duplicated inside `.data/WendaoArrow`.

## Transport

- Method: `POST`
- Default route: `/arrow-ipc`
- Default health route: `/health`
- Content-Type: `application/vnd.apache.arrow.stream`
- Payload format: Arrow IPC stream

## Runtime Configuration

WendaoArrow supports runtime configuration from:

- defaults in the package
- TOML under `[interface]`
- CLI flags such as `--host`, `--port`, `--route`, `--health-route`, and `--content-type`

Precedence is `defaults < TOML < flags`.

## Package Responsibilities

WendaoArrow provides:

- Arrow IPC request decoding
- Arrow IPC response encoding
- HTTP handler composition around a user-supplied processor
- health endpoint handling
- request and processor failure isolation
- a small server helper for local hosting

## Processor Contract

The user-supplied processor receives an Arrow table decoded from the HTTP request and returns any Arrow-writable table-like value.

The processor is expected to own:

- domain schema interpretation
- numeric analysis
- response column definitions

## Ownership Boundary

- Rust gateway:
  - request construction
  - timeout and retry policy
  - HTTP error handling
  - response validation
- WendaoArrow:
  - transport and Arrow IPC composition
- Future analyzer packages:
  - business logic
  - scoring
  - schema semantics above the transport layer

## Error Semantics

- `200` on the main route: valid Arrow IPC response
- `200` on the health route: JSON `{"status":"ok"}`
- `400` on the main route: invalid Arrow IPC request
- `500` on the main route: processor failure or invalid processor output

## Extension Path

Future versions can add:

- schema version headers
- structured trace headers
- compressed Arrow payload support
- integration adapters for analyzer packages
