# WendaoArrow

WendaoArrow is a Julia interface package that exposes a pure Arrow IPC HTTP boundary for the existing Wendao Rust gateway.

## What This Package Owns

WendaoArrow is not the analyzer itself. It owns the Julia-side transport interface that a future analyzer package can reuse.

- The existing Rust gateway owns request shaping, timeout policy, fallback, and response validation.
- WendaoArrow owns Arrow IPC encode and decode plus HTTP handler composition.
- Future Julia analyzer packages implement domain logic on top of this interface.

The Rust gateway remains in the main repository at `packages/rust/crates/xiuxian-wendao/src/gateway/`. This project holds the reusable Julia interface layer and its docs.

## Layout

- `docs/`: Wendao-style project documentation tree
- `src/`: WendaoArrow Julia package
- `test/`: interface tests
- `examples/`: minimal passthrough server
- `scripts/`: helper scripts for local runs

## Documentation

Primary docs live under:

- `docs/01_core/wendao_arrow/SPEC.md`
- `docs/01_core/wendao_arrow/VECTOR_SUBSYSTEM.md`
- `docs/01_core/wendao_arrow/architecture/http-arrow-interface.md`
- `docs/01_core/wendao_arrow/roadmap.md`

## Quick Start

Start the passthrough interface server:

```bash
.data/WendaoArrow/scripts/run_passthrough_server.sh
```

Use TOML:

```bash
.data/WendaoArrow/scripts/run_passthrough_server.sh --config .data/WendaoArrow/config/wendao_arrow.example.toml
```

Or override with flags:

```bash
.data/WendaoArrow/scripts/run_passthrough_server.sh --host 127.0.0.1 --port 18080 --route /arrow-ipc --health-route /health
```

Config precedence is `defaults < TOML < flags`.

Then point the existing Rust gateway integration at the configured route, for example `http://127.0.0.1:8080/arrow-ipc`.

Health checks are served on the configured health route, for example `http://127.0.0.1:8080/health`.

## Validation

Julia:

```bash
.data/WendaoArrow/scripts/test_wendao_arrow.sh
```
