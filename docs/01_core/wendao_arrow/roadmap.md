# Roadmap

## Phase 0: MVP Scaffold

- initialize the WendaoArrow Julia interface package
- document the Arrow-over-HTTP transport contract
- align the integration boundary with the existing Rust gateway

## Phase 1: Operational Hardening

- add interface-level health and diagnostics helpers
- add schema version marker
- define gateway timeout and retry expectations for the Rust side

## Phase 2: Analyzer Package Integration

- provide integration examples for analyzer packages
- document recommended score and trace columns
- define analyzer package lifecycle expectations

## Phase 3: Production Candidate

- define the adapter interface for main Wendao integration
- add fallback behavior on the Rust gateway side
- benchmark the remote analyzer path against in-process scoring
