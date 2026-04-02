# Roadmap

## Phase 0: MVP Scaffold

- initialize the WendaoArrow Julia interface package
- document the transport-neutral Arrow contract
- align the integration boundary with the existing Rust gateway
- establish a Flight-first package boundary

## Phase 1: Flight Alignment

- document Flight-only local transport policy for WendaoArrow
- keep stream-first processor surfaces aligned with Flight handlers
- land package-local `DoExchange` Flight service seams
- keep server-side Flight response envelopes aligned with descriptor-free
  standard client expectations
- land an optional `gRPCServer`-backed network listener surface without making
  it a default dependency of the base package runtime

## Phase 2: Operational Hardening

- keep transport-level contract version propagation explicit through packaged
  Flight responses
- deepen remote-client Flight interoperability beyond the current `pyarrow`
  and native Julia single-batch and multi-batch happy-path `DoExchange`
  proofs, now that pre-first-response input-contract failures propagate as
  explicit invalid-argument gRPC status with concrete non-empty `doc_id` /
  finite `vector_score` / schema-version diagnostics to `pyarrow` and native
  Julia clients
- keep generic `Tables.jl` batch-shape validation explicit even where Arrow
  wire payloads already enforce aligned column lengths
- keep scoring-response normalization explicit for packaged analyzer examples
  so `doc_id` / `analyzer_score` / `final_score` stay contract-shaped while
  additive columns remain possible
- keep metadata-style additive response columns explicit about nullability and
  row-count alignment, including `trace_id`, `tenant_id`, and typed columns
  such as `attempt_count`, `cache_hit`, `cache_score`, and
  `cache_generated_at`, plus typed enum columns such as `cache_backend`,
  `cache_scope`, `ranking_strategy`, and `retrieval_mode`
- keep remote proofs for metadata-style additive response columns, starting
  with `trace_id`, `tenant_id`, `attempt_count`, `cache_hit`, and
  `cache_score`, plus `cache_generated_at`, `cache_backend`, and
  `cache_scope`, plus `ranking_strategy` and `retrieval_mode`, so remote
  clients see both present and missing values, including the native-Julia
  typed-enum and cross-language primitive-storage split for
  `cache_backend`, `cache_scope`, `ranking_strategy`, and `retrieval_mode`
- preserve explicit remote diagnostics for metadata-style request metadata
  validation failures instead of deferring them into response normalization
  including empty string failures for required non-empty string metadata keys
  and invalid typed/enum metadata such as `cache_hit`, `cache_score`,
  `cache_generated_at`, `cache_backend`, `cache_scope`,
  `ranking_strategy`, or `retrieval_mode`
- preserve explicit remote diagnostics for metadata-style additive response
  normalization failures instead of collapsing them into generic processor
  failures, including typed additive columns and typed enum columns such as
  `cache_backend`, `cache_scope`, `ranking_strategy`, and `retrieval_mode`
- preserve explicit remote diagnostics for scoring-response normalization
  failures instead of collapsing them into generic processor failures
- keep the cross-process Flight regression matrix split into focused
  `test/flight_grpcserver/*` files so metadata and scoring proofs can evolve
  without one monolithic test file
- define gateway timeout and retry expectations for the Rust side

## Phase 3: Analyzer Package Integration

- provide integration examples for analyzer packages
- keep optional Flight startup scripts aligned with stream-first analyzer
  examples until a dedicated runtime package exists
- document recommended score and trace columns
- define analyzer package lifecycle expectations

## Phase 4: Production Candidate

- define the adapter interface for main Wendao integration
- add end-to-end Flight client interoperability coverage and operator-facing
  startup guidance for the optional Flight server path
- add any required fallback behavior on the Rust gateway side
- benchmark the remote analyzer path against in-process scoring
