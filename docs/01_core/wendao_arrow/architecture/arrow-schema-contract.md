# Arrow Schema Contract

## Intent

This document defines the first stable request and response contract for WendaoArrow-compatible Julia processors.

This contract is transport-neutral. It is the minimum Arrow schema surface that
Rust, WendaoArrow, and Julia analyzer packages can rely on without embedding
analyzer-specific logic in the interface layer.

## Contract Versioning

- Contract version: `v1`
- Stability level: MVP-stable
- Compatibility model:
  - additive columns are allowed when Rust does not require them
  - removing required columns is a breaking change
  - changing the semantic meaning of an existing column is a breaking change

## Transport-Level Metadata

The contract version must remain observable across transport profiles.

Preferred carriers:

- cross-profile payload metadata: Arrow schema metadata key
  `wendao.schema_version = v1`
- packaged local transport: Flight `DoExchange` payload schema metadata

The packaged WendaoArrow runtime should duplicate `wendao.schema_version = v1`
into successful Arrow response schema metadata so post-decode tooling can
inspect the payload contract without relying on transport-specific side
channels.

## Request Schema

The request payload is an Arrow table/stream represented as one or more record
batches. The packaged WendaoArrow runtime currently exposes this contract
through Flight `DoExchange`.

### Required Columns

- `doc_id: Utf8`
  - stable identifier for the candidate row
  - must be non-null
  - must be unique within one request batch set
- `vector_score: Float64`
  - coarse retrieval score produced by Rust
  - must be non-null

### Optional Columns

- `topology_score: Float64`
- `freshness_days: Int32` or `Int64`
- `token_count: Int32` or `Int64`
- `cluster_id: Utf8`
- `source_kind: Utf8`
- `attributes_json: Utf8`
- `is_valid_anchor: Boolean`

Optional columns may be absent. When present, their nullability is analyzer-defined unless otherwise documented by a higher-level analyzer package.

### Recommended Upstream Vector Columns

- `embedding: FixedSizeList<Float32, D>` or `List<Float32>`
  - candidate embedding
  - analyzer-defined when present
- `query_embedding: FixedSizeList<Float32, D>` or `List<Float32>`
  - query embedding repeated or broadcast-compatible for each row
  - analyzer-defined when present

## Request Null Semantics

- required columns:
  - null is invalid input
  - WendaoArrow may decode successfully, but the processor should reject the payload and return a processor failure
- optional columns:
  - null means unknown or not provided
- empty request:
  - invalid input for the packaged WendaoArrow stream processors
  - WendaoArrow currently rejects empty Flight exchanges before response generation

## Response Schema

The response payload is an Arrow table/stream represented as one or more record
batches.

### Required Columns

- `doc_id: Utf8`
  - stable identifier matching a request row
  - must be non-null
- `analyzer_score: Float64`
  - score produced by Julia-side analysis
  - must be non-null
- `final_score: Float64`
  - score Rust uses for downstream ranking
  - must be non-null

### Optional Columns

- `confidence: Float64`
- `diversity_penalty: Float64`
- `graph_bonus: Float64`
- `ranking_reason: Utf8`
- `trace_id: Utf8`

## Response Matching Semantics

- every response `doc_id` must exist in the request
- Rust should not assume row order is preserved
- Rust should join by `doc_id`, not by row position
- duplicate `doc_id` values in the response are invalid

## Error Semantics

Contract-level failure classes:

- invalid Arrow request payload
- processor failure
- processor output that cannot be encoded as Arrow
- unsupported contract version

### Packaged Flight Mapping

For the packaged WendaoArrow Flight runtime:

- request-decode and request-contract failures should surface as non-OK gRPC
  responses
- current contract failures surface as `grpc-status: 3` / invalid argument
- processor failures and output-encode failures should remain transport-visible
  without leaking Julia stack traces into client-visible payloads

## Rust Validation Rules

Rust should validate at least the following before accepting a Julia response:

- expected schema version is supported
- required response columns exist
- required response columns are non-null
- `doc_id` values are unique
- all response `doc_id` values map to request candidates
- `final_score` values are finite

If validation fails, Rust should treat the analyzer call as failed and enter the normal fallback or degrade path owned by the Rust gateway.

## Analyzer Guidance

Analyzer packages should:

- preserve `doc_id` exactly as received
- avoid mutating transport-level column names
- document any additional required optional columns in analyzer-specific docs
- keep custom columns additive so the base WendaoArrow contract stays reusable
- keep scoring logic outside WendaoArrow itself; the first concrete package is `WendaoAnalyzer`

## Non-Goals

This contract does not yet define:

- host-level fallback transport negotiation
- column-level capability negotiation
- partial-success result envelopes
- streaming incremental ranking semantics
