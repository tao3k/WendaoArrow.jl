# Vector Subsystem

## Boundary

WendaoArrow does not own vector semantics. It transports Arrow tables between the existing Rust gateway and future Julia analyzers.

## Recommended Upstream Vector Columns

- `doc_id`: stable candidate identifier
- `vector_score`: coarse retrieval score from the Rust side
- `embedding`: candidate vector
- `query_embedding`: query vector repeated for each row

## Recommended Downstream Result Columns

- `doc_id`: stable candidate identifier
- `analyzer_score`: analyzer-defined numeric score
- `final_score`: weighted blend returned to Rust

## Responsibilities

- Rust:
  - batch shaping
  - schema validation
  - transport policy
- WendaoArrow:
  - Arrow decode and encode
  - HTTP handler composition
- Analyzer packages:
  - vector math
  - response column definitions

## Next Extensions

- add `topology_score`
- add `cluster_id`
- add `freshness_days`
- add analyzer package integration examples
