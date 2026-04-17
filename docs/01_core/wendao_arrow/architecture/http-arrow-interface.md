# HTTP Interface Removed

WendaoArrow no longer ships a local HTTP or IPC transport surface.

The package now exposes Flight-only local transport composition through
`Arrow.Flight.Service` plus the packaged `PureHTTP2` listener path. Any host-level
fallback transport belongs above the WendaoArrow package boundary.

The removed surfaces include:

- `build_handler`
- `build_stream_handler`
- `serve`
- `serve_stream`
- local HTTP route and health-route configuration
- local Arrow IPC request and response helpers

The stable package contract is documented in:

- `transport-profiles.md`
- `arrow-schema-contract.md`
