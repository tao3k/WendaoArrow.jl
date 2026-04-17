module WendaoArrow

using Arrow
using Dates
using Logging
using Tables
using TOML

const DEFAULT_HOST = "127.0.0.1"
const DEFAULT_FLIGHT_PORT = 8815
const DEFAULT_SCHEMA_VERSION = "v1"
const DEFAULT_GATEWAY_FLIGHT_PORT = 9517
const DEFAULT_GATEWAY_SCHEMA_VERSION = "v2"

include("config.jl")
include("codec.jl")
include("ranking_strategy.jl")
include("contracts.jl")
include("flight.jl")
include("listener_backend.jl")
include("gateway_flight.jl")

export DEFAULT_HOST
export DEFAULT_FLIGHT_DESCRIPTOR_PATH
export DEFAULT_FLIGHT_PORT
export DEFAULT_GATEWAY_FLIGHT_PORT
export DEFAULT_GATEWAY_SCHEMA_VERSION
export DEFAULT_SCHEMA_VERSION
export CacheBackend
export CacheScope
export FlightExchangeRequest
export InterfaceConfig
export LinkGraphRetrievalMode
export LinkGraphRetrievalModes
export RankingStrategy
export build_flight_service
export build_stream_flight_service
export coerce_float64
export coerce_metadata_optional_enum
export coerce_optional_datetime
export coerce_optional_bool
export coerce_optional_enum
export coerce_optional_enum_string
export coerce_optional_float64
export coerce_optional_int64
export config_from_args
export coerce_string
export flight_listener_backend_capabilities
export flight_listener_backend_supported
export flight_server
export flight_descriptor
export flight_exchange_request
export flight_exchange_table
export flight_route_descriptor
export flight_schema_headers
export gateway_flight_descriptor
export gateway_knowledge_search_headers
export gateway_repo_search_headers
export load_config
export normalize_metadata_values
export normalize_scoring_response
export require_flight_listener_backend
export require_columns
export require_column_lengths
export require_schema_version
export require_unique_string_column
export merge_schema_metadata
export schema_metadata
export schema_table
export serve_flight
export serve_stream_flight

function _wait_for_flight_server(server; block::Bool)
    if block
        accept_task = getfield(server, :accept_task)
        isnothing(accept_task) || wait(accept_task)
    end
    return server
end

function _require_arrow_purehttp2_listener(subject::AbstractString)
    isdefined(Arrow.Flight, :purehttp2_flight_server) && return
    throw(
        ArgumentError(
            "$(subject) requires an Arrow.jl revision that provides Arrow.Flight.purehttp2_flight_server(...)",
        ),
    )
end

function flight_server(
    service::Arrow.Flight.Service;
    host::AbstractString = DEFAULT_HOST,
    port::Integer = DEFAULT_FLIGHT_PORT,
    request_capacity::Integer = 16,
    response_capacity::Integer = 16,
    backend::Symbol = :purehttp2,
)
    require_flight_listener_backend(backend; subject = "WendaoArrow.flight_server")
    _require_arrow_purehttp2_listener("WendaoArrow.flight_server")
    return getfield(Arrow.Flight, :purehttp2_flight_server)(
        service;
        host = host,
        port = port,
        request_capacity = request_capacity,
        response_capacity = response_capacity,
    )
end

function serve_flight(
    processor::Function;
    host::AbstractString = DEFAULT_HOST,
    port::Integer = DEFAULT_FLIGHT_PORT,
    descriptor = flight_descriptor(),
    include_request_app_metadata::Bool = false,
    block::Bool = true,
    request_capacity::Integer = 16,
    response_capacity::Integer = 16,
    backend::Symbol = :purehttp2,
)
    require_flight_listener_backend(backend; subject = "WendaoArrow.serve_flight")
    service = build_flight_service(
        processor;
        descriptor = descriptor,
        include_request_app_metadata = include_request_app_metadata,
    )
    server = flight_server(
        service;
        host = host,
        port = port,
        request_capacity = request_capacity,
        response_capacity = response_capacity,
        backend = backend,
    )
    return _wait_for_flight_server(server; block = block)
end

function serve_stream_flight(
    processor::Function;
    host::AbstractString = DEFAULT_HOST,
    port::Integer = DEFAULT_FLIGHT_PORT,
    descriptor = flight_descriptor(),
    include_request_app_metadata::Bool = false,
    block::Bool = true,
    request_capacity::Integer = 16,
    response_capacity::Integer = 16,
    backend::Symbol = :purehttp2,
)
    require_flight_listener_backend(backend; subject = "WendaoArrow.serve_stream_flight")
    service = build_stream_flight_service(
        processor;
        descriptor = descriptor,
        include_request_app_metadata = include_request_app_metadata,
    )
    server = flight_server(
        service;
        host = host,
        port = port,
        request_capacity = request_capacity,
        response_capacity = response_capacity,
        backend = backend,
    )
    return _wait_for_flight_server(server; block = block)
end

end
