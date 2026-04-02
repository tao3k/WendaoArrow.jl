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
include("gateway_flight.jl")

export DEFAULT_HOST
export DEFAULT_FLIGHT_DESCRIPTOR_PATH
export DEFAULT_FLIGHT_PORT
export DEFAULT_GATEWAY_FLIGHT_PORT
export DEFAULT_GATEWAY_SCHEMA_VERSION
export DEFAULT_SCHEMA_VERSION
export CacheBackend
export CacheScope
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
export flight_server
export flight_descriptor
export gateway_flight_client
export gateway_flight_descriptor
export gateway_flight_table
export gateway_knowledge_search
export gateway_knowledge_search_headers
export gateway_repo_search
export gateway_repo_search_headers
export load_config
export normalize_metadata_values
export normalize_scoring_response
export require_columns
export require_column_lengths
export require_schema_version
export require_unique_string_column
export schema_metadata
export serve_flight
export serve_stream_flight

function flight_server(args...; kwargs...)
    throw(
        ArgumentError(
            "WendaoArrow.flight_server requires the optional gRPCServer weak dependency in the active Julia environment",
        ),
    )
end

function serve_flight(args...; kwargs...)
    throw(
        ArgumentError(
            "WendaoArrow.serve_flight requires the optional gRPCServer weak dependency in the active Julia environment",
        ),
    )
end

function serve_stream_flight(args...; kwargs...)
    throw(
        ArgumentError(
            "WendaoArrow.serve_stream_flight requires the optional gRPCServer weak dependency in the active Julia environment",
        ),
    )
end

end
