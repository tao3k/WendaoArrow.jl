module WendaoArrow

using Arrow
using HTTP
using Logging
using Tables
using TOML

const CONTENT_TYPE = "application/vnd.apache.arrow.stream"
const DEFAULT_ROUTE = "/arrow-ipc"
const DEFAULT_HEALTH_ROUTE = "/health"
const DEFAULT_HOST = "127.0.0.1"
const DEFAULT_PORT = 8080
const JSON_CONTENT_TYPE = "application/json"

include("config.jl")
include("codec.jl")
include("http_responses.jl")
include("server.jl")

export CONTENT_TYPE
export DEFAULT_HOST
export DEFAULT_HEALTH_ROUTE
export DEFAULT_PORT
export DEFAULT_ROUTE
export InterfaceConfig
export build_handler
export config_from_args
export decode_ipc
export encode_ipc
export load_config
export serve

end
