using WendaoArrow
using gRPCServer

include("support.jl")

using .WendaoArrowExampleSupport

processor = build_list_roundtrip_example_processor(
    request_subject = "WendaoArrow list roundtrip Flight request",
    response_metadata = ["response.mode" => "list-roundtrip"],
)

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_flight(processor; host = config.host, port = config.port)
