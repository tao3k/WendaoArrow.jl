using WendaoArrow

include("support.jl")

using .WendaoArrowExampleSupport

processor = build_list_roundtrip_example_processor(
    request_subject = "WendaoArrow routed list roundtrip Flight request",
    response_metadata = ["response.mode" => "route-probe"],
)

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_flight(
    processor;
    descriptor = WendaoArrow.flight_route_descriptor("/graph/structural/rerank"),
    host = config.host,
    port = config.port,
)
