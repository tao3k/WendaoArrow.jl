using WendaoArrow

include("support.jl")

using .WendaoArrowExampleSupport

processor = build_stream_scoring_example_processor(
    request_subject = "WendaoArrow stream scoring Flight request batch",
    response_subject = "WendaoArrow stream scoring Flight response",
)

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_stream_flight(
    processor;
    descriptor = WendaoArrow.flight_descriptor(("rerank",)),
    host = config.host,
    port = config.port,
)
