using WendaoArrow

include("support.jl")

using .WendaoArrowExampleSupport

processor = build_parser_summary_like_example_processor(
    request_subject = "WendaoArrow parser-summary-like Flight request batch",
)

config = WendaoArrow.config_from_args(ARGS)

WendaoArrow.serve_stream_flight(
    processor;
    descriptor = WendaoArrow.flight_descriptor(("rerank",)),
    host = config.host,
    port = config.port,
    max_message_size = WendaoArrow.DEFAULT_GATEWAY_FLIGHT_MAX_MESSAGE_LENGTH,
)
